%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Mgr ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_mgr).

-behavior(gen_server).
-include("blockchain.hrl").
-include("blockchain_vars.hrl").
-define(TXN_CACHE, txn_cache).
-define(RECENT_BLOCK_AGE, 30 * 60).  %% 30 mins

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------
-export([
         start_link/1,
         submit/2, submit/3,
         grpc_submit/1,
         set_chain/1,
         txn_list/0,
         txn_status/1,
         make_ets_table/0
        ]).

%% Testing backdoors for CT
-export([
    force_process_cached_txns/0,
    get_rejections_deferred/0
]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------
-export([
         init/1,
         handle_call/3,
         handle_info/2,
         handle_cast/2,
         terminate/2,
         code_change/3
        ]).

-type rejection() ::
    {
        Member :: libp2p_crypto:pubkey_bin(),
        Height :: non_neg_integer() | undefined,
        RejectReason :: atom() | undefined
    }.

-type acception() ::
    {
        Member :: libp2p_crypto:pubkey_bin(),
        Height :: non_neg_integer() | undefined,
        QueuePos :: atom() | undefined
    }.

-type deferred_rejection() ::
    {
        %% TODO Complete types
        Dialer :: dialer(), %% TODO dialer() or pid()?
        TxnKey :: txn_key(),
        Txn    :: blockchain_txn:txn(),
        Member :: libp2p_crypto:pubkey_bin(),
        Height :: non_neg_integer() | undefined,
        RejectReason :: atom() | undefined
    }.

-record(state, {
          submit_f :: undefined | integer(),
          reject_f :: undefined | integer(),
          cur_block_height :: undefined | integer(),
          txn_cache :: undefined | ets:tid(),
          chain :: undefined | blockchain:blockchain(),
          has_been_synced= false :: boolean(),
          rejections_deferred :: [deferred_rejection()],
          pending_mgr_state :: any()
         }).

-record(txn_data,
        {
            callback :: fun(),
            recv_block_height=undefined :: undefined | integer(),
            acceptions=[] :: [acception()],
            rejections=[] :: [rejection()],
            dialers=[] :: dialers()
        }).

-type txn_key() :: term().
-type cached_txn_type() :: {TxnKey :: txn_key(), Txn :: blockchain_txn:txn(), TxnData :: #txn_data{}}.
-type dialers() :: [dialer()].
-type dialer() :: {pid(), libp2p_crypto:pubkey_bin()}.

-export_type([txn_key/0, cached_txn_type/0]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start_link(Args) when is_map(Args) ->
    case gen_server:start_link({local, ?MODULE}, ?MODULE, Args, [{hibernate_after, 5000}]) of
        {ok, Pid} ->
            %% if we have an ETS table reference, give ownership to the new process
            %% we likely are the `heir', so we'll get it back if this process dies
            case maps:find(ets, Args) of
                error ->
                    ok;
                {ok, Tab} ->
                    true = ets:give_away(Tab, Pid, undefined)
            end,
            {ok, Pid};
        Other ->
            Other
    end.

-spec submit(Txn :: blockchain_txn:txn(), Callback :: fun()) -> ok.
submit(Txn, Callback) ->
    gen_server:cast(?MODULE, {submit, Txn, get_txn_key(), Callback}).

-spec grpc_submit(Txn :: blockchain_txn:txn()) -> {ok, txn_key()}.
grpc_submit(Txn) ->
    TxnKey = get_txn_key(),
    gen_server:cast(?MODULE, {submit, Txn, TxnKey, fun()-> ok end}),
    {ok, TxnKey}.

-spec submit(Txn :: blockchain_txn:txn(), Key :: txn_key(), Callback :: fun()) -> ok.
submit(Txn, Key, Callback) ->
    gen_server:cast(?MODULE, {submit, Txn, Key, Callback}).

-spec set_chain(blockchain:blockchain()) -> ok.
set_chain(Chain) ->
    gen_server:cast(?MODULE, {set_chain, Chain}).

-spec txn_list() -> [cached_txn_type()].
txn_list() ->
    gen_server:call(?MODULE, txn_list, infinity).

-spec txn_status(txn_key()) -> {ok, pending, map()} | {error, not_found}.
txn_status(TxnKey) ->
    case cached_txn(TxnKey) of
        {ok, {TxnKey, _Txn, #txn_data{acceptions = Acceptions, rejections = Rejections, recv_block_height = RecvBlockHeight} = _TxnData}} ->
            {ok, pending,
                #{  recv_block_height => RecvBlockHeight,
                    acceptors => Acceptions,
                    rejectors => Rejections}};
        {error, txn_not_found} = Error ->
            Error

    end.

make_ets_table() ->
    ets:new(?TXN_CACHE,
            [named_table,
             protected,
             {heir, self(), undefined}]).

-spec get_rejections_deferred() -> [deferred_rejection()].
get_rejections_deferred() ->
    gen_server:call(?MODULE, get_rejections_deferred, infinity).

force_process_cached_txns() ->
    gen_server:call(?MODULE, force_process_cached_txns, infinity).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init(Args) ->
    lager:info("txn mgr starting...",[]),
    erlang:process_flag(trap_exit, true),
    TxnCache = case maps:find(ets, Args) of
                   error ->
                       make_ets_table();
                   {ok, Tab} ->
                       Tab
               end,

    PendingTxnMgrOpts = case maps:find(base_dir, Args) of
                   error ->
                       [];
                   {ok, Path} ->
                       [{base_dir, Path}]
               end,

    ok = blockchain_event:add_handler(self()),
    %% initialize the pending txn mgr and load in any pending txns
    {ok, PendingMgrState} = blockchain_pending_txn_mgr:init(PendingTxnMgrOpts),
    _ = blockchain_pending_txn_mgr:load_chain(PendingMgrState),
    {ok, #state{txn_cache = TxnCache, rejections_deferred = [], pending_mgr_state = PendingMgrState}}.

handle_cast({set_chain, Chain}, State=#state{chain = undefined}) ->
    NewState = initialize_with_chain(State, Chain),
    {noreply, NewState};

handle_cast({submit, Txn, Key, Callback}, State=#state{chain = undefined}) ->
    %% Got txn when there is no chain, keep it in the cache and process when its available
    %% as no chain we dont have any height data, so cache it with height = undefined
    %% we will update when the chain is set and we submit these cached txns
    %% provided key will be utilised
    %% in this flow we check upfront if the provided key is a dup and reject the txn if true
    case cached_txn(Key) of
        {ok,_} ->
            ok = invoke_callback(Callback, {error, duplicate_key});
        {error, txn_not_found} ->
            ok = cache_txn(Key, Txn, #txn_data{callback = Callback})
    end,
    {noreply, State};

handle_cast({submit, Txn, Key, Callback}, State=#state{cur_block_height = H}) ->
    %% add the txn to the cache
    %% provided key will be utilised
    lager:debug("adding txn to cache: ~s", [blockchain_txn:print(Txn)]),
    %% in this flow we check upfront if the provided key is a dup and reject the txn if true
    case cached_txn(Key) of
        {ok,_} ->
            ok = invoke_callback(Callback, {error, duplicate_key});
        {error, txn_not_found} ->
            ok = cache_txn(Key, Txn, #txn_data{callback = Callback, recv_block_height = H})
    end,
    {noreply, State};

handle_cast(_Msg, State) ->
    lager:warning("blockchain_txn_mgr got unknown cast: ~p", [_Msg]),
    {noreply, State}.

handle_call(
    force_process_cached_txns,
    _,
    #state{
        chain = Chain,
        cur_block_height = CurBlockHeight,
        submit_f = SubmitF
    }=State
) ->
    HasBeenSynced = false,
    IsNewElection = false,
    NewCGMembers = [],
    Result = process_cached_txns(Chain, CurBlockHeight, SubmitF, HasBeenSynced, IsNewElection, NewCGMembers),
    {reply, Result, State};
handle_call(get_rejections_deferred, _, #state{rejections_deferred=Deferred}=State) ->
    {reply, Deferred, State};

handle_call(txn_list, _, State) ->
    Fields = record_info(fields, txn_data),
    F = fun({_, Txn, Rec})->
            [_Tag| Values] = tuple_to_list(Rec),
             {Txn,lists:zip(Fields, Values)}
        end,
    P = lists:map(F, cached_txns()),
    {reply, maps:from_list(P), State};

handle_call(_Msg, _From, State) ->
    lager:warning("blockchain_txn_mgr got unknown call: ~p, From: ~p", [_Msg, _From]),
    {reply, ok, State}.

%% dial related failures
handle_info({dial_failed, {Dialer, TxnKey, Txn, Member}}, State) ->
    lager:debug("txn: ~s, dial_failed: ~p, Dialer: ~p", [blockchain_txn:print(Txn), Member, Dialer]),
    ok = retry(TxnKey, Txn, Dialer),
    {noreply, State};

handle_info({dial_timeout, {Dialer, TxnKey, Txn, Member}}, State) ->
    lager:debug("txn: ~s, timeout: ~p, Dialer: ~p. Dialer will be stopped", [blockchain_txn:print(Txn), Member, Dialer]),
    ok = blockchain_txn_mgr_sup:stop_dialer(Dialer),
    ok = retry(TxnKey, Txn, Dialer),
    {noreply, State};

handle_info({send_failed, {Dialer, TxnKey, Txn, Member}}, State) ->
    lager:debug("txn: ~s, send_failed: ~p, Dialer: ~p", [blockchain_txn:print(Txn), Member, Dialer]),
    ok = retry(TxnKey, Txn, Dialer),
    {noreply, State};

%% dialed CG member related failures
handle_info({txn_accepted, {Dialer, TxnKey, Txn, Member, Height, QueuePos}}, State) ->
    lager:debug("txn: ~s, accepted_by: ~p, Dialer: ~p at height: ~p and queuepos: ~p", [blockchain_txn:print(Txn), Member, Dialer, Height, QueuePos]),
    ok = accepted(TxnKey, Txn, Member, Dialer, Height, QueuePos),
    {noreply, State};
handle_info({txn_failed, {Dialer, TxnKey, Txn, Member, FailReason}}, State) ->
    lager:info("txn: ~s, failed with reason: ~p, member: ~p Dialer: ~p", [blockchain_txn:print(Txn), FailReason, Member, Dialer]),
    ok = retry(TxnKey, Txn, Dialer),
    {noreply, State};
handle_info(
    {{txn_rejected, {Dialer, TxnKey, Txn, Member, Height, RejectReason}} = Rejection},
    #state{
        cur_block_height = CurBlockHeight,
        reject_f = RejectF,
        rejections_deferred = Deferred0
    } = State0
) ->
    RejectorHeight =
        case Height of
            %% txn protocol v1 - no height
            undefined ->
                CurBlockHeight;
            %% txn protocol v2 or later - has height
            _ ->
                Height
        end,
    lager:debug(
        "txn: ~s, rejected_by: ~p, Dialer: ~p, "
        "my height: ~p, rejector height: ~p"
        "reject reason: ~p",
        [
            blockchain_txn:print(Txn), Member, Dialer,
            CurBlockHeight, RejectorHeight, RejectReason
        ]
    ),
    MaxRejectionAge =
        application:get_env(blockchain, txn_mgr_rejection_max_age, 15),
    Deferred1 =
        case CurBlockHeight - RejectorHeight of
            %% future:
            Age when Age < 0 ->
                %% TODO Maybe limit how far in the future?
                lager:warning(
                    "Received txn rejection from the future. Deferring: ~p",
                    [Rejection]
                ),
                ordsets:add_element(Rejection, Deferred0);
            %% present or recent past:
            Age when (Age >= 0) andalso (Age < MaxRejectionAge) ->
                if
                    Age > 0 andalso Age =< 2 ->
                        lager:debug(
                            "Received txn rejection from the past. "
                            "From ~b blocks ago. Counting: ~p",
                            [Age, Rejection]
                        );
                    Age > 2 ->
                        lager:warning(
                            "Received txn rejection from older, "
                            "but still acceptable past. "
                            "From ~b blocks ago. Counting: ~p",
                            [Age, Rejection]
                        );
                    true ->
                        ok
                end,
                ok = rejected(TxnKey, Txn, Member, Dialer, CurBlockHeight, RejectF, RejectorHeight, RejectReason),
                Deferred0;
            %% distant past:
            Age ->
                lager:warning(
                    "Received txn rejection from ancient, unacceptable past. "
                    "From ~b blocks ago. Ignoring: ~p",
                    [Age, Rejection]
                ),
                Deferred0
        end,
    State1 = State0#state{rejections_deferred = Deferred1},
    {noreply, State1};

handle_info({blockchain_event, {new_chain, NC}}, State) ->
    NewState = initialize_with_chain(State, NC),
    {noreply, NewState};

handle_info({blockchain_event, {add_block, _BlockHash, _Sync, _Ledger} = Event}, State0=#state{chain = undefined}) ->
    lager:info("received add block event whilst no chain and sync ~p.  Initializing chain and then handling block",[_Sync]),
    NC = blockchain_worker:blockchain(),
    State = initialize_with_chain(State0, NC),
    handle_add_block_event(Event, State#state{chain = NC});
handle_info({blockchain_event, {add_block, _BlockHash, Sync, Ledger} = Event}, State) ->
    lager:debug("received add block event, sync is ~p",[Sync]),
    %% update submit_f and reject_f per block, allow for num_consensus_members chain var updates
    {ok, N} = blockchain:config(?num_consensus_members, Ledger),
    handle_add_block_event(Event, State#state{submit_f = submit_f(N), reject_f = reject_f(N)});

handle_info(_Msg, State) ->
    lager:warning("blockchain_txn_mgr got unknown info msg: ~p", [_Msg]),
    {noreply, State}.

terminate(_Reason, _State) ->
    lager:debug("terminating with reason ~p", [_Reason]),
    %% stop dialers of cached txns
    [blockchain_txn_mgr_sup:stop_dialers(TxnData#txn_data.dialers) || {_TxnKey, _Txn, TxnData} <- cached_txns()],
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal functions
%% ------------------------------------------------------------------

-spec initialize_with_chain(#state{}, blockchain:blockchain()) -> #state{}.
initialize_with_chain(State, Chain)->
    {ok, Height} = blockchain:height(Chain),
    Ledger = blockchain:ledger(Chain),
    %% rewrite any cached txn from before we had a chain with the current block height, none of these will have been submitted as yet
    F = fun({TxnKey, Txn, TxnData}) ->
            ok = cache_txn(TxnKey, Txn, TxnData#txn_data{recv_block_height = Height})
        end,
    lists:foreach(F, cached_txns()),
    %% initialise submit_f and reject_f with current ledger value
    {ok, N} = blockchain:config(?num_consensus_members, Ledger),
    State#state{chain=Chain, cur_block_height = Height, submit_f = submit_f(N), reject_f = reject_f(N)}.

-spec handle_add_block_event({atom(), blockchain_block:hash(), boolean(),
                                blockchain_ledger_v1:ledger()}, #state{}) -> {noreply, #state{}}.
handle_add_block_event({add_block, BlockHash, Sync, _Ledger}, State=#state{chain = Chain,
                                                                           cur_block_height = CurBlockHeight})->
    #state{submit_f = SubmitF, chain = Chain} = State,
    case blockchain:get_block(BlockHash, Chain) of
        {ok, Block} ->
            Now = erlang:system_time(seconds),
            BlockTime = blockchain_block:time(Block),
            BlockAge = Now - BlockTime,
            HasBeenSynced = (Sync == false orelse BlockAge < ?RECENT_BLOCK_AGE) orelse State#state.has_been_synced,
            BlockHeight = blockchain_block:height(Block),
            %% purge any txns included in the new block from our cache
            ok = purge_block_txns_from_cache(Block),
            %% check if a new election occurred in this block
            %% If so we will only keep existing acceptions/rejections for rolled over members
            {IsNewElection, NewCGMembers} = check_block_for_new_election(Block),
            %% reprocess all txns remaining in the cache
            ok = process_cached_txns(Chain, BlockHeight, SubmitF, HasBeenSynced == false, IsNewElection, NewCGMembers),
            %% only update the current block height if its not a sync block
            NewCurBlockHeight = maybe_update_block_height(CurBlockHeight, BlockHeight, Sync),
            lager:debug("received block height: ~p,  updated state block height: ~p", [BlockHeight, NewCurBlockHeight]),
            State1 = State#state{cur_block_height = NewCurBlockHeight, has_been_synced=HasBeenSynced},
            State2 = process_deferred_rejections(State1),
            {noreply, State2};
        _ ->
            lager:error("failed to find block with hash: ~p", [BlockHash]),
            {noreply, State}
    end.

process_deferred_rejections(
    #state{
        rejections_deferred = Deferred0,
        reject_f            = RejectF,
        cur_block_height    = CurBlockHeight
    }=State
) ->
    IsPast    = fun({_, _, _, _, H, _}) -> H   < CurBlockHeight end,
    IsCurrent = fun({_, _, _, _, H, _}) -> H =:= CurBlockHeight end,
    {Current, Deferred1} = lists:partition(IsCurrent, Deferred0),
    {[]     , Deferred1} = lists:partition(IsPast   , Deferred1), % Sanity check
    lager:debug(
        "Processing deferred rejections. "
        "Count now current: ~b, count still deferred: ~b",
        [length(Current), length(Deferred1)]
    ),
    Reject =
        fun ({Dialer, TxnKey, Txn, Member, RejectorHeight, RejectReason}) ->
            ok = rejected(TxnKey, Txn, Member, Dialer, CurBlockHeight, RejectF, RejectorHeight, RejectReason)
        end,
    lists:foreach(Reject, Current),
    State#state{rejections_deferred=Deferred1}.

-spec purge_block_txns_from_cache(blockchain_block:block()) -> ok.
purge_block_txns_from_cache(Block)->
    MinedTxns = blockchain_block:transactions(Block),
    _ = lists:foldl(
        fun({TxnKey, Txn, #txn_data{callback=Callback, dialers=Dialers}}, Acc) ->
            %% keep a list of each cached txn we find in the block
            %% as we iterate over each cached txn, check each against this list
            %% if the cached txn does not appear in the list but it does appear in the block
            %% then we know its our first encounter with this txn
            %% We can involve the callback with success and append it to the accumulator list
            %% if we subsequently see another copy of the same txn in the accumulator list
            %% then we know its a dup and so we can involve the callback with an error
            %% this ensures that any original txn AND its dups are purged from the cache
            case {lists:member(Txn, MinedTxns), lists:member(Txn, Acc)} of
                {true, false} ->
                    %% the cached txn is in the block and not in our accumulator
                    %% invoke callback with success
                    ok = invoke_callback(Callback, ok),
                    ok = delete_cached_txn(TxnKey),
                    ok = blockchain_txn_mgr_sup:stop_dialers(Dialers),
                    [Txn | Acc];
                {true, true} ->
                    %% the cached txn is in the block and IS in our accumulator
                    %% invoke callback with dup error
                    ok = invoke_callback(Callback, {error, {invalid, duplicate_txn}}),
                    ok = delete_cached_txn(TxnKey),
                    ok = blockchain_txn_mgr_sup:stop_dialers(Dialers),
                    Acc;
                {false, _} ->
                    Acc
            end
        end, [], sorted_cached_txns()),
    ok.

-spec check_block_for_new_election(blockchain_block:block()) -> {boolean(), [libp2p_crypto:pubkey_bin()]}.
check_block_for_new_election(Block)->
    HasElectionFun = fun(T) -> blockchain_txn:type(T) == blockchain_txn_consensus_group_v1 end,
    case blockchain_utils:find_txn(Block, HasElectionFun) of
        [] ->
            {false, []};
        %% There can only be one election in a block
        [NewGroupTxn] ->
            {true, blockchain_txn_consensus_group_v1:members(NewGroupTxn)}
    end.

-spec maybe_update_block_height(undefined | integer(), integer(), boolean()) -> undefined | integer().
maybe_update_block_height(CurBlockHeight, _BlockHeight, true = _Sync) ->
    CurBlockHeight;
maybe_update_block_height(_CurBlockHeight, BlockHeight, _Sync) ->
    BlockHeight.

-spec invoke_callback(fun(), ok | {error, {invalid, atom()}} | {error, {invalid, {any}}} | {error, rejected} | {error, duplicate_key}) -> ok.
invoke_callback(Callback, Msg) ->
    spawn(fun() -> Callback(Msg) end),
    ok.

-spec signatory_rand_members(blockchain:blockchain(), integer(), [libp2p_crypto:pubkey_bin()], [libp2p_crypto:pubkey_bin()], dialers()) -> {ok, [libp2p_crypto:pubkey_bin()]}.
signatory_rand_members(Chain, SubmitF, Acceptions, Rejections, Dialers) ->
    {_, MembersBeingDialed} = lists:unzip(Dialers),
    {ok, PrevBlock} = blockchain:head_block(Chain),
    Signatories = [Signer || {Signer, _} <- blockchain_block:signatures(PrevBlock),
        not (Signer =:= blockchain_swarm:pubkey_bin())],
    case Signatories of
        [] ->
            %% rescue block! no signatures we can use
            %% so use a random consensus member
            Ledger = blockchain:ledger(Chain),
            {ok, Members0} = blockchain_ledger_v1:consensus_members(Ledger),
            Members = ((Members0 -- [blockchain_swarm:pubkey_bin()] -- Acceptions) -- Rejections) -- MembersBeingDialed,
            RandomMembers = blockchain_utils:shuffle(Members),
            {ok, lists:sublist(RandomMembers, SubmitF)};
        _ ->
            %% we have signatories
            RandomSignatories = ((blockchain_utils:shuffle(Signatories) -- Acceptions) -- Rejections) -- MembersBeingDialed,
            {ok, lists:sublist(RandomSignatories, SubmitF)}
    end.

-spec retry(txn_key(), blockchain_txn:txn(), pid()) -> ok.
retry(TxnKey, Txn, Dialer) ->
    case cached_txn(TxnKey) of
        {error, _} ->
            %% We no longer have this txn, do nothing
            ok;
        {ok, {TxnKey, Txn, #txn_data{dialers = Dialers} = TxnData}} ->
            RemainingDialers = lists:keydelete(Dialer, 1, Dialers),
            cache_txn(TxnKey, Txn, TxnData#txn_data{dialers = RemainingDialers})
    end.

-spec process_cached_txns(blockchain:blockchain(), undefined | integer(),
                integer(), boolean(), boolean(), [libp2p_crypto:pubkey_bin()]) -> ok.
process_cached_txns(_Chain, _CurBlockHeight, _SubmitF, true = _Sync, _IsNewElection, _NewGroupMember)->
    ok;
process_cached_txns(Chain, CurBlockHeight, SubmitF, _Sync, IsNewElection, NewGroupMembers)->
    %% get a sorted list of the cached txns
    CachedTxns = sorted_cached_txns(),
    {_, Txns, _} = lists:unzip3(CachedTxns),
    %% validate the cached txns
    {ValidTransactions, InvalidTransactions} = blockchain_txn:validate(Txns, Chain),
    ok = lists:foreach(
        fun({TxnKey, Txn, #txn_data{recv_block_height = RecvBlockHeight} = _TxnData} = CachedTxn) ->
            case {lists:keyfind(Txn, 1, InvalidTransactions), lists:member(Txn, ValidTransactions)} of
                {false, false} ->
                    %% the txn is not in the valid nor the invalid list
                    %% this means the validations cannot decide as yet, such as is the case with a
                    %% bad or out of sequence nonce
                    %% in this scenario we give the txn N blocks for the validation to make a decision
                    %% as to whether valid or invalid
                    %% if that does not happen by N blocks we will assume the txn will never become valid
                    %% this prevents an account becoming wedged such as would be the case if
                    %% a payer account submits a bunch of txns where a lower nonce txn got lost or
                    %% an issue with the speculative nonce
                    %% it also helps to prevent txn mgr's cache from getting cluttered long term
                    %% with txns which will never become valid or invalid
                    TxnMaxBlockSpan = application:get_env(blockchain, txn_max_block_span, 15),
                    case (CurBlockHeight - RecvBlockHeight) > TxnMaxBlockSpan of
                        true ->
                            lager:debug("txn has exceeded max block space, rejecting: ~p", [blockchain_txn:hash(Txn)]),
                            process_invalid_txn(CachedTxn, {error, {invalid, undecided_timeout}});
                        _ ->
                            lager:debug("txn has undecided validations, leaving in cache: ~p", [blockchain_txn:hash(Txn)]),
                            ok
                    end;
                {{Txn, _InvalidReason}, true} ->
                    %% hmm we have a txn which is a member of the valid and the invalid list
                    %% this can only mean we have dup txns, like 2 payment txns submitted with same nonce and payload
                    %% During validate, the first will be rendered valid, the second will be declared invalid
                    %% in this scenario we want to accept one of these txns, we dont really care which one
                    %% but the decision of which will be based on the txn with the earlier key ( keys are timestamps atm )
                    %% as we will hit this path at least twice ( once per duplicated txn ), the decision logic needs to be deterministic

                    %% find all elements in the sorted list of txns which match this txn payload
                    L1 = lists:filter(fun({_, LTxn, _})-> LTxn =:= Txn end, CachedTxns),
                    %% now sort those by key in ascending order
                    L2 = lists:sort(fun({KeyA, _, _}, {KeyB, _, _}) -> KeyA < KeyB end, L1),
                    %% if the head matches the current TxnKey then we will accept  this as the valid item otherwise invalid
                    case hd(L2) of
                        {TxnKey, _, _} ->
                            %% accept this copy of the txn as valid
                            process_valid_txn(Chain, CachedTxns, CachedTxn, SubmitF, NewGroupMembers,
                                                CurBlockHeight, IsNewElection);
                        _ ->
                            %% declare this copy as invalid
                            process_invalid_txn(CachedTxn, {error, {invalid, duplicate_txn}})
                    end;
                {{Txn, InvalidReason}, _} ->
                    %% the txn is invalid
                    process_invalid_txn(CachedTxn, {error, {invalid, InvalidReason}});
                {_, true} ->
                    %% the txn is valid and a new election may or may not have occurred
                    process_valid_txn(Chain, CachedTxns, CachedTxn, SubmitF, NewGroupMembers,
                                        CurBlockHeight, IsNewElection)
            end
        end, CachedTxns).

-spec process_invalid_txn(cached_txn_type(), {error, {invalid, atom()}} | {error, {invalid, {any}}})-> ok.
process_invalid_txn({TxnKey, Txn, TxnData}, CallbackResponse) ->
    %% the txn is invalid, remove from cache and invoke callback
    %% any txn in the invalid list is considered unrecoverable, it will never become valid
    %% stop all existing dialers for the txn
    lager:info("txn declared invalid with reason ~p, removing from cache and invoking callback: ~p",[CallbackResponse, blockchain_txn:hash(Txn)]),
    #txn_data{callback = Callback, dialers = Dialers} = TxnData,
    ok = blockchain_txn_mgr_sup:stop_dialers(Dialers),
    ok = invoke_callback(Callback, CallbackResponse),
    delete_cached_txn(TxnKey).

-spec process_valid_txn(blockchain:blockchain(), [cached_txn_type()], cached_txn_type(),
                            undefined | integer(), [libp2p_crypto:pubkey_bin()], integer(), boolean())-> ok.
process_valid_txn(Chain, CachedTxns, {TxnKey, Txn, TxnData}, SubmitF, NewGroupMembers,
                    CurBlockHeight, IsNewElection) when IsNewElection == true ->
    %% the txn is valid and a new election has occurred, so keep txn in cache and resubmit
    %% keep any existing acceptions/rejections from the previous round
    lager:debug("txn with key ~p is valid and there is a new election: ~p.  Checking if it needs to be resubmitted", [TxnKey, blockchain_txn:hash(Txn)]),
    #txn_data{acceptions = Acceptions, rejections = Rejections,
              recv_block_height = RecvBlockHeight, dialers = Dialers} = TxnData,
    RecvBlockHeight0 = normalise_block_height(CurBlockHeight, RecvBlockHeight),
    %% figure out which dialers still point to members of the new consensus group
    {RemainingDialers, StaleDialers} = lists:partition(fun({_DialerPid, DialerMember}) ->
                                            lists:member(DialerMember, NewGroupMembers)
                                    end, Dialers),
    %% stop all the dialers to members no longer in the consensus group
    ok = blockchain_txn_mgr_sup:stop_dialers(StaleDialers),
    %% remove any acceptions and rejections from members no longer in the consensus group
    {NewAcceptions, NewRejections} = purge_old_cg_members(Acceptions, Rejections, NewGroupMembers),
    %% check if the txn has any dependencies and resubmit as required
    check_for_deps_and_resubmit(TxnKey, Txn, CachedTxns, Chain, SubmitF,
                                TxnData#txn_data{recv_block_height = RecvBlockHeight0,
                                                 acceptions = NewAcceptions,
                                                 rejections = NewRejections,
                                                 dialers = RemainingDialers});
process_valid_txn(Chain, CachedTxns, {TxnKey, Txn, TxnData}, SubmitF, _NewGroupMembers,
                    CurBlockHeight, _IsNewElection) ->
    %% the txn is valid and there has not been a new election
    %% if we dont have sufficient acceptions at this point, resubmit to additional members
    lager:debug("txn with key ~p is valid and there is NO new election: ~p.  Checking if it needs to be resubmitted", [TxnKey, blockchain_txn:hash(Txn)]),
    #txn_data{acceptions = Acceptions, rejections = Rejections,
              recv_block_height = RecvBlockHeight, dialers = Dialers} = TxnData,
    case length(Dialers) < (SubmitF - length(Acceptions)) of
        true ->
            RecvBlockHeight0 = normalise_block_height(CurBlockHeight, RecvBlockHeight),
            %% check if the txn has any dependencies and resubmit as required
            check_for_deps_and_resubmit(TxnKey, Txn, CachedTxns, Chain, SubmitF,
                                        TxnData#txn_data{recv_block_height = RecvBlockHeight0});
        false ->
            %% the txn remains valid and the txn has sufficient acceptions, so do nothing
            lager:debug("txn is valid but no need to resubmit to new or additional members: ~p Accepted: ~p Rejected ~p Dialers ~p F ~p",
                [blockchain_txn:hash(Txn), length(Acceptions), length(Rejections), Dialers, SubmitF]),
            ok
    end.



-spec check_for_deps_and_resubmit(txn_key(), blockchain_txn:txn(), [blockchain_txn:txn()], blockchain:blockchain(), integer(), #txn_data{}) -> ok.
check_for_deps_and_resubmit(TxnKey, Txn, CachedTxns, Chain, SubmitF, #txn_data{ acceptions = Acceptions,
                                                                                rejections = Rejections,
                                                                                dialers = Dialers} = TxnData)->
    %% check if this transaction has any dependencies
    %% figure out what, if anything, this transaction depends on
    case lists:filter(fun({DepTxnKey, _DepTxn, _DepTxnData}) -> cached_txn(DepTxnKey) /= {error, txn_not_found} end, blockchain_txn:depends_on(Txn, CachedTxns)) of
        [] ->
            %% NOTE: we assume we have correct dependency resolution here, if you add a new transaction with
            %% dependencies and don't fix depends_on, your transaction will probably get rejected
            NewDialers = submit_txn_to_cg(Chain, TxnKey, Txn, SubmitF, Acceptions, Rejections, Dialers),
            lager:info("Resubmitting txn: ~p to ~b new dialers", [blockchain_txn:hash(Txn), length(NewDialers)]),
            cache_txn(TxnKey, Txn, TxnData#txn_data{dialers = Dialers ++ NewDialers});
        Dependencies ->
            %% for txns with dep txns, we only want to submit to members which have accepted one of the dep txns previously
            %% so we need to build up an explicit set of elegible members rather than sending to random CG members
            %% eligible members will be those members which have accepted the dependant upon txn or if its not yet
            %% accepted the members to which it has been submitted ( the dialled list )
            {Dep1TxnKey, _Dep1Txn, _Dep1TxnData0} = hd(Dependencies),
            {ok, {_, _, #txn_data{acceptions = Dep1TxnAcceptions, dialers = Dep1TxnDialers} = _Dep1TxnData1}} = cached_txn(Dep1TxnKey),
            A0 =
                case Dep1TxnAcceptions  of
                    [] ->
                        [Dep1TxnDialedMember || {_, Dep1TxnDialedMember} <- Dep1TxnDialers];
                    Dep1TxnAccs ->
                        Dep1TxnAccs
                end,

            ElegibleMembers = sets:to_list(lists:foldl(fun({Dep2TxnKey, _Dep2Txn, _Dep2TxnData}, Acc) ->
                                                                {ok, {_, _, #txn_data{acceptions = Dep2TxnAcceptions, dialers = Dep2TxnDialers} = _Dep2TxnData1}} = cached_txn(Dep2TxnKey),
                                                                A1 =
                                                                    case Dep2TxnAcceptions  of
                                                                        [] ->
                                                                            [Dep2TxnDialedMember || {_, Dep2TxnDialedMember} <- Dep2TxnDialers];
                                                                        Dep2TxnAccs ->
                                                                            Dep2TxnAccs
                                                                    end,
                                                               sets:intersection(Acc, sets:from_list(A1))
                                                       end, sets:from_list(A0), tl(Dependencies))),
            lager:debug("txn ~p has eligible members: ~p", [blockchain_txn:hash(Txn), ElegibleMembers]),
            {_, ExistingDialers} = lists:unzip(Dialers),
            %% remove any CG members from the elegible list which have already accepted the txn and also
            %% those which we are already dialling
            %% dont exclude any previous rejectors as the reason rejected may no longer be valid
            %% previous rejectors may include members which dependant upon txn has now been accepted by
            ElegibleMembers1 = (ElegibleMembers -- Acceptions) -- ExistingDialers,
            %% determine max number of new diallers we need to start and then use this to get our target list to dial
            MaxNewDiallersCount = SubmitF - length(Acceptions) - length(Dialers),
            NewDialers = dial_members(lists:sublist(ElegibleMembers1, MaxNewDiallersCount), Chain, TxnKey, Txn),
            lager:debug("txn ~p depends on ~p other txns, can dial ~p members and dialed ~p", [blockchain_txn:hash(Txn), length(Dependencies), length(ElegibleMembers1), length(NewDialers)]),
            cache_txn(TxnKey, Txn, TxnData#txn_data{dialers =  Dialers ++ NewDialers})
    end.

-spec purge_old_cg_members([libp2p_crypto:pubkey_bin()], [libp2p_crypto:pubkey_bin()],
                                  [libp2p_crypto:pubkey_bin()]) -> {[libp2p_crypto:pubkey_bin()], [libp2p_crypto:pubkey_bin()]}.
purge_old_cg_members(Acceptions0, Rejections0, NewGroupMembers) ->
    Acceptions = [ {M, H, QP} || {M, H, QP} <- NewGroupMembers, lists:key_member(M, 1, Acceptions0) == true ],
    Rejections = [ {M, H, R} || {M, H, R} <- NewGroupMembers, lists:key_member(M, 1, Rejections0) == true ],
    {Acceptions, Rejections}.

-spec accepted(txn_key(), blockchain_txn:txn(), libp2p_crypto:pubkey_bin(), pid(), pos_integer() | undefined, non_neg_integer() | undefined) -> ok.
accepted(TxnKey, Txn, Member, Dialer, Height, QueuePos) ->
    %% stop the dialer which accepted the txn, we dont have any further use for it
    ok = blockchain_txn_mgr_sup:stop_dialer(Dialer),
    case cached_txn(TxnKey) of
        {error, _} ->
            %% We no longer have this txn, do nothing
            lager:debug("cannot find accepted txn ~p with dialer ~p", [Txn, Dialer]),
            ok;
        {ok, {TxnKey, Txn, #txn_data{acceptions = Acceptions, dialers = Dialers} = TxnData}} ->
            case lists:keymember(Dialer, 1, Dialers) of
                false ->
                    %% some kind of orphaned dialer
                    lager:warning("got accept response from orphaned dialer ~p", [Dialer]),
                    ok;
                true ->
                    %% add the member to the accepted list, so we avoid potentially resubmitting to same one again later
                    cache_txn(TxnKey, Txn, TxnData#txn_data{ acceptions = lists:keysort(1, [{Member, Height, QueuePos} |Acceptions]),
                                                    dialers = lists:keydelete(Dialer, 1, Dialers)})
            end
    end.

-spec rejected(txn_key(), blockchain_txn:txn(), libp2p_crypto:pubkey_bin(), pid(), undefined | integer(), integer(), pos_integer(), atom()) -> ok.
rejected(TxnKey, Txn, Member, Dialer, CurBlockHeight, RejectF, RejectorHeight, RejectReason) ->
    %% stop the dialer which rejected the txn
    ok = blockchain_txn_mgr_sup:stop_dialer(Dialer),
    case cached_txn(TxnKey) of
        {error, _} ->
            lager:debug("cannot find rejected txn ~p with dialer ~p", [Txn, Dialer]),
            %% We no longer have this txn, do nothing
            ok;
        {ok, {TxnKey, Txn, #txn_data{rejections = Rejections, dialers = Dialers} = TxnData}} ->
            case lists:keymember(Dialer, 1, Dialers) of
                false ->
                    %% some kind of orphaned dialer
                    lager:warning("got reject response from orphaned dialer ~p", [Dialer]),
                    ok;
                true ->
                    %% add the member to the rejections list, so we avoid resubmitting to one which already rejected
                    NewTxnData = TxnData#txn_data{  rejections = lists:keysort(1, [{Member, RejectorHeight, RejectReason} |Rejections]),
                                                    dialers = lists:keydelete(Dialer, 1, Dialers)},
                    reject_actions({TxnKey, Txn, NewTxnData}, RejectF, CurBlockHeight)
             end
    end.

%% txn has exceeded the max number of rejections
%% delete it and invoke callback
-spec reject_actions(cached_txn_type(), integer(), integer()) -> ok.
reject_actions({TxnKey, _Txn, #txn_data{callback = Callback, dialers = Dialers, rejections = _Rejections}},
                RejectF,
                _CurBlockHeight)
    when length(_Rejections) > RejectF ->
    %% txn has been exceeded our max rejection count
    %% TODO pass reject reason to callback
    ok = invoke_callback(Callback, {error, rejected}),
    ok = blockchain_txn_mgr_sup:stop_dialers(Dialers),
    delete_cached_txn(TxnKey);
%% the txn has been rejected but has not yet exceeded the max number of rejections,
%% so resend to another CG member
reject_actions({TxnKey, Txn, TxnData},
                _RejectF,
                _CurBlockHeight) ->
    cache_txn(TxnKey, Txn, TxnData).

-spec submit_txn_to_cg(blockchain:blockchain(), txn_key(), blockchain_txn:txn(), integer(), [libp2p_crypto:pubkey_bin()], [libp2p_crypto:pubkey_bin()], dialers()) -> dialers().
submit_txn_to_cg(Chain, TxnKey, Txn, SubmitCount, Acceptions, Rejections, Dialers)->
    {ok, Members} = signatory_rand_members(Chain, SubmitCount, Acceptions, Rejections, Dialers),
    dial_members(Members, Chain, TxnKey, Txn).

-spec dial_members([libp2p_crypto:pubkey_bin()], blockchain:blockchain(), txn_key(), blockchain_txn:txn()) -> dialers().
dial_members(Members, Chain, TxnKey, Txn)->
    dial_members(Members, Chain, TxnKey, Txn, []).

-spec dial_members([libp2p_crypto:pubkey_bin()], blockchain:blockchain(), txn_key(), blockchain_txn:txn(), dialers()) -> dialers().
dial_members([], _Chain, _TxnKey, _Txn, AccDialers)->
    AccDialers;
dial_members([Member | Rest], Chain, TxnKey, Txn, AccDialers)->
    {ok, Dialer} = blockchain_txn_mgr_sup:start_dialer([self(), TxnKey, Txn, Member]),
    ok = blockchain_txn_dialer:dial(Dialer),
    dial_members(Rest, Chain, TxnKey, Txn, [{Dialer, Member} | AccDialers]).

-spec cache_txn(txn_key(), blockchain_txn:txn(), #txn_data{}) -> ok.
cache_txn(Key, Txn, TxnDataRec) ->
    true = ets:insert(?TXN_CACHE, {Key, Txn, TxnDataRec}),
    ok.

-spec delete_cached_txn(txn_key())-> ok.
delete_cached_txn(Key) ->
    true = ets:delete(?TXN_CACHE, Key),
    ok.

-spec cached_txn(txn_key())-> {ok, cached_txn_type()} | {error, txn_not_found}.
cached_txn(Key)->
    case ets:lookup(?TXN_CACHE, Key) of
        [Res] -> {ok, Res};
        _ -> {error, txn_not_found}
    end.

-spec cached_txns()-> [cached_txn_type()].
cached_txns()->
    ets:tab2list(?TXN_CACHE).

-spec sorted_cached_txns()-> [] | [cached_txn_type()].
sorted_cached_txns()->
    TxnList = ets:tab2list(?TXN_CACHE),
    sort_txns(TxnList).

-spec sort_txns([cached_txn_type()]) -> [cached_txn_type()].
sort_txns(Txns)->
    lists:sort(fun({_, TxnA, _}, {_, TxnB, _}) -> blockchain_txn:sort(TxnA, TxnB) end, Txns).

-spec normalise_block_height(integer(), undefined | integer()) -> integer().
normalise_block_height(CurBlockHeight, undefined)->
    CurBlockHeight;
normalise_block_height(_CurBlockHeight, RecvBlockHeight)->
    RecvBlockHeight.

-spec submit_f(integer()) -> integer().
submit_f(NumMembers)->
    %% F/2+1
    trunc(((NumMembers - 1) div 3 ) / 2) + 1.

-spec reject_f(integer()) -> integer().
reject_f(NumMembers)->
    %% 2F+1
    (trunc((NumMembers) div 3) * 2) + 1.

-spec get_txn_key()-> blockchain_txn_mgr:txn_key().
get_txn_key()->
    integer_to_binary(erlang:monotonic_time()).
