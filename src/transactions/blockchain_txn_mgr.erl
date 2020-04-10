%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Mgr ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_mgr).

-behavior(gen_server).
-include("blockchain.hrl").
-define(TXN_CACHE, txn_cache).
%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------
-export([
         start_link/1,
         submit/2,
         set_chain/1,
         txn_list/0,
         make_ets_table/0
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

-record(state, {
          submit_f :: undefined | integer(),
          reject_f :: undefined | integer(),
          cur_block_height :: undefined | integer(),
          txn_cache :: undefined | ets:tid(),
          chain :: undefined | blockchain:blockchain()
         }).

-type cached_txn_type() :: {Txn :: blockchain_txn:txn(),
                                {Callback :: fun(), RecvBlockHeight :: undefined | integer(),
                                Acceptions :: [libp2p_crypto:pubkey_bin()],
                                Rejections :: [libp2p_crypto:pubkey_bin()],
                                Dialers :: undefined | [pid()]}}.


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
    gen_server:cast(?MODULE, {submit, Txn, Callback}).

-spec set_chain(blockchain:blockchain()) -> ok.
set_chain(Chain) ->
    gen_server:cast(?MODULE, {set_chain, Chain}).

-spec txn_list() -> [cached_txn_type()].
txn_list() ->
    gen_server:call(?MODULE, txn_list, infinity).

make_ets_table() ->
    ets:new(?TXN_CACHE,
            [named_table,
             {heir, self(), undefined},
             {write_concurrency, true},
             {read_concurrency, true}]).

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
    ok = blockchain_event:add_handler(self()),
    {ok, #state{txn_cache = TxnCache}}.

handle_cast({set_chain, Chain}, State=#state{chain = undefined}) ->
    NewState = initialize_with_chain(State, Chain),
    {noreply, NewState};

handle_cast({submit, Txn, Callback}, State=#state{chain = undefined}) ->
    %% Got txn when there is no chain, keep it in the cache and process when its available
    %% as no chain we dont have any height data, so cache it with height = undefined
    %% we will update when the chain is set and we submit these cached txns
    ok = cache_txn(Txn, Callback, undefined, [], [],[]),
    {noreply, State};

handle_cast({submit, Txn, Callback}, State=#state{chain = Chain,
                                                  cur_block_height = H,
                                                  submit_f = SubmitF}) ->
    %% send the txn to consensus group
    lager:debug("submitting txn to cg: ~s", [blockchain_txn:print(Txn)]),
    Dialers = submit_txn_to_cg(Chain, Txn, SubmitF, [], []),
    ok = cache_txn(Txn, Callback, H, [], [], Dialers),
    {noreply, State};

handle_cast(_Msg, State) ->
    lager:warning("blockchain_txn_mgr got unknown cast: ~p", [_Msg]),
    {noreply, State}.

handle_call(txn_list, _, State) ->
    {reply, cached_txns(), State};

handle_call(_Msg, _From, State) ->
    lager:warning("blockchain_txn_mgr got unknown call: ~p, From: ~p", [_Msg, _From]),
    {reply, ok, State}.

handle_info({no_group, {Dialer, Txn, Member}}, #state{chain = Chain, submit_f = SubmitF} = State) ->
    lager:info("txn: ~s, no group: ~p, Dialer: ~p", [blockchain_txn:print(Txn), Member, Dialer]),
    ok = retry(Txn, Chain, SubmitF),
    {noreply, State};

handle_info({dial_failed, {Dialer, Txn, Member}}, #state{chain = Chain, submit_f = SubmitF} = State) ->
    lager:debug("txn: ~s, dial_failed: ~p, Dialer: ~p", [blockchain_txn:print(Txn), Member, Dialer]),
    ok = retry(Txn, Chain, SubmitF),
    {noreply, State};

handle_info({send_failed, {Dialer, Txn, Member}}, #state{chain = Chain, submit_f = SubmitF} = State) ->
    lager:debug("txn: ~s, send_failed: ~p, Dialer: ~p", [blockchain_txn:print(Txn), Member, Dialer]),
    ok = retry(Txn, Chain, SubmitF),
    {noreply, State};

handle_info({accepted, {Dialer, Txn, Member}}, State) ->
    lager:debug("txn: ~s, accepted_by: ~p, Dialer: ~p", [blockchain_txn:print(Txn), Member, Dialer]),
    ok = accepted(Txn, Member, Dialer),
    {noreply, State};

handle_info({rejected, {Dialer, Txn, Member}}, #state{chain = Chain, cur_block_height = CurBlockHeight,
                                                        reject_f = RejectF} = State) ->
    lager:debug("txn: ~s, rejected_by: ~p, Dialer: ~p", [blockchain_txn:print(Txn), Member, Dialer]),
    ok = rejected(Chain, Txn, Member, Dialer, CurBlockHeight, RejectF),
    {noreply, State};

handle_info({blockchain_event, {new_chain, NC}}, State) ->
    NewState = initialize_with_chain(State, NC),
    {noreply, NewState};

handle_info({blockchain_event, {add_block, _BlockHash, _Sync, _Ledger} = Event}, State0=#state{chain = undefined}) ->
    lager:info("received add block event whilst no chain.  Initializing chain and then handling block"),
    NC = blockchain_worker:blockchain(),
    State = initialize_with_chain(State0, NC),
    handle_add_block_event(Event, State#state{chain = NC});
handle_info({blockchain_event, {add_block, _BlockHash, _Sync, _Ledger} = Event}, State) ->
    lager:debug("received add block event"),
    handle_add_block_event(Event, State);

handle_info(_Msg, State) ->
    lager:warning("blockchain_txn_mgr got unknown info msg: ~p", [_Msg]),
    {noreply, State}.

terminate(_Reason, _State) ->
    lager:debug("terminating with reason ~p", [_Reason]),
    %% stop dialers of cached txns
    catch [blockchain_txn_mgr_sup:stop_dialers(Dialers) || {_Txn, {_, _, _, _, Dialers}} <- cached_txns()],
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal functions
%% ------------------------------------------------------------------

-spec initialize_with_chain(#state{}, blockchain:blockchain()) -> #state{}.
initialize_with_chain(State, Chain)->
    {ok, Height} = blockchain:height(Chain),
    {ok, PrevBlock} = blockchain:head_block(Chain),
    Signatories = [Signer || {Signer, _} <- blockchain_block:signatures(PrevBlock)],
    SubmitF = submit_f(length(Signatories)),
    RejectF = reject_f(length(Signatories)),
    %% process any cached txn from before we had a chain, none of these will have been submitted as yet
    F = fun({Txn, {Callback, _RecvBlockHeight, _Acceptions, _Rejections, _Dialers}}) ->
            NewDialers = submit_txn_to_cg(Chain, Txn, SubmitF, [], []),
            ok = cache_txn(Txn, Callback, Height, [], [], NewDialers)
        end,
    lists:foreach(F, cached_txns()),
    State#state{chain=Chain, cur_block_height = Height, submit_f = SubmitF, reject_f = RejectF}.

-spec handle_add_block_event({atom(), blockchain_block:hash(), boolean(),
                                blockchain_ledger_v1:ledger()}, #state{}) -> {noreply, #state{}}.
handle_add_block_event({add_block, BlockHash, _Sync, _Ledger}, State=#state{chain = Chain})->
    #state{submit_f = SubmitF, chain = Chain} = State,
    case blockchain:get_block(BlockHash, Chain) of
        {ok, Block} ->
            BlockHeight = blockchain_block:height(Block),
            %% purge any txns included in the new block from our cache
            ok = purge_block_txns_from_cache(Block),
            %% check if a new election occurred in this block
            %% If so we will only keep existing acceptions/rejections for rolled over members
            {IsNewElection, NewCGMembers} = check_block_for_new_election(Block),
            %% reprocess all txns remaining in the cache
            ok = process_cached_txns(Chain, BlockHeight, SubmitF, IsNewElection, NewCGMembers),
            {noreply, State#state{cur_block_height = BlockHeight}};
        _ ->
            lager:error("failed to find block with hash: ~p", [BlockHash]),
            {noreply, State}
    end.

-spec purge_block_txns_from_cache(blockchain_block:block()) -> ok.
purge_block_txns_from_cache(Block)->
    MinedTxns = blockchain_block:transactions(Block),
    ok = lists:foreach(
        fun({Txn, {Callback, _RecvBlockHeight, _Acceptions, _Rejections, Dialers}}) ->
            case lists:member(Txn, MinedTxns) of
                true ->
                    %% txn has been mined in last block
                    ok = blockchain_txn_mgr_sup:stop_dialers(Dialers),
                    ok = invoke_callback(Callback, ok),
                    delete_cached_txn(Txn);
                false ->
                    noop
            end
        end, sorted_cached_txns()).

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

-spec invoke_callback(fun(), ok | {error, invalid} | {error, rejected}) -> ok.
invoke_callback(Callback, Msg) ->
    spawn(fun() -> Callback(Msg) end),
    ok.

-spec signatory_rand_members(blockchain:blockchain(), integer(), [libp2p_crypto:pubkey_bin()], [libp2p_crypto:pubkey_bin()]) -> {ok, [libp2p_crypto:pubkey_bin()]}.
signatory_rand_members(Chain, SubmitF, Acceptions, Rejections) ->
    {ok, PrevBlock} = blockchain:head_block(Chain),
    Signatories = [Signer || {Signer, _} <- blockchain_block:signatures(PrevBlock),
        not (Signer =:= blockchain_swarm:pubkey_bin())],
    case Signatories of
        [] ->
            %% rescue block! no signatures we can use
            %% so use a random consensus member
            Ledger = blockchain:ledger(Chain),
            {ok, Members0} = blockchain_ledger_v1:consensus_members(Ledger),
            Members = (Members0 -- [blockchain_swarm:pubkey_bin()] -- Acceptions) -- Rejections,
            RandomMembers = blockchain_utils:shuffle(Members),
            {ok, lists:sublist(RandomMembers, SubmitF)};
        _ ->
            %% we have signatories
            RandomSignatories = (blockchain_utils:shuffle(Signatories) -- Acceptions) -- Rejections,
            {ok, lists:sublist(RandomSignatories, SubmitF)}
    end.

-spec retry(blockchain_txn:txn(), blockchain:blockchain(), integer()) -> ok.
retry(Txn, Chain, SubmitF) ->
    case cached_txn(Txn) of
        {error, _} ->
            %% We no longer have this txn, do nothing
            ok;
        {ok, {Txn, {Callback, RecvBlockHeight, Acceptions, Rejections, _Dialers}}} ->
            %% resubmit the txn to CG
            NewDialers = submit_txn_to_cg(Chain, Txn, SubmitF, Acceptions, Rejections),
            cache_txn(Txn, Callback, RecvBlockHeight, Acceptions, Rejections, NewDialers)
    end.

-spec process_cached_txns(blockchain:blockchain(), undefined | integer(),
                          integer(), boolean(), [libp2p_crypto:pubkey_bin()]) -> ok.
process_cached_txns(Chain, CurBlockHeight, SubmitF, IsNewElection, NewGroupMembers)->
    %% get a sorted list of the cached txns
    CachedTxns = sorted_cached_txns(),
    {Txns, _} = lists:unzip(CachedTxns),
    %% validate the cached txns
    {ValidTransactions, InvalidTransactions} = blockchain_txn:validate(Txns, Chain),
    ok = lists:foreach(
        fun({Txn, {Callback, RecvBlockHeight, Acceptions, Rejections, Dialers}}) ->
            case {lists:member(Txn, InvalidTransactions), lists:member(Txn, ValidTransactions)} of
                {false, false} ->
                    %% the txn is not in the valid nor the invalid list
                    %% this means the validations cannot decide as yet, such as is the case with a bad or out of sequence nonce
                    %% stop the dialers as we dont know when this will be resubmitted
                    ok = blockchain_txn_mgr_sup:stop_dialers(Dialers),
                    %% update the txn in the cache to remove the now stopped dialers
                    cache_txn(Txn, Callback, RecvBlockHeight, Acceptions, Rejections, []);
                {true, _} ->
                    %% the txn is invalid, remove from cache and invoke callback
                    %% any txn in the invalid list is considered unrecoverable, it will never become valid
                    %% stop all existing dialers for the txn
                    lager:info("Invalidated txn: ~p", [blockchain_txn:hash(Txn)]),
                    ok = blockchain_txn_mgr_sup:stop_dialers(Dialers),
                    ok = invoke_callback(Callback, {error, invalid}),
                    delete_cached_txn(Txn);
                {_, true} ->
                    case IsNewElection orelse length(Dialers) < (SubmitF - length(Acceptions)) of
                        true ->
                            %% the txn is valid and a new election has occurred, so keep txn in cache and resubmit
                            %% keep any existing acceptions/rejections from the previous round
                            ok = blockchain_txn_mgr_sup:stop_dialers(Dialers),
                            RecvBlockHeight0 = normalise_block_height(CurBlockHeight, RecvBlockHeight),
                            lager:info("Resubmitting txn: ~p", [blockchain_txn:hash(Txn)]),
                            {NewAcceptions, NewRejections} = purge_old_cg_members(Acceptions, Rejections, NewGroupMembers),
                            NewDialers = submit_txn_to_cg(Chain, Txn, SubmitF, NewAcceptions, NewRejections),
                            cache_txn(Txn, Callback, RecvBlockHeight0, NewAcceptions, NewRejections, NewDialers);
                        false ->
                            ok
                    end
            end
        end, CachedTxns).

-spec purge_old_cg_members([libp2p_crypto:pubkey_bin()], [libp2p_crypto:pubkey_bin()],
                                  [libp2p_crypto:pubkey_bin()]) -> {[libp2p_crypto:pubkey_bin()], [libp2p_crypto:pubkey_bin()]}.
purge_old_cg_members(Acceptions0, Rejections0, NewGroupMembers) ->
    Acceptions = [ M || M <- NewGroupMembers, lists:member(M,Acceptions0) == true ],
    Rejections = [ M || M <- NewGroupMembers, lists:member(M,Rejections0) == true ],
    {Acceptions, Rejections}.

-spec accepted(blockchain_txn:txn(), libp2p_crypto:pubkey_bin(), pid()) -> ok.
accepted(Txn, Member, Dialer) ->
    %% stop the dialer which accepted the txn, we dont have any further use for it
    ok = blockchain_txn_mgr_sup:stop_dialer(Dialer),
    case cached_txn(Txn) of
        {error, _} ->
            %% We no longer have this txn, do nothing
            ok;
        {ok, {Txn, {Callback, RecvBlockHeight, Acceptions, Rejections, Dialers}}} ->
            %% add the member to the accepted list, so we avoid potentially resubmitting to same one again later
            cache_txn(Txn, Callback, RecvBlockHeight, lists:usort([Member|Acceptions]), Rejections, Dialers -- Dialer)
    end.

-spec rejected(blockchain:blockchain(), blockchain_txn:txn(), libp2p_crypto:pubkey_bin(), pid(), undefined | integer(), integer()) -> ok.
rejected(Chain, Txn, Member, Dialer, CurBlockHeight, RejectF) ->
    %% stop the dialer which rejected the txn
    ok = blockchain_txn_mgr_sup:stop_dialer(Dialer),
    case cached_txn(Txn) of
        {error, _} ->
            %% We no longer have this txn, do nothing
            ok;
        {ok, {Txn, {Callback, RecvBlockHeight, Acceptions, Rejections, Dialers}}} ->
            %% add the member to the rejections list, so we avoid resubmitting to one which already rejected
            UpdatedTxnPayload = {Txn, {Callback, RecvBlockHeight, Acceptions, lists:usort([Member|Rejections]), Dialers -- Dialer}},
            reject_actions(Chain, UpdatedTxnPayload, RejectF, CurBlockHeight)
    end.

%% txn has exceeded the max number of rejections
%% delete it and invoke callback
-spec reject_actions(blockchain:blockchain(), cached_txn_type(), integer(), integer()) -> ok.
reject_actions(_Chain,
                {Txn, {Callback, _RecvBlockHeight, _Acceptions, _Rejections, _Dialers}},
                RejectF,
                _CurBlockHeight)
    when length(_Rejections) > RejectF ->
    %% txn has been exceeded our max rejection count
    ok = invoke_callback(Callback, {error, rejected}),
    delete_cached_txn(Txn);
%% the txn has been rejected but has not yet exceeded the max number of rejections,
%% so resend to another CG member
reject_actions(Chain,
                {Txn, {Callback, RecvBlockHeight, Acceptions, Rejections, Dialers}},
                _RejectF,
                _CurBlockHeight) ->
    [NewDialer] = submit_txn_to_cg(Chain, Txn, 1, Acceptions, Rejections),
    cache_txn(Txn, Callback, RecvBlockHeight, Acceptions, Rejections, [NewDialer | Dialers]).

-spec submit_txn_to_cg(blockchain:blockchain(), blockchain_txn:txn(), integer(), [libp2p_crypto:pubkey_bin()], [libp2p_crypto:pubkey_bin()]) -> [pid()].
submit_txn_to_cg(Chain, Txn, SubmitCount, Acceptions, Rejections)->
    {ok, Members} = signatory_rand_members(Chain, SubmitCount, Acceptions, Rejections),
    dial_members(Members, Chain, Txn).

-spec dial_members([libp2p_crypto:pubkey_bin()], blockchain:blockchain(), blockchain_txn:txn(), [pid()]) -> [pid()].
dial_members(Members, Chain, Txn)->
    dial_members(Members, Chain, Txn, []).
dial_members([], _Chain, _Txn, AccDialers)->
    AccDialers;
dial_members([Member | Rest], Chain, Txn, AccDialers)->
    {ok, Dialer} = blockchain_txn_mgr_sup:start_dialer([self(), Txn, Member]),
    ok = blockchain_txn_dialer:dial(Dialer),
    dial_members(Rest, Chain, Txn, [Dialer | AccDialers]).

-spec cache_txn(blockchain_txn:txn(), fun(), undefined | integer(), [libp2p_crypto:pubkey_bin()], [libp2p_crypto:pubkey_bin()], [pid()]) -> ok.
cache_txn(Txn, Callback, RecvBlockHeight, Acceptions, Rejections, Dialers) ->
    true = ets:insert(?TXN_CACHE, {Txn, {Callback, RecvBlockHeight, Acceptions, Rejections, Dialers}}),
    ok.

-spec delete_cached_txn(blockchain_txn:txn())-> ok.
delete_cached_txn(Txn) ->
    true = ets:delete(?TXN_CACHE, Txn),
    ok.

-spec cached_txn(blockchain_txn:txn())-> {ok, cached_txn_type()} | {error, txn_not_found}.
cached_txn(Txn)->
    case ets:lookup(?TXN_CACHE, Txn) of
        [Txn] -> {ok, Txn};
        _ -> {error, txn_not_found}
    end.

-spec cached_txns()-> [cached_txn_type()].
cached_txns()->
    ets:tab2list(?TXN_CACHE).

-spec sorted_cached_txns()-> [] | [cached_txn_type()].
sorted_cached_txns()->
    TxnList = ets:tab2list(?TXN_CACHE),
    sort_txns(TxnList).

-spec sort_txns([blockchain_txn:txn()]) -> [blockchain_txn:txn()].
sort_txns(Txns)->
    lists:sort(fun({TxnA, _}, {TxnB, _}) -> blockchain_txn:sort(TxnA, TxnB) end, Txns).

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
