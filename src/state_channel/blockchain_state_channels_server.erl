%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channels Server ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channels_server).

-behavior(gen_server).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------
-export([
    start_link/1,
    credits/1, nonce/1,
    request/1,
    burn/2
]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-include("blockchain.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(SERVER, ?MODULE).
-define(STATE_CHANNELS, <<"blockchain_state_channels_server.STATE_CHANNELS">>).

-record(state, {
    db :: rocksdb:db_handle() | undefined,
    swarm :: pid(),
    state_channels = #{} :: #{blockchain_state_channel_v1:id() => blockchain_state_channel_v1:state_channel()},
    clients = #{} :: clients(),

    payees_to_sc = #{} :: #{libp2p_crypto:pubkey_bin() => blockchain_state_channel_v1:id()}
}).

-type state() :: #state{}.
-type clients() :: #{blockchain_state_channel_v1:id() => [libp2p_crypto:pubkey_bin()]}.

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start_link(Args) ->
    gen_server:start_link({local, ?SERVER}, ?SERVER, Args, []).

-spec credits(blockchain_state_channel_v1:id()) -> {ok, non_neg_integer()}.
credits(ID) ->
    gen_server:call(?SERVER, {credits, ID}).

-spec nonce(blockchain_state_channel_v1:id()) -> {ok, non_neg_integer()}.
nonce(ID) ->
    gen_server:call(?SERVER, {nonce, ID}).

-spec request(blockchain_state_channel_request_v1:request()) -> ok.
request(Req) ->
    gen_server:cast(?SERVER, {request, Req}).

% TODO: Replace this with real burn
-spec burn(blockchain_state_channel_v1:id(), non_neg_integer()) -> ok.
burn(ID, Amount) ->
    gen_server:cast(?SERVER, {burn, ID, Amount}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init([Swarm]=_Args) ->
    lager:info("~p init with ~p", [?SERVER, _Args]),
    {ok, DB} = blockchain_state_channel_db:get(),
    {ok, State} = load_state(DB, Swarm),
    {ok, State}.

handle_call({credits, ID}, _From, #state{state_channels=SCs}=State) ->
    Reply = case maps:get(ID, SCs, undefined) of
        undefined -> {error, not_found};
        SC -> {ok, blockchain_state_channel_v1:credits(SC)}
    end,
    {reply, Reply, State};
handle_call({nonce, ID}, _From, #state{state_channels=SCs}=State) ->
    Reply = case maps:get(ID, SCs, undefined) of
        undefined -> {error, not_found};
        SC -> {ok, blockchain_state_channel_v1:nonce(SC)}
    end,
    {reply, Reply, State};
handle_call(_Msg, _From, State) ->
    lager:warning("rcvd unknown call msg: ~p from: ~p", [_Msg, _From]),
    {reply, ok, State}.

% TODO: Replace this with real burn
handle_cast({burn, ID, Amount}, #state{swarm=Swarm, state_channels=SCs}=State) ->
    case maps:is_key(ID, SCs) of
        true ->
            {noreply, State};
        false ->
            {Owner, _} = blockchain_utils:get_pubkeybin_sigfun(Swarm),
            SC0 = blockchain_state_channel_v1:new(ID, Owner),
            SC1 = blockchain_state_channel_v1:credits(Amount, SC0),
            {noreply, State#state{state_channels=maps:put(ID, SC1, SCs)}}
    end;
handle_cast({request, Req}, #state{db=DB, swarm=Swarm}=State0) ->
    case blockchain_state_channel_request_v1:validate(Req) of
        {error, _Reason} ->
            lager:warning("got invalid req ~p: ~p", [Req, _Reason]),
            {noreply, State0};
        true ->
            case select_state_channel(Req, State0) of
                {error, _Reason} ->
                    % TODO: Maybe counter offer here?
                    lager:warning("no valid state channel found for ~p:~p", [Req, _Reason]),
                    {noreply, State0};
                {ok, SC0} ->
                    case blockchain_state_channel_v1:validate_request(Req, SC0) of
                        {error, _Reason} ->
                            % TODO: Maybe counter offer here?
                            lager:warning("failed to validate req ~p:~p", [Req, _Reason]),
                            {noreply, State0};
                        ok ->
                            % TODO: Update packet stuff
                            {_, PayerSigFun} = blockchain_utils:get_pubkeybin_sigfun(Swarm),
                            SC1 = blockchain_state_channel_v1:add_request(Req, PayerSigFun, SC0),
                            ok = blockchain_state_channel_v1:save(DB, SC1),
                            State1 = update_state(SC1, Req, State0),
                            ok = update_clients(SC1, State1),
                            lager:info("added request ~p to state channel ~p", [Req, blockchain_state_channel_v1:id(SC1)]),
                            {noreply, State1}
                    end
            end
    end;
handle_cast(_Msg, State) ->
    lager:warning("rcvd unknown cast msg: ~p", [_Msg]),
    {noreply, State}.

handle_info(_Msg, State) ->
    lager:warning("rcvd unknown info msg: ~p", [_Msg]),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _state) ->
    ok.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

-spec update_clients(blockchain_state_channel_v1:state_channel(), state()) -> ok.
update_clients(SC, #state{swarm=Swarm, clients=Clients}) ->
    ID = blockchain_state_channel_v1:id(SC),
    PubKeyBins = maps:get(ID, Clients, []),
    lists:foreach(
        fun(PubKeyBin) ->
            Address = libp2p_crypto:pubkey_bin_to_p2p(PubKeyBin),
            case blockchain_state_channel_handler:dial(Swarm, Address, []) of
                {error, _Reason} ->
                    lager:warning("failed to dial ~p:~p", [Address, _Reason]);
                {ok, Pid} ->
                    blockchain_state_channel_handler:broadcast(Pid, SC)
            end  
        end,
        PubKeyBins
    ).

-spec select_state_channel(blockchain_state_channel_request_v1:request(), state()) ->
    {ok, blockchain_state_channel_v1:state_channel()} | {error, any()}.
select_state_channel(Req, #state{state_channels=SCs, payees_to_sc=PayeesToSC}=State) ->
    case maps:size(SCs) == 0 of
        true ->
            {error, no_state_channel};
        false ->
            Amount = blockchain_state_channel_request_v1:amount(Req),
            Payee = blockchain_state_channel_request_v1:payee(Req),
            case maps:get(Payee, PayeesToSC, undefined) of
                undefined ->
                    [SC|_] = lists:sort(
                        fun(SCA, SCB) ->
                            blockchain_state_channel_v1:credits(SCA) >= blockchain_state_channel_v1:credits(SCB)
                        end,
                        maps:values(SCs)
                    ),
                    Credits = blockchain_state_channel_v1:credits(SC),
                    case Credits-Amount >= 0 of
                        false -> {error, not_enough_credits};
                        true -> {ok, SC}
                    end;
                ID ->
                    SC = maps:get(ID, SCs),
                    Credits = blockchain_state_channel_v1:credits(SC),
                    case Credits-Amount >= 0 of
                        false ->
                            select_state_channel(Req, State#state{payees_to_sc=#{}});
                        true ->
                            {ok, SC}
                    end
            end
    end.

-spec load_state(rocksdb:db_handle(), pid()) -> {ok, state()} | {error, any()}.
load_state(DB, Swarm) ->
    case get_state_channels(DB) of
        {error, _}=Error ->
            Error;
        {ok, SCIDs} ->
            SCs = lists:foldl(
                fun(ID, Acc) ->
                    case blockchain_state_channel_v1:get(DB, ID) of
                        {error, _Reason} ->
                            % TODO: Maybe cleanup not_found state channels from list
                            lager:warning("could not get state channel ~p: ~p", [ID, _Reason]),
                            Acc;
                        {ok, SC} ->
                            maps:put(ID, SC, Acc)
                    end
                end,
                maps:new(),
                SCIDs
            ),
            Clients = lists:foldl(
                fun(SC, Acc0) ->
                    ID = blockchain_state_channel_v1:id(SC),
                    Balances = blockchain_state_channel_v1:balances(SC),
                    Payees = lists:foldl(
                        fun({Payee, _}, Acc1) ->
                            case lists:member(Payee, Acc1) of
                                true -> Acc1;
                                false -> [Payee|Acc1]
                            end
                        end,
                        [],
                        Balances
                    ),
                    maps:put(ID, Payees, Acc0)
                end,
                maps:new(),
                maps:values(SCs)
            ),
            {ok, #state{db=DB, swarm=Swarm, state_channels=SCs, clients=Clients}}
    end.

-spec update_state(blockchain_state_channel_v1:state_channel(), blockchain_state_channel_request_v1:request(), state()) -> state().
update_state(SC, Req, #state{db=DB, state_channels=SCs, payees_to_sc=PayeesToSC, clients=Clients}=State) ->
    ID = blockchain_state_channel_v1:id(SC),
    Payee = blockchain_state_channel_request_v1:payee(Req),
    ok = save_state_channels(DB, ID),
    Payees0 = maps:get(ID, Clients, []),
    Payees1 = case lists:member(Payee, Payees0) of
        false -> [Payee|Payees0];
        true -> Payees0
    end,
    State#state{state_channels=maps:put(ID, SC, SCs),
                clients=maps:put(ID, Payees1, Clients),
                payees_to_sc=maps:put(Payee, ID, PayeesToSC)}.

-spec save_state_channels(rocksdb:db_handle(), blockchain_state_channel_v1:id()) -> ok | {error, any()}.
save_state_channels(DB, ID) ->
    case get_state_channels(DB) of
        {error, _}=Error ->
            Error;
        {ok, SCIDs} ->
            case lists:member(ID, SCIDs) of
                true ->
                    ok;
                false ->
                    rocksdb:put(DB, ?STATE_CHANNELS, erlang:term_to_binary([ID|SCIDs]), [{sync, true}])
            end
    end.

-spec get_state_channels(rocksdb:db_handle()) -> {ok, [blockchain_state_channel_v1:id()]} | {error, any()}.
get_state_channels(DB) ->
    case rocksdb:get(DB, ?STATE_CHANNELS, [{sync, true}]) of
        {ok, Bin} -> {ok, erlang:binary_to_term(Bin)};
        not_found -> {ok, []};
        Error -> Error
    end.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

select_state_channel_test() ->
    Req0 = blockchain_state_channel_request_v1:new(<<"payee">>, 1, 12),
    State0 = #state{state_channels= #{}, payees_to_sc= #{}},
    ?assertEqual({error, no_state_channel}, select_state_channel(Req0, State0)),

    Req1 = blockchain_state_channel_request_v1:new(<<"payee">>, 1, 12),
    ID1 = <<"1">>,
    SC1 =blockchain_state_channel_v1:new(ID1, <<"owner">>),
    State1 = #state{state_channels= #{ID1 => SC1}, payees_to_sc= #{<<"payee">> => ID1}},
    ?assertEqual({error, not_enough_credits}, select_state_channel(Req1, State1)),

    Req2 = blockchain_state_channel_request_v1:new(<<"payee">>, 1, 12),
    ID2 = <<"2">>,
    SC2 = blockchain_state_channel_v1:credits(10, blockchain_state_channel_v1:new(ID2, <<"owner">>)),
    State2 = #state{state_channels= #{ID2 => SC2}, payees_to_sc= #{<<"payee">> => ID2}},
    ?assertEqual({ok, SC2}, select_state_channel(Req2, State2)),

    Req4 = blockchain_state_channel_request_v1:new(<<"payee">>, 1, 12),
    ID3 = <<"3">>,
    SC3 = blockchain_state_channel_v1:new(ID3, <<"owner">>),
    ID4 = <<"4">>,
    SC4 = blockchain_state_channel_v1:credits(10, blockchain_state_channel_v1:new(ID4, <<"owner">>)),
    State4 = #state{state_channels= #{ID3 => SC3, ID4 => SC4}, payees_to_sc= #{<<"payee">> => ID3}},
    ?assertEqual({ok, SC4}, select_state_channel(Req4, State4)),
    ok.

load_test() ->
    BaseDir = test_utils:tmp_dir("load_test"),
    {ok, DB} = open_db(BaseDir),
    {ok, Swarm} = start_swarm(load_test, BaseDir),
    {ok, PubKey, _, _} =libp2p_swarm:keys(Swarm),
    PubKeyBin = libp2p_crypto:pubkey_to_bin(PubKey),
    ID = <<"1">>,
    SC = blockchain_state_channel_v1:new(ID, PubKeyBin),
    ok = rocksdb:put(DB, ?STATE_CHANNELS, erlang:term_to_binary([ID]), [{sync, true}]),
    State = #state{
        db=DB,
        swarm=Swarm,
        state_channels=#{ID => SC},
        clients=#{ID => []},
        payees_to_sc=#{}
    },
    ?assertEqual(ok, blockchain_state_channel_v1:save(DB, SC)),
    ?assertEqual({ok, State}, load_state(DB, Swarm)),
    ok = rocksdb:close(DB),
    libp2p_swarm:stop(Swarm),
    ok.

update_state_test() ->
    BaseDir = test_utils:tmp_dir("update_state_test"),
    {ok, DB} = open_db(BaseDir),
    {ok, Swarm} = start_swarm(update_state_test, BaseDir),
    {ok, PubKey, _, _} =libp2p_swarm:keys(Swarm),
    PubKeyBin = libp2p_crypto:pubkey_to_bin(PubKey),
    ID = <<"1">>,
    SC = blockchain_state_channel_v1:new(ID, PubKeyBin),
    Payee = <<"payee">>,
    Req = blockchain_state_channel_request_v1:new(Payee, 1, 12),
    State0 = #state{db=DB, swarm=Swarm, state_channels=#{}, payees_to_sc=#{}},
    State1 = State0#state{state_channels=#{ID => SC}, payees_to_sc=#{Payee => ID}, clients=#{ID => [Payee]}},

    ?assertEqual(State1, update_state(SC, Req, State0)),
    ?assertEqual({ok, [ID]}, get_state_channels(DB)),

    ok = rocksdb:close(DB),
    libp2p_swarm:stop(Swarm),
    ok.

open_db(Dir) ->
    DBDir = filename:join(Dir, "state_channels.db"),
    ok = filelib:ensure_dir(DBDir),
    GlobalOpts = application:get_env(rocksdb, global_opts, []),
    DBOptions = [{create_if_missing, true}] ++ GlobalOpts,
    {ok, _DB} = rocksdb:open(DBDir, DBOptions).

start_swarm(Name, BaseDir) ->
    NewOpts = lists:keystore(base_dir, 1, [], {base_dir, BaseDir})
        ++ [{libp2p_nat, [{enabled, false}]}],
    libp2p_swarm:start(Name, NewOpts).

-endif.