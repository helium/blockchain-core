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
    payment_req/2,
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
    state_channels = #{} :: #{blockchain_state_channel:id() => blockchain_state_channel:state_channel()},
    payees = #{} :: #{libp2p_crypto:pubkey_bin() => blockchain_state_channel:id()},
    clients = #{} :: #{blockchain_state_channel:id() => [pid()]}
}).

-type state() :: #state{}.

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start_link(Args) ->
    gen_server:start_link({local, ?SERVER}, ?SERVER, Args, []).

-spec credits(blockchain_state_channel:id()) -> {ok, non_neg_integer()}.
credits(ID) ->
    gen_server:call(?SERVER, {credits, ID}).

-spec nonce(blockchain_state_channel:id()) -> {ok, non_neg_integer()}.
nonce(ID) ->
    gen_server:call(?SERVER, {nonce, ID}).

-spec payment_req(blockchain_dcs_payment_req:dcs_payment_req(), pid()) -> ok.
payment_req(Req, Stream) ->
    gen_server:cast(?SERVER, {payment_req, Req, Stream}).

% TODO: Replace this with real burn
-spec burn(blockchain_state_channel:id(), non_neg_integer()) -> ok.
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
        SC -> {ok, blockchain_state_channel:credits(SC)}
    end,
    {reply, Reply, State};
handle_call({nonce, ID}, _From, #state{state_channels=SCs}=State) ->
    Reply = case maps:get(ID, SCs, undefined) of
        undefined -> {error, not_found};
        SC -> {ok, blockchain_state_channel:nonce(SC)}
    end,
    {reply, Reply, State};
handle_call(_Msg, _From, State) ->
    lager:warning("rcvd unknown call msg: ~p from: ~p", [_Msg, _From]),
    {reply, ok, State}.

% TODO: Replace this with real burn
handle_cast({burn, ID, Amount}, #state{swarm=Swarm, state_channels=SCs}=State) ->
    {Owner, _} = blockchain_utils:get_pubkeybin_sigfun(Swarm),
    SC0 = blockchain_state_channel:new(ID, Owner),
    SC1 = blockchain_state_channel:credits(Amount, SC0),
    {noreply, State#state{state_channels=maps:put(ID, SC1, SCs)}};
handle_cast({payment_req, Req, Stream}, #state{db=DB, swarm=Swarm}=State) ->
    case blockchain_state_channel_payment_req:validate(Req) of
        {error, _Reason} ->
            lager:warning("got invalid req ~p: ~p", [Req, _Reason]),
            {noreply, State};
        true ->
            case select_state_channel(Req, State) of
                {error, _Reason} ->
                    % TODO: Maybe counter offer here?
                    lager:warning("no valid state channel found for ~p:~p", [Req, _Reason]),
                    {noreply, State};
                {ok, SC0} ->
                    Amount = blockchain_state_channel_payment_req:amount(Req),
                    {Payer, PayerSigFun} = blockchain_utils:get_pubkeybin_sigfun(Swarm),
                    Payee = blockchain_state_channel_payment_req:payee(Req),
                    Payment = blockchain_state_channel_payment:new(Payer, Payee, Amount, <<>>),
                    case blockchain_state_channel:validate_payment(Payment, SC0) of
                        {error, _Reason} ->
                            % TODO: Maybe counter offer here?
                            lager:warning("failed to validate payment ~p:~p", [Payment, _Reason]),
                            {noreply, State};
                        ok ->
                            % TODO: Update packet stuffle
                            % TODO: Broadcast state channel here
                            SC1 = blockchain_state_channel:add_payment(Payment, PayerSigFun, SC0),
                            ok = blockchain_state_channel:save(DB, SC1),
                            {noreply, update_state(SC1, Payment, Stream, State)}
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

-spec broadcast_state_channel(blockchain_state_channel:state_channel(), state()) -> ok.
broadcast_state_channel(SC, #state{clients=Clients}) ->
    ID = blockchain_state_channel:id(SC),
    Streams = maps:get(ID, Clients, []),
    lists:foreach(
        fun(Stream) ->
            Stream ! {broadcast, SC}
        end,
        Streams
    ).

-spec select_state_channel(blockchain_state_channel_payment_req:payment_req(), state()) ->
    {ok, blockchain_state_channel:state_channel()} | {error, any()}.
select_state_channel(Req, #state{state_channels=SCs, payees=Payees}=State) ->
    case maps:size(SCs) == 0 of
        true ->
            {error, no_state_channel};
        false ->
            Amount = blockchain_state_channel_payment_req:amount(Req),
            Payee = blockchain_state_channel_payment_req:payee(Req),
            case maps:get(Payee, Payees, undefined) of
                undefined ->
                    [SC|_] = lists:sort(
                        fun(SCA, SCB) ->
                            blockchain_state_channel:credits(SCA) >= blockchain_state_channel:credits(SCB)
                        end,
                        maps:values(SCs)
                    ),
                    Credits = blockchain_state_channel:credits(SC),
                    case Credits-Amount >= 0 of
                        false -> {error, not_enough_credits};
                        true -> {ok, SC}
                    end;
                ID ->
                    SC = maps:get(ID, SCs),
                    Credits = blockchain_state_channel:credits(SC),
                    case Credits-Amount >= 0 of
                        false ->
                            select_state_channel(Req, State#state{payees=#{}});
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
                    case blockchain_state_channel:get(DB, ID) of
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
            {ok, #state{db=DB, swarm=Swarm, state_channels=SCs}}
    end.

-spec update_state(blockchain_state_channel:state_channel(), blockchain_state_channel_payment:payment(), pid(), state()) -> state().
update_state(SC, Payment, Stream, #state{db=DB, state_channels=SCs, payees=Payees, clients=Clients}=State) ->
    ID = blockchain_state_channel:id(SC),
    Payee = blockchain_state_channel_payment:payee(Payment),
    ok = save_state_channels(DB, ID),
    Streams0 = maps:get(ID, Clients, []),
    Streams1 = case lists:member(Stream, Streams0) of
        false -> [Stream|Streams0];
        true -> Streams0
    end,
    State#state{state_channels=maps:put(ID, SC, SCs), payees=maps:put(Payee, ID, Payees), clients=maps:put(ID, Streams1, Clients)}.

-spec save_state_channels(rocksdb:db_handle(), blockchain_state_channel:id()) -> ok | {error, any()}.
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

-spec get_state_channels(rocksdb:db_handle()) -> {ok, [blockchain_state_channel:id()]} | {error, any()}.
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
    Req0 = blockchain_state_channel_payment_req:new(<<"payee">>, 1, 12),
    State0 = #state{state_channels= #{}, payees= #{}},
    ?assertEqual({error, no_state_channel}, select_state_channel(Req0, State0)),

    Req1 = blockchain_state_channel_payment_req:new(<<"payee">>, 1, 12),
    ID1 = <<"1">>,
    SC1 =blockchain_state_channel:new(ID1, <<"owner">>),
    State1 = #state{state_channels= #{ID1 => SC1}, payees= #{<<"payee">> => ID1}},
    ?assertEqual({error, not_enough_credits}, select_state_channel(Req1, State1)),

    Req2 = blockchain_state_channel_payment_req:new(<<"payee">>, 1, 12),
    ID2 = <<"2">>,
    SC2 = blockchain_state_channel:credits(10, blockchain_state_channel:new(ID2, <<"owner">>)),
    State2 = #state{state_channels= #{ID2 => SC2}, payees= #{<<"payee">> => ID2}},
    ?assertEqual({ok, SC2}, select_state_channel(Req2, State2)),

    Req4 = blockchain_state_channel_payment_req:new(<<"payee">>, 1, 12),
    ID3 = <<"3">>,
    SC3 = blockchain_state_channel:new(ID3, <<"owner">>),
    ID4 = <<"4">>,
    SC4 = blockchain_state_channel:credits(10, blockchain_state_channel:new(ID4, <<"owner">>)),
    State4 = #state{state_channels= #{ID3 => SC3, ID4 => SC4}, payees= #{<<"payee">> => ID3}},
    ?assertEqual({ok, SC4}, select_state_channel(Req4, State4)),
    ok.

load_test() ->
    BaseDir = test_utils:tmp_dir("load_test"),
    {ok, DB} = open_db(BaseDir),
    {ok, Swarm} = start_swarm(load_test, BaseDir),
    {ok, PubKey, _, _} =libp2p_swarm:keys(Swarm),
    PubKeyBin = libp2p_crypto:pubkey_to_bin(PubKey),
    ID = <<"1">>,
    SC = blockchain_state_channel:new(ID, PubKeyBin),
    ok = rocksdb:put(DB, ?STATE_CHANNELS, erlang:term_to_binary([ID]), [{sync, true}]),
    State = #state{
        db=DB,
        swarm=Swarm,
        state_channels=#{ID => SC},
        payees=#{}
    },
    ?assertEqual(ok, blockchain_state_channel:save(DB, SC)),
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
    SC = blockchain_state_channel:new(ID, PubKeyBin),
    Payee = <<"payee">>,
    Payment = blockchain_state_channel_payment:new(PubKeyBin, Payee, 1, <<>>),
    State0 = #state{db=DB, swarm=Swarm, state_channels=#{}, payees=#{}},
    State1 = State0#state{state_channels=#{ID => SC}, payees=#{Payee => ID}, clients=#{ID => [self()]}},

    ?assertEqual(State1, update_state(SC, Payment, self(), State0)),
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