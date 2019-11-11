%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel Server ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channel_server).

-behavior(gen_server).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------
-export([
    start_link/1,
    credits/0, nonce/0,
    burn/1, payment_req/1
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

-record(state, {
    db :: rocksdb:db_handle() | undefined,
    keys :: {libp2p_crypto:pubkey_bin(), libp2p_crypto:sig_fun()} | undefined,
    state_channel :: blockchain_state_channel:state_channel()
}).

-type state() :: #state{}.

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start_link(Args) ->
    gen_server:start_link({local, ?SERVER}, ?SERVER, Args, []).

-spec credits() -> {ok, non_neg_integer()}.
credits() ->
    gen_server:call(?SERVER, credits).

-spec nonce() -> {ok, non_neg_integer()}.
nonce() ->
    gen_server:call(?SERVER, nonce).

-spec payment_req(blockchain_dcs_payment_req:dcs_payment_req()) -> ok.
payment_req(Req) ->
    gen_server:cast(?SERVER, {payment_req, Req}).

% TODO: Replace this with real burn
-spec burn(non_neg_integer()) -> ok.
burn(Amount) ->
    gen_server:cast(?SERVER, {burn, Amount}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init(_Args) ->
    {ok, DB} = blockchain_state_channel_db:get(),
    {ok, PubKey, SigFun, _} = blockchain_swarm:keys(),
    PubKeyBin = libp2p_crypto:pubkey_to_bin(PubKey),
    Keys = {PubKeyBin, SigFun},
    {ok, State} = load_state(DB, Keys),
    {ok, State}.

handle_call(credits, _From, #state{state_channel=SC}=State) ->
    {reply, {ok, blockchain_state_channel:credits(SC)}, State};
handle_call(nonce, _From, #state{state_channel=SC}=State) ->
    {reply, {ok, blockchain_state_channel:nonce(SC)}, State};
handle_call(_Msg, _From, State) ->
    lager:warning("rcvd unknown call msg: ~p from: ~p", [_Msg, _From]),
    {reply, ok, State}.

handle_cast({burn, Amount}, #state{state_channel=SC0}=State0) ->
    Credits0 = blockchain_state_channel:credits(SC0),
    SC1 = blockchain_state_channel:credits(Credits0+Amount, SC0),
    State1 = State0#state{state_channel=SC1},
    {noreply, save_state(State1)};
handle_cast({payment_req, Req}, #state{keys={Payer, PayerSigFun}, state_channel=SC0}=State0) ->
    case blockchain_dcs_payment_req:validate(Req) of
        {error, _Reason} ->
            lager:warning("got invalid req ~p: ~p", [Req, _Reason]),
            {noreply, State0};
        true ->
            Amount = blockchain_dcs_payment_req:amount(Req),
            Credits = blockchain_state_channel:credits(SC0),
            case Credits - Amount >= 0 of
                false ->
                    lager:warning("not enough data credits to handle req ~p/~p", [Amount, Credits]),
                    {noreply, State0};
                true ->
                    % TODO: Update packet stuff
                    Nonce = blockchain_state_channel:nonce(SC0),
                    Payee = blockchain_dcs_payment_req:payee(Req),
                    Payment = blockchain_dcs_payment:new(Payer, Payee, Amount, <<>>, Nonce+1),
                    SignedPayment = blockchain_dcs_payment:sign(Payment, PayerSigFun),
                    % TODO: Broadcast payment here
                    SC1 = blockchain_state_channel:add_payment(SignedPayment, SC0),
                    State1 = State0#state{state_channel=SC1},
                    {noreply, save_state(State1)}
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

-spec save_state(state()) -> state().
save_state(#state{db=DB, keys={PubKeyBin, _}}=State) ->
    ok = rocksdb:put(DB, PubKeyBin, encode_state(State), []),
    State.

-spec load_state(rocksdb:db_handle(), {libp2p_crypto:pubkey_bin(), libp2p_crypto:sig_fun()}) -> {ok, state()} | {error, any()}.
load_state(DB, {PubKeyBin, _}=Keys) ->
    case rocksdb:get(DB, PubKeyBin, [{sync, true}]) of
        {ok, BinaryState} ->
            State = decode_state(BinaryState),
            {ok, State#state{db=DB, keys=Keys}};
        not_found ->
            {ok, #state{
                db=DB,
                keys=Keys,
                state_channel=blockchain_state_channel:new(PubKeyBin)
            }};
        Error ->
            Error
    end.

-spec encode_state(state()) -> binary().
encode_state(State) ->
    erlang:term_to_binary(State#state{db=undefined, keys=undefined}).

-spec decode_state(binary()) -> state().
decode_state(BinaryState) ->
    erlang:binary_to_term(BinaryState).

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

save_load_test() ->
    BaseDir = test_utils:tmp_dir("save_load_test"),
    {ok, DB} = open_db(BaseDir),
    #{public := PubKey0, secret := PrivKey0} = libp2p_crypto:generate_keys(ecc_compact),
    PubKeyBin0 = libp2p_crypto:pubkey_to_bin(PubKey0),
    SigFun0 = libp2p_crypto:mk_sig_fun(PrivKey0),
    Keys0 = {PubKeyBin0, SigFun0},
    State0 = #state{
        db=DB,
        keys=Keys0,
        state_channel=blockchain_state_channel:new(PubKeyBin0)
    },
    ?assertEqual(State0, save_state(State0)),
    ?assertEqual({ok, State0}, load_state(DB, Keys0)),

    #{public := PubKey1, secret := PrivKey1} = libp2p_crypto:generate_keys(ecc_compact),
    PubKeyBin1 = libp2p_crypto:pubkey_to_bin(PubKey1),
    SigFun1 = libp2p_crypto:mk_sig_fun(PrivKey1),
    Keys1 = {PubKeyBin1, SigFun1},
    State1 = #state{
        db=DB,
        keys=Keys1,
        state_channel=blockchain_state_channel:new(PubKeyBin1)
    },
    ?assertEqual({ok, State1}, load_state(DB, Keys1)),

    ok = rocksdb:close(DB),
    ok.


open_db(Dir) ->
    DBDir = filename:join(Dir, "state_channels.db"),
    ok = filelib:ensure_dir(DBDir),
    GlobalOpts = application:get_env(rocksdb, global_opts, []),
    DBOptions = [{create_if_missing, true}] ++ GlobalOpts,
    {ok, _DB} = rocksdb:open(DBDir, DBOptions).


-endif.