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
    swarm :: pid(),
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
init([Swarm]=_Args) ->
    lager:info("~p init with ~p", [?SERVER, _Args]),
    {ok, DB} = blockchain_state_channel_db:get(),
    {ok, State} = load_state(DB, Swarm),
    {ok, State}.

handle_call(credits, _From, #state{state_channel=SC}=State) ->
    {reply, {ok, blockchain_state_channel:credits(SC)}, State};
handle_call(nonce, _From, #state{state_channel=SC}=State) ->
    {reply, {ok, blockchain_state_channel:nonce(SC)}, State};
handle_call(_Msg, _From, State) ->
    lager:warning("rcvd unknown call msg: ~p from: ~p", [_Msg, _From]),
    {reply, ok, State}.

handle_cast({burn, Amount}, #state{state_channel=SC0}=State) ->
    Credits0 = blockchain_state_channel:credits(SC0),
    SC1 = blockchain_state_channel:credits(Credits0+Amount, SC0),
    {noreply, State#state{state_channel=SC1}};
handle_cast({payment_req, Req}, #state{db=DB, swarm=Swarm, state_channel=SC0}=State) ->
    case blockchain_dcs_payment_req:validate(Req) of
        {error, _Reason} ->
            lager:warning("got invalid req ~p: ~p", [Req, _Reason]),
            {noreply, State};
        true ->
            Amount = blockchain_dcs_payment_req:amount(Req),
            Credits = blockchain_state_channel:credits(SC0),
            case Credits - Amount >= 0 of
                false ->
                    lager:warning("not enough data credits to handle req ~p/~p", [Amount, Credits]),
                    {noreply, State};
                true ->
                    % TODO: Maybe counter offer here?
                    {Payer, PayerSigFun} = get_pubkeybin_sigfun(Swarm),
                    % TODO: Update packet stuff
                    Nonce = blockchain_state_channel:nonce(SC0),
                    Payee = blockchain_dcs_payment_req:payee(Req),
                    Payment = blockchain_dcs_payment:new(Payer, Payee, Amount, <<>>, Nonce+1),
                    SignedPayment = blockchain_dcs_payment:sign(Payment, PayerSigFun),
                    % TODO: Broadcast payment here
                    SC1 = blockchain_state_channel:add_payment(SignedPayment, SC0),
                    ok = blockchain_state_channel:save(DB, SC1),
                    {noreply, State#state{state_channel=SC1}}
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

-spec get_pubkeybin_sigfun(pid()) -> {libp2p_crypto:pubkey_bin(), fun()}.
get_pubkeybin_sigfun(Swarm) ->
    {ok, PubKey, PayerSigFun, _} =libp2p_swarm:keys(Swarm),
    PubKeyBin = libp2p_crypto:pubkey_to_bin(PubKey),
    {PubKeyBin, PayerSigFun}.

-spec load_state(rocksdb:db_handle(), pid()) -> {ok, state()} | {error, any()}.
load_state(DB, Swarm) ->
    {ok, PubKey, _, _} =libp2p_swarm:keys(Swarm),
    PubKeyBin = libp2p_crypto:pubkey_to_bin(PubKey),
    case blockchain_state_channel:get(DB, PubKeyBin) of
        {ok, SC} ->
            {ok, #state{db=DB, swarm=Swarm, state_channel=SC}};
        {error, not_found} ->
            {ok, #state{
                db=DB,
                swarm=Swarm,
                state_channel=blockchain_state_channel:new(PubKeyBin)
            }};
        Error ->
            Error
    end.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

load_test() ->
    BaseDir = test_utils:tmp_dir("load_test"),
    {ok, DB} = open_db(BaseDir),
    {ok, Swarm} = start_swarm(load_test, BaseDir),
    {ok, PubKey, _, _} =libp2p_swarm:keys(Swarm),
    PubKeyBin = libp2p_crypto:pubkey_to_bin(PubKey),
    SC = blockchain_state_channel:new(PubKeyBin),
    State = #state{
        db=DB,
        swarm=Swarm,
        state_channel=SC
    },
    ?assertEqual(ok, blockchain_state_channel:save(DB, SC)),
    ?assertEqual({ok, State}, load_state(DB, Swarm)),
    ok = rocksdb:close(DB),
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