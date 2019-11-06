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

    credits = 0 :: non_neg_integer(),
    nonce = 0 :: non_neg_integer(),
    packets :: merkerl:merkle()
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

handle_call(credits, _From, #state{credits=Credits}=State) ->
    {reply, {ok, Credits}, State};
handle_call(nonce, _From, #state{nonce=Nonce}=State) ->
    {reply, {ok, Nonce}, State};
handle_call(_Msg, _From, State) ->
    lager:warning("rcvd unknown call msg: ~p from: ~p", [_Msg, _From]),
    {reply, ok, State}.

handle_cast({burn, Amount}, #state{credits=Credits}=State0) ->
    State1 = State0#state{credits=Credits+Amount},
    {noreply, save_state(State1)};
handle_cast({payment_req, Req}, #state{credits=Credits, nonce=Nonce}=State0) ->
    case blockchain_dcs_payment_req:validate(Req) of
        {error, _Reason} ->
            lager:warning("got invalid req ~p: ~p", [Req, _Reason]),
            {noreply, State0};
        true ->
            Amount = blockchain_dcs_payment_req:amount(Req),
            case Credits - Amount >= 0 of
                false ->
                    lager:warning("not enough data credits to handle req ~p/~p", [Amount, Credits]),
                    {noreply, State0};
                true ->
                    % TODO: Create / broadcast payment here
                    State1 = State0#state{credits=Credits-Amount, nonce=Nonce+1},
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

terminate(_Reason,  #state{db=DB}) ->
    ok = rocksdb:close(DB).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

-spec save_state(state()) -> ok.
save_state(#state{db=DB, keys={PubKeyBin, _}}=State) ->
    ok = rocksdb:put(DB, PubKeyBin, encode_state(State), []),
    State.

-spec load_state(rocksdb:db_handle(), {libp2p_crypto:pubkey_bin(), libp2p_crypto:sig_fun()}) -> state().
load_state(DB, {PubKeyBin, _}=Keys) ->
    case rocksdb:get(DB, PubKeyBin, [{sync, true}]) of
        {ok, BinaryState} ->
            State = decode_state(BinaryState),
            {ok, State#state{db=DB, keys=Keys}};
        not_found ->
            {ok, #state{
                db=DB,
                keys=Keys,
                credits=0,
                nonce=0,
                packets=merkerl:new([], fun merkerl:hash_value/1)
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
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    PubKeyBin = libp2p_crypto:pubkey_to_bin(PubKey),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Keys = {PubKeyBin, SigFun},
    State = #state{
        db=DB,
        keys={PubKeyBin, SigFun},
        credits=0,
        nonce=0,
        packets=merkerl:new([], fun merkerl:hash_value/1)
    },
    ?assertEqual(State, save_state(State)),
    ?assertEqual({ok, State}, load_state(DB, Keys)),
    ok = rocksdb:close(DB),
    ok.


open_db(Dir) ->
    DBDir = filename:join(Dir, "state_channels.db"),
    ok = filelib:ensure_dir(DBDir),
    GlobalOpts = application:get_env(rocksdb, global_opts, []),
    DBOptions = [{create_if_missing, true}] ++ GlobalOpts,
    {ok, _DB} = rocksdb:open(DBDir, DBOptions).


-endif.