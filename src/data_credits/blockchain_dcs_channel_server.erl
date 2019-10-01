%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Data Credits Channel Server ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_dcs_channel_server).

-behavior(gen_server).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------
-export([
    start_link/1,
    credits/1
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

-define(SERVER, ?MODULE).
-define(CLIENTS, <<"channel_clients">>).

-record(state, {
    db :: rocksdb:db_handle(),
    cf :: rocksdb:cf_handle(),
    keys :: libp2p_crypto:key_map(),
    credits = 0 :: non_neg_integer(),
    nonce = 0 :: non_neg_integer(),
    merkle_tree :: merkerl:merkle()
}).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start_link(Args) ->
    gen_server:start_link(?SERVER, Args, []).

-spec credits(pid()) -> {ok, non_neg_integer()}.
credits(Pid) ->
    gen_statem:call(Pid, credits).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init([DB, CF, Keys, 0]=Args) ->
    lager:info("~p init with ~p", [?SERVER, Args]),
    {ok, Nonce} = blockchain_dcs_utils:get_nonce(DB, CF),
    {ok, Credits} = blockchain_dcs_utils:get_credits(DB, CF),
    {ok, #state{
        db=DB,
        cf=CF,
        keys=Keys,
        credits=Credits,
        nonce=Nonce,
        % TODO: Rebuild tree
        merkle_tree=merkerl:new([], fun merkerl:hash_value/1)
    }};
init([DB, CF, Keys, Credits]=Args) ->
    lager:info("~p init with ~p", [?SERVER, Args]),
    Payment = blockchain_dcs_payment:new(
        <<>>,
        Keys,
        0,
        blockchain_swarm:pubkey_bin(),
        blockchain_swarm:pubkey_bin(),
        Credits
    ),
    ok = blockchain_dcs_payment:store(DB, CF, Payment),
    {ok, #state{
        db=DB,
        cf=CF,
        keys=Keys,
        credits=Credits,
        nonce=0,
        merkle_tree=merkerl:new([], fun merkerl:hash_value/1)
    }}.

handle_call(credits, _From, #state{credits=Credits}=State) ->
    {reply, {ok, Credits}, State};
handle_call(_Msg, _From, State) ->
    lager:warning("rcvd unknown call msg: ~p from: ~p", [_Msg, _From]),
    {reply, ok, State}.

handle_cast({payment_req, PaymentReq}, #state{db=DB, cf=CF, keys=Keys,
                                              credits=Credits, nonce=Nonce0,
                                              merkle_tree=Tree0}=State) ->
    % TODO: Check that we can pay for it
    Nonce1 = Nonce0+1,
    Payee = blockchain_dcs_payment_req:payee(PaymentReq),
    Amount = blockchain_dcs_payment_req:amount(PaymentReq),
    Payment = blockchain_dcs_payment:new(Payee, Amount, Nonce1),
    #{secret := PrivKey}=Keys,
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    SignedPayment = blockchain_dcs_payment:sign(Payment, SigFun),
    ok = blockchain_dcs_payment:store(DB, CF, SignedPayment),
    lager:info("got payment request ~p (leftover: ~p)", [PaymentReq, Credits-Amount]),
    TreeValues = merkerl:values(Tree0),
    Tree1 = merkerl:new(TreeValues ++ [SignedPayment], fun merkerl:hash_value/1),
    Clients0 = channel_clients(DB, CF),
    case maps:is_key(Payee, Clients0) of
        true ->
            ok = broacast(maps:keys(Clients0), Tree1);
        false ->
            Clients1 = maps:put(Payee, <<>>, Clients0),
            ok = add_client(DB, CF, Payee, Clients1),
            ok = broacast(maps:keys(Clients1), Tree1)
    end,
    case Credits-Amount == 0 of
        false ->
            {noreply, State#state{credits=Credits-Amount, nonce=Nonce1}};
        true ->
            % TODO: Should settle right here
            {stop, normal, State#state{credits=Credits-Amount, nonce=Nonce1}}
    end;
handle_cast(_Msg, State) ->
    lager:warning("rcvd unknown cast msg: ~p", [_Msg]),
    {noreply, State}.

handle_info(_Msg, State) ->
    lager:warning("rcvd unknown info msg: ~p", [_Msg]),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

-spec channel_clients(rocksdb:db_handle(), rocksdb:cf_handle()) -> {ok, map()} | {error, any()}.
channel_clients(DB, CF) ->
    case rocksdb:get(DB, CF, ?CLIENTS, [{sync, true}]) of
        {ok, Bin} ->
            {ok, erlang:binary_to_term(Bin)};
        not_found ->
            {ok, #{}};
        _Error ->
            lager:error("failed to get ~p: ~p", [?CLIENTS, _Error]),
            _Error
    end.

-spec add_client(rocksdb:db_handle(), rocksdb:cf_handle(), libp2p_crypto:pubkey_bin(), map()) -> ok | {error, any()}.
add_client(DB, CF, PubKeyBin, Client0) ->
    Client1 = maps:put(PubKeyBin, <<>>, Client0),
    rocksdb:put(DB, CF, ?CLIENTS, erlang:term_to_binary(Client1), []).

-spec broacast([libp2p_crypto:pubkey_bin()], merkerl:merkle()) -> ok.
broacast([], _Tree) ->
    ok;
broacast([PubKeyBin|Clients], Tree) ->
    Swarm = blockchain_swarm:swarm(),
    P2PAddr = libp2p_crypto:pubkey_bin_to_p2p(PubKeyBin),
    % TODO: Maybe save stream so we don't have to re dial everytime?
    case libp2p_swarm:dial_framed_stream(Swarm,
                                         P2PAddr,
                                         ?DATA_CREDITS_CHANNEL_PROTOCOL,
                                         blockchain_dcs_channel_stream,
                                         [])
    of
        {ok, Stream} ->
            lager:info("broadcasting payment update to ~p", [PubKeyBin]),
            EncodedTree = erlang:term_to_binary(Tree),
            blockchain_dcs_channel_stream:send_update(Stream, EncodedTree),
            broacast(Clients, Tree);
        _Error ->
            broacast(Clients, Tree)
    end.
