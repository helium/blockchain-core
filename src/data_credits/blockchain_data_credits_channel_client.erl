%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Data Credits Channel Client ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_data_credits_channel_client).

-behavior(gen_server).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------
-export([
    start/1
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

-record(state, {
    db :: rocksdb:db_handle(),
    cf :: rocksdb:cf_handle(),
    payer :: libp2p_crypto:pubkey_bin(),
    stream :: undefined | pid()
}).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start(Args) ->
    gen_server:start(?SERVER, Args, []).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init([DB, CF, Payer, Amount]=Args) ->
    lager:info("~p init with ~p", [?SERVER, Args]),
    self() ! {send_payment_req, Amount},
    {ok, #state{
        db=DB,
        cf=CF,
        payer=Payer
    }}.

handle_call(_Msg, _From, State) ->
    lager:warning("rcvd unknown call msg: ~p from: ~p", [_Msg, _From]),
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    lager:warning("rcvd unknown cast msg: ~p", [_Msg]),
    {noreply, State}.

handle_info({send_payment_req, Amount}, #state{payer=Payer}=State) ->
    Swarm = blockchain_swarm:swarm(),
    P2PAddr = libp2p_crypto:pubkey_bin_to_p2p(Payer),
    case libp2p_swarm:dial_framed_stream(Swarm,
                                         P2PAddr,
                                         ?DATA_CREDITS_PROTOCOL,
                                         blockchain_data_credits_handler,
                                         [])
    of
        {ok, Stream} ->
            lager:info("sending payment request (~p) to ~p", [Amount, Payer]),
            Stream ! {payment_req, Amount},
            {noreply, State};
        Error ->
            lager:error("failed to dial ~p ~p", [P2PAddr, Error]),
            {stop, dial_error, State}
    end;
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