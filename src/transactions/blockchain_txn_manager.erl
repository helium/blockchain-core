%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Manager ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_manager).

-behavior(gen_server).

-include("blockchain.hrl").

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------
-export([
         start_link/1,
         submit/3
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

-record(state, {}).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start_link(Args) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

-spec submit(Txn :: blockchain_transactions:transaction(),
             ConsensusAddrs :: [libp2p_crypto:address()],
             Callback :: fun()) -> ok.
submit(Txn, ConsensusAddrs, Callback) ->
    gen_server:cast(?MODULE, {submit, Txn, ConsensusAddrs, Callback}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init(_Args) ->
    {ok, #state{}}.

handle_cast({submit, Transaction, ConsensusAddrs, Callback}, State) ->
    self() ! {send, [], Transaction, ConsensusAddrs, Callback},
    {noreply, State};
handle_cast(_Msg, State) ->
    lager:warning("blockchain_txn_manager got unknown cast: ~p", [_Msg]),
    {noreply, State}.

handle_call(_, _, State) ->
    {reply, ok, State}.

handle_info({send, SentBefore, Txn, ConsensusAddrs, Callback}, State) ->
    F = (length(ConsensusAddrs) - 1) div 3,
    Res = [{dial(Addr, Txn), Addr} || Addr <- random_n(F+1, ConsensusAddrs), not lists:member(Addr, SentBefore)],
    SuccessSent = [Addr || {ok, Addr} <- Res],
    case length(SuccessSent) + length(SentBefore) > F of
        true ->
            Callback(ok);
        false ->
            self() ! {send, SentBefore ++ SuccessSent, Txn}
    end,
    {noreply, State};
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

dial(Addr, Txn) ->
    DataToSend = erlang:term_to_binary({blockchain_transactions:type(Txn), Txn}),
    P2PAddress = libp2p_crypto:address_to_p2p(Addr),
    Swarm = blockchain_swarm:swarm(),
    case libp2p_swarm:dial_framed_stream(Swarm, P2PAddress, ?TX_PROTOCOL, blockchain_txn_handler, [self()]) of
        {ok, Stream} ->
            lager:info("dialed peer ~p via ~p~n", [Addr, ?TX_PROTOCOL]),
            libp2p_framed_stream:send(Stream, DataToSend),
            libp2p_framed_stream:close(Stream),
            ok;
        Other ->
            lager:notice("Failed to dial ~p service on ~p : ~p", [?TX_PROTOCOL, Addr, Other]),
            Other
    end.

random_n(N, List) ->
    lists:sublist(shuffle(List), N).

shuffle(List) ->
    [x || {_,x} <- lists:sort([{rand:uniform(), N} || N <- List])].
