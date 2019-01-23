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
         submit/5,
         status/1
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
          enqueued = [] :: [{ok | error, blockchain_transactions:transactions()}]
         }).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start_link(Args) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

-spec submit(blockchain_transactions:transaction(), [libp2p_crypto:address()], atom(), non_neg_integer(), fun()) -> ok.
submit(Transaction, Receivers, Handler, Retries, CallbackFun) ->
    gen_server:cast(?MODULE, {submit, Transaction, Receivers, Handler, Retries, CallbackFun}).

-spec status(blockchain_transactions:transaction()) -> {ok, blockchain_transactions:transaction()} | false.
status(Transaction) ->
    gen_server:call(?MODULE, {status, Transaction}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init(_Args) ->
    {ok, #state{}}.

handle_cast({submit, Transaction, Receivers, Handler, Retries, _CallbackFun}, State=#state{enqueued=Enqueued}) ->
    DataToSend = erlang:term_to_binary({blockchain_transactions:type(Transaction), Transaction}),
    RandomConsensusAddress = lists:nth(rand:uniform(length(Receivers)), Receivers),
    P2PAddress = libp2p_crypto:address_to_p2p(RandomConsensusAddress),
    Swarm = blockchain_swarm:swarm(),
    Res = case Retries > 0 of
              true ->
                  case libp2p_swarm:dial_framed_stream(Swarm, P2PAddress, ?TX_PROTOCOL, Handler, [self()]) of
                      {ok, Stream} ->
                          lager:info("dialed peer ~p via ~p~n", [RandomConsensusAddress, ?TX_PROTOCOL]),
                          libp2p_framed_stream:send(Stream, DataToSend),
                          libp2p_framed_stream:close(Stream),
                          {ok, {Transaction, 0}};
                      Other ->
                          lager:notice("Failed to dial ~p service on ~p : ~p", [?TX_PROTOCOL, RandomConsensusAddress, Other]),
                          self() ! {retry, Transaction, Receivers, Handler, Retries - 1, _CallbackFun}
                  end;
              false ->
                  case libp2p_swarm:dial_framed_stream(Swarm, P2PAddress, ?TX_PROTOCOL, Handler, [self()]) of
                      {ok, Stream} ->
                          lager:info("dialed peer ~p via ~p~n", [RandomConsensusAddress, ?TX_PROTOCOL]),
                          libp2p_framed_stream:send(Stream, DataToSend),
                          libp2p_framed_stream:close(Stream),
                          {ok, {Transaction, 0}};
                      Other ->
                          lager:notice("Failed to dial ~p service on ~p : ~p", [?TX_PROTOCOL, RandomConsensusAddress, Other]),
                          {error, {Transaction, zero_retries_and_failed}}
                  end
          end,

    {noreply, State#state{enqueued=[Res | Enqueued]}};
handle_cast(_, State) ->
    {noreply, State}.

handle_call({status, Transaction}, _From, State=#state{enqueued=Enqueued}) ->
    Res = lists:keyfind(Transaction, 2, Enqueued),
    {reply, Res, State}.

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
