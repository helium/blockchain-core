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
         submit/5
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

-record(state, { }).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start_link(Args) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

-spec submit(Transaction :: blockchain_transactions:transaction(),
             Receivers :: [libp2p_crypto:address()],
             Handler :: atom(),
             Retries :: non_neg_integer(),
             CallbackFun :: fun()) -> ok.
submit(Transaction, Receivers, Handler, Retries, CallbackFun) ->
    gen_server:cast(?MODULE, {submit, Transaction, Receivers, Handler, Retries, CallbackFun}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init(_Args) ->
    {ok, #state{}}.

handle_cast({submit, Transaction, Receivers, Handler, Retries, CallbackFun}, State) when Retries > 0 ->
    DataToSend = erlang:term_to_binary({blockchain_transactions:type(Transaction), Transaction}),
    RandomConsensusAddress = lists:nth(rand:uniform(length(Receivers)), Receivers),
    P2PAddress = libp2p_crypto:address_to_p2p(RandomConsensusAddress),
    Swarm = blockchain_swarm:swarm(),
    case libp2p_swarm:dial_framed_stream(Swarm, P2PAddress, ?TX_PROTOCOL, Handler, [self()]) of
        {ok, Stream} ->
            lager:info("dialed peer ~p via ~p~n", [RandomConsensusAddress, ?TX_PROTOCOL]),
            libp2p_framed_stream:send(Stream, DataToSend),
            libp2p_framed_stream:close(Stream),
            CallbackFun(ok);
        Other ->
            lager:notice("Failed to dial ~p service on ~p : ~p", [?TX_PROTOCOL, RandomConsensusAddress, Other]),
            case Retries > 0 of
                true ->
                    erlang:send_after(1000, self(), {submit, Transaction, Receivers, Handler, Retries - 1, CallbackFun}),
                    CallbackFun({retry, Retries - 1});
                false ->
                    CallbackFun({error, no_retries_and_failed})
            end
    end,
    {noreply, State};
handle_cast(_, State) ->
    {noreply, State}.

handle_call(_, _, State) ->
    {reply, ok, State}.

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
