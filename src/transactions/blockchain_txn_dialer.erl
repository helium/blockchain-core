%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Dialer ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_dialer).

-behavior(gen_server).

-include("blockchain.hrl").

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------
-export([
         start_link/1
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
          parent,
          txn,
          member
         }).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init(Args) ->
    lager:info("blockchain_txn_dialer started with ~p", [Args]),
    [Parent, Txn, Member] = Args,
    {ok, #state{parent=Parent, txn=Txn, member=Member}}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_call(_, _, State) ->
    {reply, ok, State}.

handle_info(dial, State) ->
    Swarm = blockchain_swarm:swarm(),
    P2PAddress = libp2p_crypto:pubkey_bin_to_p2p(State#state.member),
    TxnHash = blockchain_txn:hash(State#state.txn),
    case libp2p_swarm:dial_framed_stream(Swarm,
                                         P2PAddress,
                                         ?TX_PROTOCOL,
                                         blockchain_txn_handler,
                                         [self(), blockchain_txn:hash(State#state.txn)]) of
        {error, Reason} ->
            lager:error("libp2p_framed_stream dial failed. Reason: ~p, To: ~p, TxnHash: ~p",
                        [Reason, P2PAddress, TxnHash]);
        {ok, Stream} ->
            DataToSend = blockchain_txn:serialize(State#state.txn),
            case libp2p_framed_stream:send(Stream, DataToSend) of
                {error, Reason} ->
                    lager:error("libp2p_framed_stream send failed. Reason: ~p, To: ~p, TxnHash: ~p",
                                [Reason, P2PAddress, TxnHash]);
                _ ->
                    ok
            end
    end,
    {noreply, State};
handle_info({blockchain_txn_response, {ok, _TxnHash}}, State) ->
    State#state.parent ! {accepted, {self(), State#state.txn, State#state.member}},
    {noreply, State};
handle_info({blockchain_txn_response, {error, _TxnHash}}, State) ->
    State#state.parent ! {rejected, {self(), State#state.txn, State#state.member}},
    % retry
    erlang:send_after(1000, self(), dial),
    {noreply, State};
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

