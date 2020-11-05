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
         start_link/1,
         dial/1
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
          parent :: pid(),
          txn_key :: blockchain_txn_mgr:txn_key(),
          txn :: blockchain_txn:txn(),
          member :: libp2p_crypto:pubkey_bin(),
          timeout = make_ref() :: reference()
         }).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

dial(Pid) ->
    gen_server:cast(Pid, dial).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init(Args) ->
    lager:debug("blockchain_txn_dialer started with ~p", [Args]),
    [Parent, TxnKey, Txn, Member] = Args,
    Ref = erlang:send_after(30000, Parent, {timeout, {self(), TxnKey, Txn, Member}}),
    {ok, #state{parent=Parent, txn_key = TxnKey, txn=Txn, member=Member, timeout=Ref}}.

handle_call(_, _, State) ->
    {reply, ok, State}.

handle_cast(dial, State=#state{member=Member, txn_key = TxnKey, txn=Txn, parent=Parent, timeout=Ref}) ->
    Swarm = blockchain_swarm:swarm(),
    P2PAddress = libp2p_crypto:pubkey_bin_to_p2p(Member),
    TxnHash = blockchain_txn:hash(Txn),
    case libp2p_swarm:dial_framed_stream(Swarm,
                                         P2PAddress,
                                         ?TX_PROTOCOL,
                                         blockchain_txn_handler,
                                         [self(), blockchain_txn:hash(Txn)]) of
        {error, Reason} ->
            erlang:cancel_timer(Ref),
            lager:error("libp2p_framed_stream dial failed. Reason: ~p, To: ~p, TxnHash: ~p",
                        [Reason, P2PAddress, TxnHash]),
            Parent ! {dial_failed, {self(), TxnKey, Txn, Member}},
            {stop, normal, State};
        {ok, Stream} ->
            DataToSend = blockchain_txn:serialize(Txn),
            case libp2p_framed_stream:send(Stream, DataToSend) of
                {error, Reason} ->
                    erlang:cancel_timer(Ref),
                    lager:error("libp2p_framed_stream send failed. Reason: ~p, To: ~p, TxnHash: ~p",
                                [Reason, P2PAddress, TxnHash]),
                    Parent ! {send_failed, {self(), TxnKey, Txn, Member}},
                    {stop, normal, State};
                _ ->
                    {noreply, State}
            end
    end;
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({blockchain_txn_response, {ok, _TxnHash}}, State=#state{parent=Parent, txn_key = TxnKey, txn=Txn, member=Member, timeout=Ref}) ->
    erlang:cancel_timer(Ref),
    Parent ! {accepted, {self(), TxnKey, Txn, Member}},
    {stop, normal, State};
handle_info({blockchain_txn_response, {no_group, _TxnHash}}, State=#state{parent=Parent, txn_key = TxnKey, txn=Txn, member=Member, timeout=Ref}) ->
    erlang:cancel_timer(Ref),
    Parent ! {no_group, {self(), TxnKey, Txn, Member}},
    {stop, normal, State};
handle_info({blockchain_txn_response, {error, _TxnHash}}, State=#state{parent=Parent, txn_key = TxnKey, txn=Txn, member=Member, timeout=Ref}) ->
    erlang:cancel_timer(Ref),
    Parent ! {rejected, {self(), TxnKey, Txn, Member}},
    {stop, normal, State};
handle_info(_Msg, State) ->
    lager:info("txn dialer got unexpected info ~p", [_Msg]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

