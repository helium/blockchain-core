%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Dialer ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_dialer).

-behavior(gen_server).

-include("blockchain.hrl").
-include_lib("helium_proto/include/blockchain_txn_handler_pb.hrl").

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
          request_type :: atom(),
          txn_key :: blockchain_txn_mgr:txn_key(),
          txn :: blockchain_txn:txn(),
          member :: libp2p_crypto:pubkey_bin(),
          timeout = make_ref() :: reference(),
          protocol_version :: undefined | string() % set at dial
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
    [Parent, RequestType, TxnKey, Txn, Member] = Args,
    Ref = erlang:send_after(30000, Parent, {dial_timeout, {RequestType, {self(), TxnKey, Txn, Member}}}),
    {ok, #state{parent=Parent, request_type = RequestType, txn_key = TxnKey, txn=Txn, member=Member, timeout=Ref}}.

handle_call(_, _, State) ->
    {reply, ok, State}.

handle_cast(dial, State=#state{}) ->
    case dial_(State) of
        ok ->
            {noreply, State};
        {error, _} ->
            {stop, normal, State}
    end;
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({blockchain_txn_response, Resp},
    State=#state{parent=Parent, request_type = ReqType, txn_key = TxnKey, txn=Txn, member=Member, timeout=Ref}) ->
    erlang:cancel_timer(Ref),
    Parent ! {blockchain_txn_response, {ReqType, {self(), TxnKey, Txn, Member, Resp}}},
    {stop, normal, State};
handle_info(_Msg, State) ->
    lager:info("txn dialer got unexpected info ~p", [_Msg]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Internal ===================================================================

-spec dial_(#state{}) -> ok | {error, _}.
dial_(#state{member=Member, request_type = RequestType, txn_key = TxnKey, txn=Txn, parent=Parent, timeout=Ref}) ->
    SwarmTID = blockchain_swarm:tid(),
    P2PAddress = libp2p_crypto:pubkey_bin_to_p2p(Member),
    TxnHash = blockchain_txn:hash(Txn),
    (fun
        Dial ([]) ->
            lager:warning("txn dialing failed - no compatible protocol versions"),
            Parent ! {dial_failed, {RequestType, {self(), TxnKey, Txn, Member}}},
            {error, no_supported_protocols};
        Dial ([ProtocolVersion | SupportedProtocolVersions]) ->
            case
                libp2p_swarm:dial_framed_stream(
                    SwarmTID,
                    P2PAddress,
                    ProtocolVersion,
                    blockchain_txn_handler,
                    [ProtocolVersion, self(), blockchain_txn:hash(Txn)]
                )
            of
                {error, protocol_unsupported} ->
                    lager:debug(
                        "txn dialing failed with protocol version: ~p, "
                        "trying next supported protocol version.",
                        [ProtocolVersion]
                    ),
                    Dial(SupportedProtocolVersions);
                {error, Reason}=Error ->
                    erlang:cancel_timer(Ref),
                    lager:error(
                        "libp2p_framed_stream dial failed. "
                        "Reason: ~p, To: ~p, TxnHash: ~p",
                        [Reason, P2PAddress, TxnHash]
                    ),
                    Parent ! {dial_failed, {RequestType, {self(), TxnKey, Txn, Member}}},
                    Error;
                {ok, Stream} ->
                    %% if the negotiated protocol does not support the request type, then
                    %% dont be sending it, just fail things
                    case blockchain_txn_handler:send_request(Stream, ProtocolVersion, RequestType, TxnKey, Txn) of
                        {error, req_not_supported} = Error ->
                            lager:info("req_not_supported for type ~p and txn ~p", [RequestType, Txn]),
                            erlang:cancel_timer(Ref),
                            Parent ! {req_not_supported, {RequestType, {self(), TxnKey, Txn, Member}}},
                            Error;
                        {error, Reason} = Error ->
                            lager:error(
                                        "blockchain_txn_handler send failed. "
                                        "Reason: ~p, To: ~p, TxnHash: ~p",
                                        [Reason, P2PAddress, TxnHash]
                                    ),
                            Parent ! {send_failed, {RequestType, {self(), TxnKey, Txn, Member}}},
                            Error;
                        ok ->
                            ok
                    end
            end
    end)(?SUPPORTED_TX_PROTOCOLS).
