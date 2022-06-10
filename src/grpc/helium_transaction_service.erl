%%%-------------------------------------------------------------------
%%
%% Handler for the transaction service's unary submit/query API/RPC
%%
%%%-------------------------------------------------------------------
-module(helium_transaction_service).

-behaviour(helium_transaction_bhvr).

-include("grpc/autogen/server/transaction_pb.hrl").

-export([submit/2, query/2]).

%% ------------------------------------------------------------------
%% helium_transaction_bhvr unary callbacks
%% ------------------------------------------------------------------
-spec submit(ctx:ctx(), transaction_pb:txn_submit_req_v1_pb()) ->
    {ok, transaction_pb:txn_submit_resp_v1_pb()} | {error, grpcbox_stream:grpc_error_response()}.
submit(Ctx, #txn_submit_req_v1_pb{txn = WrappedTxn}) ->
    Txn = blockchain_txn:unwrap_txn(WrappedTxn),
    {ok, TxnKey} = blockchain_txn_mgr:grpc_submit(Txn),

    Resp = #txn_submit_resp_v1_pb{key = TxnKey,
                                  routing_address = RoutingAddr,
                                  height = current_height(),
                                  signature = <<>>},
    SignedResp = sign_resp(Resp, txn_submit_resp_v1_pb),

    {ok, Resp#txn_submit_resp_v1_pb{signature = SignedResp}, Ctx}.

-spec query(ctx:ctx(), transaction_pb:txn_query_req_v1_pb()) ->
    {ok, transaction_pb:txn_query_resp_v1_pb()} | {error, grpcbox_stream:grpc_error_response()}.
query(Ctx, #txn_query_req_v1_pb{key = Key}) ->
    Resp = case blockchain_txn_mgr:txn_status(Key) of
               {ok, pending, CacheMap} ->
                   Acceptors = lists:map(fun(Acc) -> acceptor_to_record(Acc) end, maps:get(acceptors, CacheMap)),
                   Rejectors = lists:map(fun(Rej) -> rejector_to_record(Rej) end, maps:get(rejectors, CacheMap)),
                   #txn_query_resp_v1_pb{
                       status = pending,
                       acceptors = Acceptors,
                       rejectors = Rejectors,
                       %% TODO: is the details field still needed?
                       details = <<>>,
                       height = maps:get(recv_block_height, CacheMap),
                       signature = <<>>
                   };
               {error, txn_not_found} ->
                   %% TODO: do we even need a `pending | failed` status anymore or
                   %% just return pending txns or else a grpcbox_stream error?
                   #txn_query_resp_v1_pb{
                       status = failed,
                       acceptors = [],
                       rejectors = [],
                       details = <<>>,
                       height = undefined,
                       signature = <<>>
                   }
           end,

    SignedResp = sign_resp(Resp, txn_query_resp_v1_pb),
    {ok, Resp#txn_query_resp_v1_pb{signature = SignedResp}, Ctx}.

%% ------------------------------------------------------------------
%% internal functions
%% ------------------------------------------------------------------
-spec routing_info() -> transaction_pb:routing_address_pb().
routing_addr() ->
    PubKeyBin = blockchain_swarm:pubkey_bin(),
    URI = blockchain_utils:addr2uri(PubKeyBin),
    #routing_address_pb{pub_key = PubKeyBin, uri = URI}.

-spec sign_resp(#transaction_pb:txn_submit_resp_v1_pb() |
                #transaction_pb:txn_query_resp_v1_pb(), atom()) -> binary().
sign_resp(Resp, Type) ->
    {ok, _, SigFun, _} = blockchain_swarm:keys(),
    EncodedRespBin = transaction_pb:encode_msg(Resp, Type),
    SigFun(EncodedRespBin).

-spec current_height() -> pos_integer() | undefined.
current_height() ->
    Chain = blockchain_worker:blockchain(),
    try blockchain:height(Chain) of
        {ok, Height} when is_integer(Height) -> Height
    catch
        %% we may have submitted a txn before the receiver has the chain
        %% in which case the blockchain_txn_mgr has cached the txn for later submission
        _ -> undefined
    end

-spec acceptor_to_record({Member :: libp2p_crypto:pubkey_bin(),
                          Height :: non_neg_integer() | undefined,
                          QueuePos :: non_neg_integer() | undefined,
                          QueueLen :: non_neg_integer() | undefined}) -> transaction_pb:acceptor_pb().
acceptor_to_record({Member, Height, QueuePos, QueueLen}) ->
    #acceptor_pb{height = Height, queue_pos = QueuePos, queue_len = QueueLen, pub_key = Member}.

-spec rejector_to_record({Member :: libp2p_crypto:pubkey_bin(),
                          Height :: non_neg_integer() | undefined,
                          RejectReason :: atom() | undefined}) -> transaction_pb:rejector_pb().
rejector_to_reecord({Member, Height, RejectReason}) ->
    #rejector_pb{height = Height, reason = RejectReason, pub_key = Member}.
