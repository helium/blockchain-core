%%%-------------------------------------------------------------------
%%
%% Handler for the transaction service's unary submit/query API/RPC
%%
%%%-------------------------------------------------------------------
-module(helium_transaction_service).

-behaviour(helium_transaction_transaction_bhvr).
-include("autogen/server/transaction_pb.hrl").

-export([submit/2, query/2]).

%% ------------------------------------------------------------------
%% helium_transaction_bhvr unary callbacks
%% ------------------------------------------------------------------
-spec submit(ctx:ctx(), transaction_pb:txn_submit_req_v1_pb()) ->
    {ok, transaction_pb:txn_submit_resp_v1_pb()} | grpcbox_stream:grpc_error_response().
submit(Ctx, #txn_submit_req_v1_pb{txn = WrappedTxn, key = Key}) ->
    Txn = blockchain_txn:unwrap_txn(WrappedTxn),
    {ok, Key, CurHeight} = blockchain_txn_mgr:grpc_submit(Txn, Key),
    Resp = #txn_submit_resp_v1_pb{key = Key,
                                  validator = routing_info(),
                                  recv_height = CurHeight,
                                  signature = <<>>},
    SignedResp = sign_resp(Resp, txn_submit_resp_v1_pb),
    {ok, Resp#txn_submit_resp_v1_pb{signature = SignedResp}, Ctx}.

-spec query(ctx:ctx(), transaction_pb:txn_query_req_v1_pb()) ->
    {ok, transaction_pb:txn_query_resp_v1_pb()} | grpcbox_stream:grpc_error_response().
query(Ctx, #txn_query_req_v1_pb{key = Key}) ->
    Resp = case blockchain_txn_mgr:txn_status(Key) of
               {ok, Status = pending, CacheMap} ->
                   Acceptors = lists:map(fun(Acc) -> acceptor_to_record(Acc) end, maps:get(acceptors, CacheMap)),
                   Rejectors = lists:map(fun(Rej) -> rejector_to_record(Rej) end, maps:get(rejectors, CacheMap)),
                   #txn_query_resp_v1_pb{
                       status = Status,
                       key = maps:get(key, CacheMap),
                       height = maps:get(height, CacheMap),
                       recv_height = maps:get(recv_block_height, CacheMap),
                       acceptors = Acceptors,
                       rejectors = Rejectors,
                       signature = <<>>
                   };
               {error, txn_not_found, CurHeight} ->
                   #txn_query_resp_v1_pb{
                       status = not_found,
                       key = Key,
                       height = CurHeight,
                       acceptors = [],
                       rejectors = [],
                       signature = <<>>
                   }
           end,
    SignedResp = sign_resp(Resp, txn_query_resp_v1_pb),
    {ok, Resp#txn_query_resp_v1_pb{signature = SignedResp}, Ctx}.

%% ------------------------------------------------------------------
%% internal functions
%% ------------------------------------------------------------------
-spec routing_info() -> transaction_pb:routing_address_pb().
routing_info() ->
    PubKeyBin = blockchain_swarm:pubkey_bin(),
    URI = blockchain_utils:addr2uri(PubKeyBin),
    #routing_address_pb{pub_key = PubKeyBin, uri = URI}.

-spec sign_resp(transaction_pb:txn_submit_resp_v1_pb() |
                transaction_pb:txn_query_resp_v1_pb(), atom()) -> binary().
sign_resp(Resp, Type) ->
    {ok, _, SigFun, _} = blockchain_swarm:keys(),
    EncodedRespBin = transaction_pb:encode_msg(Resp, Type),
    SigFun(EncodedRespBin).

-spec acceptor_to_record({Member :: libp2p_crypto:pubkey_bin(),
                          Height :: non_neg_integer() | undefined,
                          QueuePos :: non_neg_integer() | undefined,
                          QueueLen :: non_neg_integer() | undefined}) -> transaction_pb:acceptor_pb().
acceptor_to_record({Member, Height, QueuePos, QueueLen}) ->
    #acceptor_pb{height = Height, queue_pos = QueuePos, queue_len = QueueLen, pub_key = Member}.

-spec rejector_to_record({Member :: libp2p_crypto:pubkey_bin(),
                          Height :: non_neg_integer() | undefined,
                          RejectReason :: atom() | undefined}) -> transaction_pb:rejector_pb().
rejector_to_record({Member, Height, RejectReason}) ->
    #rejector_pb{height = Height, reason = RejectReason, pub_key = Member}.
