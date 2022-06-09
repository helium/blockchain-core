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
submit(_Ctx, #txn_submit_req_v1_pb{txn = WrappedTxn}) ->
    Txn = blockchain_txn:unwrap_txn(WrappedTxn),
    .

-spec query(ctx:ctx(), transaction_pb:txn_query_req_v1_pb()) ->
    {ok, transaction_pb:txn_query_resp_v1_pb()} | {error, grpcbox_stream:grpc_error_response()}.
query() ->
    .
