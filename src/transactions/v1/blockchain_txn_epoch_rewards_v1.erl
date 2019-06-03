%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Epoch Rewards ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_epoch_rewards_v1).

-behavior(blockchain_txn).

-include("../../pb/blockchain_txn_epoch_rewards_v1_pb.hrl").

-export([
    new/2,
    hash/1,
    start_of_epoch/1,
    end_of_epoch/1,
    signature/1,
    fee/1,
    sign/2,
    is_valid/2,
    absorb/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type txn_epoch_rewards() :: #blockchain_txn_epoch_rewards_v1_pb{}.
-export_type([txn_epoch_rewards/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(binary(), binary()) -> txn_epoch_rewards().
new(Start, End) ->
    #blockchain_txn_epoch_rewards_v1_pb{start_of_epoch=Start, end_of_epoch=End}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec hash(txn_epoch_rewards()) -> blockchain_txn:hash().
hash(Txn) ->
    BaseTxn = Txn#blockchain_txn_epoch_rewards_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_epoch_rewards_v1_pb:encode_msg(BaseTxn),
    crypto:hash(sha256, EncodedTxn).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec start_of_epoch(txn_epoch_rewards()) -> binary().
start_of_epoch(Txn) ->
    Txn#blockchain_txn_epoch_rewards_v1_pb.start_of_epoch.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec end_of_epoch(txn_epoch_rewards()) -> binary().
end_of_epoch(Txn) ->
    Txn#blockchain_txn_epoch_rewards_v1_pb.end_of_epoch.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec signature(txn_epoch_rewards()) -> binary().
signature(Txn) ->
    Txn#blockchain_txn_epoch_rewards_v1_pb.signature.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec fee(txn_epoch_rewards()) -> non_neg_integer().
fee(_Txn) ->
    0.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec sign(txn_epoch_rewards(), libp2p_crypto:sig_fun()) -> txn_epoch_rewards().
sign(Txn, SigFun) ->
    EncodedTxn = blockchain_txn_epoch_rewards_v1_pb:encode_msg(Txn),
    Txn#blockchain_txn_epoch_rewards_v1_pb{signature=SigFun(EncodedTxn)}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_valid(txn_epoch_rewards(), blockchain:blockchain()) -> ok | {error, any()}.
is_valid(_Txn, _Chain) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb(txn_epoch_rewards(), blockchain:blockchain()) -> ok | {error, any()}.
absorb(_Txn, _Chain) ->
    ok.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Tx = #blockchain_txn_epoch_rewards_v1_pb{start_of_epoch= <<"start_of_epoch">>, end_of_epoch= <<"end_of_epoch">>},
    ?assertEqual(Tx, new(<<"start_of_epoch">>, <<"end_of_epoch">>)).

start_of_epoch_test() ->
    Tx = new(<<"start_of_epoch">>, <<"end_of_epoch">>),
    ?assertEqual(<<"start_of_epoch">>, start_of_epoch(Tx)).

end_of_epoch_test() ->
    Tx = new(<<"start_of_epoch">>, <<"end_of_epoch">>),
    ?assertEqual(<<"end_of_epoch">>, end_of_epoch(Tx)).

-endif.
