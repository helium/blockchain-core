%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Price Oracle ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_price_oracle_v1).

-behavior(blockchain_txn).

-include("blockchain_utils.hrl").
-include("blockchain_vars.hrl").
-include_lib("helium_proto/include/blockchain_txn_price_oracle_v1_pb.hrl").

-define(MAX_HEIGHT_DIFF, 10).

-export([
    new/3,
    hash/1,
    price/1,
    oracle_public_key/1,
    block_height/1,
    signature/1,
    fee/1,
    sign/2,
    is_valid/2,
    absorb/2,
    print/1
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type txn_price_oracle() :: #blockchain_txn_price_oracle_v1_pb{}.
-export_type([txn_price_oracle/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(OraclePublicKey :: binary(), Price :: non_neg_integer(),
          BlockHeight :: non_neg_integer()) -> txn_price_oracle().
new(OraclePK, Price, BlockHeight) ->
    #blockchain_txn_price_oracle_v1_pb{
       oracle_public_key=OraclePK,
       price=Price,
       block_height = BlockHeight,
       signature = <<>>
      }.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec hash(txn_price_oracle()) -> blockchain_txn:hash().
hash(Txn) ->
    BaseTxn = Txn#blockchain_txn_price_oracle_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_price_oracle_v1_pb:encode_msg(BaseTxn),
    crypto:hash(sha256, EncodedTxn).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec price(txn_price_oracle()) -> non_neg_integer().
price(Txn) ->
    Txn#blockchain_txn_price_oracle_v1_pb.price.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec signature(txn_price_oracle()) -> binary().
signature(Txn) ->
    Txn#blockchain_txn_price_oracle_v1_pb.signature.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec block_height(txn_price_oracle()) -> non_neg_integer().
block_height(Txn) ->
    Txn#blockchain_txn_price_oracle_v1_pb.block_height.


%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec oracle_public_key(txn_price_oracle()) -> binary().
oracle_public_key(Txn) ->
    Txn#blockchain_txn_price_oracle_v1_pb.oracle_public_key.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec fee(txn_price_oracle()) -> non_neg_integer().
fee(_Txn) ->
    0.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec sign(txn_price_oracle(), libp2p_crypto:sig_fun()) -> txn_price_oracle().
sign(Txn, SigFun) ->
    Zeroed = Txn#blockchain_txn_price_oracle_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_price_oracle_v1_pb:encode_msg(Zeroed),
    Txn#blockchain_txn_price_oracle_v1_pb{signature=SigFun(EncodedTxn)}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_valid(txn_price_oracle(), blockchain:blockchain()) -> ok | {error, any()}.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Price = ?MODULE:price(Txn),
    Signature = ?MODULE:signature(Txn),
    OraclePK = ?MODULE:oracle_public_key(Txn),
    BlockHeight = ?MODULE:block_height(Txn),
    BaseTxn = Txn#blockchain_txn_price_oracle_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_price_oracle_v1_pb:encode_msg(BaseTxn),
    case blockchain_txn:validate_fields([{{oracle_public_key, OraclePK},
                                          {member, ?price_oracle_public_keys}}, % XXX: Implement
                                         {{price, Price}, {integer, 1, 100}}]) of
        ok ->
            %% maybe these tests should be reversed...
            %% or maybe we should fold over a list of closures...
            case libp2p_crypto:verify(EncodedTxn, Signature, OraclePK) of
                false ->
                    {error, bad_signature};
                true ->
                    case validate_block_height(BlockHeight,
                                               blockchain_ledger:blockheight(Ledger)) of
                        false ->
                            {error, bad_block_height};
                        true ->
                            ok
                    end
            end;
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb(txn_price_oracle(), blockchain:blockchain()) -> ok | {error, any()}.
absorb(Txn, Chain) ->
    _Ledger = blockchain:ledger(Chain),
    _Fee = ?MODULE:fee(Txn),
    %% TODO: Implement
    ok.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec print(txn_price_oracle()) -> iodata().
print(undefined) -> <<"type=price_oracle, undefined">>;
print(#blockchain_txn_price_oracle_v1_pb{oracle_public_key=OraclePK,
                                         price=Price, block_height = BH,
                                         signature=Sig}) ->
    io_lib:format("type=price_oracle oracle_signature=~p, price=~p, block_height=~p, signature=~p",
                  [OraclePK, Price, BH, Sig]).

%% ------------------------------------------------------------------
%% Private functions
%% ------------------------------------------------------------------
validate_block_height(MsgHeight, Current) when (Current - MsgHeight) < ?MAX_HEIGHT_DIFF ->
    true;
validate_block_height(_MsgHeight, _Current) -> false.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Tx = #blockchain_txn_price_oracle_v1_pb{
        oracle_public_key = <<"pk">>,
        price = 1,
        block_height = 2
    },
    ?assertEqual(Tx, new(<<"pk">>, 1, 2)).

oracle_public_key_test() ->
    Tx = new(<<"pk">>, 1, 2),
    ?assertEqual(<<"pk">>, oracle_public_key(Tx)).

price_test() ->
    Tx = new(<<"pk">>, 1, 2),
    ?assertEqual(1, price(Tx)).

-endif.
