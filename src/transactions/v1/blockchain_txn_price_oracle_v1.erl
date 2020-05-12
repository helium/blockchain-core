%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Price Oracle ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_price_oracle_v1).

-behavior(blockchain_txn).

-include("blockchain_utils.hrl").
-include_lib("helium_proto/include/blockchain_txn_price_oracle_v1_pb.hrl").

-export([
    new/2,
    hash/1,
    price/1,
    oracle_signature/1,
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
-spec new(OracleSignature :: binary(), Price :: non_neg_integer()) -> txn_price_oracle().
new(OracleSig, Price) ->
    #blockchain_txn_price_oracle_v1_pb{
       oracle_signature=OracleSig,
       price=Price,
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
-spec oracle_signature(txn_price_oracle()) -> binary().
oracle_signature(Txn) ->
    Txn#blockchain_txn_price_oracle_v1_pb.oracle_signature.

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
    EncodedTxn = blockchain_txn_price_oracle_v1_pb:encode_msg(Txn),
    Txn#blockchain_txn_price_oracle_v1_pb{signature=SigFun(EncodedTxn)}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_valid(txn_price_oracle(), blockchain:blockchain()) -> ok | {error, any()}.
is_valid(Txn, Chain) ->
    _Ledger = blockchain:ledger(Chain),
    Price = ?MODULE:price(Txn),
    Signature = ?MODULE:signature(Txn),
    PubKey = ?MODULE:oracle_signature(Txn),
    BaseTxn = Txn#blockchain_txn_price_oracle_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_price_oracle_v1_pb:encode_msg(BaseTxn),
    case blockchain_txn:validate_fields([{{oracle_signature, PubKey}, {address, libp2p}}, %% XXX: validate its one of our accepted keys
                                         {{price, Price}, {integer, 1, 100}}]) of
        ok ->
            case libp2p_crypto:verify(EncodedTxn, Signature, PubKey) of
                false ->
                    {error, bad_signature};
                true ->
                    %% TODO: Implement
                    ok
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
print(#blockchain_txn_price_oracle_v1_pb{oracle_signature=OracleSig,
                                         price=Price, signature=Sig}) ->
    io_lib:format("type=price_oracle oracle_signature=~p, price=~p, signature=~p",
                  [OracleSig, Price, Sig]).

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Tx = #blockchain_txn_price_oracle_v1_pb{
        oracle_signature = <<"oracle">>,
        price = 1
    },
    ?assertEqual(Tx, new(<<"oracle">>, 1)).

oracle_signature_test() ->
    Tx = new(<<"oracle">>, 1),
    ?assertEqual(<<"oracle">>, oracle_signature(Tx)).

price_test() ->
    Tx = new(<<"oracle">>, 1),
    ?assertEqual(1, price(Tx)).

-endif.
