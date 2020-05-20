%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Price Oracle ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_price_oracle_v1).

-behavior(blockchain_txn).
%-behavior(blockchain_json).

-include("blockchain_utils.hrl").
-include("blockchain_vars.hrl").
-include_lib("helium_proto/include/blockchain_txn_price_oracle_v1_pb.hrl").

%% Controls how old the price is allowed to be.
%%
%% If the current block height differs from the recorded block height
%% in the message, reject the price.
-define(MAX_HEIGHT_DIFF, 20).

-export([
    new/3,
    hash/1,
    price/1,
    public_key/1,
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
%% Create a new price oracle transaction.
%% The public key should be
%% <code>
%% base64:encode(<<Len/integer, Key/binary>>)
%% </code>
%% Price should be provided in 1/1000ths of a cent: 100000=$1
%% Blockheight is expected to be the current height of the chain when
%% txn is submitted
%% @end
%%--------------------------------------------------------------------
-spec new(OraclePublicKey :: binary(), Price :: non_neg_integer(),
          BlockHeight :: non_neg_integer()) -> txn_price_oracle().
new(OraclePK, Price, BlockHeight) ->
    #blockchain_txn_price_oracle_v1_pb{
       public_key=OraclePK,
       price=Price,
       block_height = BlockHeight,
       signature = <<>>
      }.

%%--------------------------------------------------------------------
%% @doc
%% Provide a raw binary hash of the transaction
%% @end
%%--------------------------------------------------------------------
-spec hash(txn_price_oracle()) -> blockchain_txn:hash().
hash(Txn) ->
    BaseTxn = Txn#blockchain_txn_price_oracle_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_price_oracle_v1_pb:encode_msg(BaseTxn),
    crypto:hash(sha256, EncodedTxn).

%%--------------------------------------------------------------------
%% @doc
%% The price estimate in thousandths of a cent (1/1000th cent). $1=100_000
%% This is a uint64 under the hood.
%% @end
%%--------------------------------------------------------------------
-spec price(txn_price_oracle()) -> non_neg_integer().
price(Txn) ->
    Txn#blockchain_txn_price_oracle_v1_pb.price.

%%--------------------------------------------------------------------
%% @doc
%% The signature from the oracle's private key
%% @end
%%--------------------------------------------------------------------
-spec signature(txn_price_oracle()) -> binary().
signature(Txn) ->
    Txn#blockchain_txn_price_oracle_v1_pb.signature.

%%--------------------------------------------------------------------
%% @doc
%% The block height at the time of this transaction
%% @end
%%--------------------------------------------------------------------
-spec block_height(txn_price_oracle()) -> non_neg_integer().
block_height(Txn) ->
    Txn#blockchain_txn_price_oracle_v1_pb.block_height.


%%--------------------------------------------------------------------
%% @doc
%% Provide the Base64 encoded public key from the oracle for this
%% transaction. See `new/3' for details about the public key
%% encoding.
%% @end
%%--------------------------------------------------------------------
-spec public_key(txn_price_oracle()) -> binary().
public_key(Txn) ->
    Txn#blockchain_txn_price_oracle_v1_pb.public_key.

%%--------------------------------------------------------------------
%% @doc
%% Return the fee for this transaction. (Value: 0)
%% @end
%%--------------------------------------------------------------------
-spec fee(txn_price_oracle()) -> non_neg_integer().
fee(_Txn) ->
    0.

%%--------------------------------------------------------------------
%% @doc
%% Sign the transaction using the provided function.
%% @end
%%--------------------------------------------------------------------
-spec sign(txn_price_oracle(), libp2p_crypto:sig_fun()) -> txn_price_oracle().
sign(Txn, SigFun) ->
    Zeroed = Txn#blockchain_txn_price_oracle_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_price_oracle_v1_pb:encode_msg(Zeroed),
    Txn#blockchain_txn_price_oracle_v1_pb{signature=SigFun(EncodedTxn)}.

%%--------------------------------------------------------------------
%% @doc
%% Validate that this txn has a valid oracle public key, that the
%% price is an integer and not negative, and that the signature is
%% valid
%% @end
%%--------------------------------------------------------------------
-spec is_valid(txn_price_oracle(), blockchain:blockchain()) -> ok | {error, any()}.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Price = ?MODULE:price(Txn),
    Signature = ?MODULE:signature(Txn),
    RawTxnPK = ?MODULE:public_key(Txn),
    BinTxnPK = decode_public_key(RawTxnPK),
    TxnPK = libp2p_crypto:bin_to_pubkey(BinTxnPK),
    BlockHeight = ?MODULE:block_height(Txn),
    {ok, LedgerHeight} = blockchain_ledger_v1:current_height(Ledger),
    BaseTxn = Txn#blockchain_txn_price_oracle_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_price_oracle_v1_pb:encode_msg(BaseTxn),
    {ok, RawOracleKeys} = blockchain:config(?price_oracle_public_keys, Ledger),
    OracleKeys = blockchain_utils:vars_keys_to_list(RawOracleKeys),
    case blockchain_txn:validate_fields([{{oracle_public_key, BinTxnPK}, {member, OracleKeys}},
                                         {{price, Price}, {is_integer, 0}}]) of
        ok ->
            case libp2p_crypto:verify(EncodedTxn, Signature, TxnPK) of
                false ->
                    {error, bad_signature};
                true ->
                    case validate_block_height(BlockHeight, LedgerHeight) of
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
%% When this block is absorbed, it triggers a price recalculation.
%% @end
%%--------------------------------------------------------------------
-spec absorb(txn_price_oracle(), blockchain:blockchain()) -> ok | {error, any()}.
absorb(_Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),


    %% potentially recalculate price
    {ok, Ht} = blockchain_ledger_v1:current_height(Ledger),
    case blockchain:get_block(Ht) of
        {ok, Block} ->
            NewPrice = recalc_price(Block, Chain),
            blockchain_ledger_v1:add_oracle_price(NewPrice, Ledger);
        Other -> Other
    end.

%%--------------------------------------------------------------------
%% @doc
%% Serialize this transaction to iodata.
%% @end
%%--------------------------------------------------------------------
-spec print(txn_price_oracle()) -> iodata().
print(undefined) -> <<"type=price_oracle, undefined">>;
print(#blockchain_txn_price_oracle_v1_pb{public_key=OraclePK,
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

select_blocks_by_time(HrAgo, DayAgo, Block, Acc) ->
    BlockT = blockchain_block:time(Block),
    if
        BlockT > HrAgo andalso BlockT < DayAgo -> [ Block | Acc ];
        BlockT > DayAgo -> return;
        true -> Acc
    end.

recalc_price(Start, Chain) ->
    CurrentTime = erlang:system_time(seconds),
    HrAgo = CurrentTime - 3600, % seconds in an hour
    DayAgo = CurrentTime - 86400, % seconds in a day

    EligibleBlocks = blockchain:fold_chain(
                       fun(Block, Acc) -> select_blocks_by_time(HrAgo, DayAgo, Block, Acc) end,
                       [], Start, Chain),

    median(lists:sort(lists:flatten(
                 [ [ lists:foldl(fun(T, A) ->
                          %% isolate price oracle txns
                                         case blockchain_txn:type(T) of
                                             price_oracle -> [ price(T) | A ];
                                             _ -> A
                                         end
                  end,
                  [], Ts) || Ts <- blockchain_block:transactions(Block) ] || Block <- EligibleBlocks ]))).


median(L) ->
    Middle = length(L) div 2,
    lists:nth(Middle, L).

decode_public_key(B64) ->
    <<Len/integer, Key/bytes>> = base64:decode(B64),
    case Len == byte_size(Key) of
        true -> Key;
        false -> {error, key_does_not_match_keylen}
    end.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Tx = #blockchain_txn_price_oracle_v1_pb{
        public_key = <<"oracle">>,
        price = 1,
        block_height = 2
    },
    ?assertEqual(Tx, new(<<"oracle">>, 1, 2)).

oracle_public_key_test() ->
    Tx = new(<<"oracle">>, 1, 2),
    ?assertEqual(<<"oracle">>, public_key(Tx)).

price_test() ->
    Tx = new(<<"oracle">>, 1, 2),
    ?assertEqual(1, price(Tx)).

-endif.
