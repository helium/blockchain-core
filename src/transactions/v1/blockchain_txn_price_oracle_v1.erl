%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Price Oracle ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_price_oracle_v1).

-dialyzer([
    {nowarn_function, is_valid/2}
]).

-behavior(blockchain_txn).
-behavior(blockchain_json).

-include("blockchain.hrl").
-include("blockchain_json.hrl").
-include("blockchain_utils.hrl").
-include("blockchain_vars.hrl").
-include_lib("helium_proto/include/blockchain_txn_price_oracle_v1_pb.hrl").

-export([
    new/3,
    hash/1,
    price/1,
    public_key/1,
    block_height/1,
    signature/1,
    fee/1,
    fee_payer/2,
    sign/2,
    is_valid/2,
    is_well_formed/1,
    is_prompt/2,
    absorb/2,
    print/1,
    json_type/0,
    to_json/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(T, #blockchain_txn_price_oracle_v1_pb).

-type t() :: txn_price_oracle().

-type txn_price_oracle() :: #blockchain_txn_price_oracle_v1_pb{}.

-export_type([t/0, txn_price_oracle/0]).

%%--------------------------------------------------------------------
%% @doc
%% Create a new price oracle transaction.
%% The public key should be
%% <code>
%% base64:encode(<<Len/integer, Key/binary>>)
%% </code>
%% Price should be provided in 1/100,000,000ths of a cent: 100000000=$1
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
%% The price estimate in one hundred millionth of a cent (1/100_000_000th cent).
%% USD$1=100000000
%% %% This is a uint64 under the hood.
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

-spec fee_payer(txn_price_oracle(), blockchain_ledger_v1:ledger()) -> libp2p_crypto:pubkey_bin() | undefined.
fee_payer(_Txn, _Ledger) ->
    undefined.

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
%% price is an integer and not negative, that the public key for
%% this price is a member of the list of approved oracles, and
%% that this transaction is within an "acceptable" distance from
%% when the transaction was submitted vs. the current block height.
%%
%% The acceptable distance is controlled by the
%% `price_oracle_height_delta' chain variable.
%% @end
%%--------------------------------------------------------------------
-spec is_valid(txn_price_oracle(), blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Price = ?MODULE:price(Txn),
    Signature = ?MODULE:signature(Txn),
    RawTxnPK = ?MODULE:public_key(Txn),
    TxnPK = libp2p_crypto:bin_to_pubkey(RawTxnPK),
    BlockHeight = ?MODULE:block_height(Txn),
    {ok, LedgerHeight} = blockchain_ledger_v1:current_height(Ledger),
    BaseTxn = Txn#blockchain_txn_price_oracle_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_price_oracle_v1_pb:encode_msg(BaseTxn),
    {ok, RawOracleKeys} = blockchain:config(?price_oracle_public_keys, Ledger),
    {ok, MaxHeight} = blockchain:config(?price_oracle_height_delta, Ledger),
    OracleKeys = blockchain_utils:bin_keys_to_list(RawOracleKeys),

    case blockchain_txn:validate_fields([{{oracle_public_key, RawTxnPK}, {member, OracleKeys}},
                                         {{price, Price}, {is_integer, 0}}]) of
        ok ->
            case libp2p_crypto:verify(EncodedTxn, Signature, TxnPK) of
                false ->
                    {error, bad_signature};
                true ->
                    case validate_block_height(RawTxnPK, BlockHeight,
                                               LedgerHeight, MaxHeight, Ledger) of
                        false ->
                            {error, bad_block_height};
                        true ->
                            ok
                    end
            end;
        Error ->
            Error
    end.

-spec is_well_formed(t()) -> ok | {error, {contract_breach, any()}}.
is_well_formed(?T{}) ->
    ok.

-spec is_prompt(t(), blockchain:blockchain()) ->
    {ok, blockchain_txn:is_prompt()} | {error, any()}.
is_prompt(?T{}, _) ->
    {ok, yes}.

%%--------------------------------------------------------------------
%% @doc
%% When this block is absorbed, price entries are stored in the
%% ledger
%% @end
%%--------------------------------------------------------------------
-spec absorb(txn_price_oracle(), blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    {ok, LedgerHeight} = blockchain_ledger_v1:current_height(Ledger),
    {ok, #block_info_v2{time = Time}} = blockchain:get_block_info(LedgerHeight, Chain),

    Entry = blockchain_ledger_oracle_price_entry:new(
              Time,
              LedgerHeight,
              ?MODULE:public_key(Txn),
              ?MODULE:price(Txn)),

    blockchain_ledger_v1:add_oracle_price(Entry, Ledger).

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

json_type() ->
    <<"price_oracle_v1">>.

-spec to_json(txn_price_oracle(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Txn, _Opts) ->
    #{ type => ?MODULE:json_type(),
       hash => ?BIN_TO_B64(hash(Txn)),
       fee => fee(Txn),
       public_key => ?BIN_TO_B58(public_key(Txn)),
       price => price(Txn),
       block_height => block_height(Txn)
     }.

%% ------------------------------------------------------------------
%% Private functions
%% ------------------------------------------------------------------

validate_block_height(PK, MsgHeight, Current, MaxHeight, Ledger)
                                        when (Current - MsgHeight) =< MaxHeight ->
    case blockchain_ledger_v1:current_oracle_price_list(Ledger) of
        {ok, []} -> true;
        {ok, PriceEntries} ->
            MyReportingHeights = [ blockchain_ledger_oracle_price_entry:block_height(Entry)
                                   || Entry <- PriceEntries,
                                      blockchain_ledger_oracle_price_entry:public_key(Entry) == PK],
            %% make sure this is not the empty list if we have not reported
            %% lately by prepending a 0 to the list
            MaxReportedHeight = lists:max([0 | MyReportingHeights]),
            MsgHeight > MaxReportedHeight
    end;
validate_block_height(_PK, _MsgHeight, _Current, _MaxHeight, _Ledger) -> false.


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

block_height_test() ->
    Tx = new(<<"oracle">>, 1, 2),
    ?assertEqual(2, block_height(Tx)).

-endif.
