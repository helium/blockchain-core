%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Block ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_block).

-export([
    new/5,
    prev_hash/1,
    height/1,
    transactions/1,
    signature/1,
    meta/1,
    remove_signature/1,
    sign_block/2,
    new_genesis_block/1, new_genesis_block/2,
    is_genesis/1,
    is_block/1,
    hash_block/1,
    verify_signature/4,
    payment_transactions/1,
    coinbase_transactions/1,
    add_gateway_transactions/1,
    assert_location_transactions/1,
    poc_request_transactions/1,
    serialize/1,
    deserialize/1
]).

-include("blockchain.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(block, {
    prev_hash :: blockchain_block:hash(),
    height = 0 :: non_neg_integer(),
    transactions = [] :: blockchain_transactions:transactions(),
    signature :: binary(),
    meta = #{} :: #{any() => any()}
}).

-type block() :: #block{}.
-type hash() :: <<_:256>>. %% SHA256 digest
-type meta() :: #{any() => any()}.

-export_type([block/0, hash/0, meta/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(hash(), non_neg_integer() ,blockchain_transactions:transactions()
          ,binary(), meta()) -> block().
new(PrevHash, Height, Transactions, Signature, Meta) ->
    #block{
        prev_hash=PrevHash,
        height=Height,
        transactions=Transactions,
        signature=Signature,
        meta=Meta
    }.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec prev_hash(block()) -> hash().
prev_hash(Block) ->
    Block#block.prev_hash.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec height(block()) -> non_neg_integer().
height(Block) ->
    Block#block.height.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec transactions(block()) -> blockchain_transactions:transactions().
transactions(Block) ->
    Block#block.transactions.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec signature(block()) -> binary().
signature(Block) ->
    Block#block.signature.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec meta(block()) -> meta().
meta(Block) ->
    Block#block.meta.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec remove_signature(block()) -> block().
remove_signature(Block) ->
    Block#block{signature = <<>>}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec sign_block(binary(), block()) -> block().
sign_block(Signature, Block) ->
    Block#block{signature=Signature}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new_genesis_block(blockchain_transactions:transactions()) -> block().
new_genesis_block(Transactions) ->
    ?MODULE:new(<<0:256>>, 1, Transactions, <<>>, #{}).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new_genesis_block(blockchain_transactions:transactions(), meta()) -> block().
new_genesis_block(Transactions, MetaData) ->
    ?MODULE:new(<<0:256>>, 1, Transactions, <<>>, MetaData).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_genesis(block()) -> boolean().
is_genesis(Block) ->
    case prev_hash(Block) == <<0:256>> andalso height(Block) == 1 of
        true -> true;
        _ -> false
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_block(block()) -> boolean().
is_block(Block) ->
    erlang:is_record(Block, block).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec hash_block(block()) -> hash().
hash_block(Block) ->
    crypto:hash(sha256, erlang:term_to_binary(?MODULE:remove_signature(Block))).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec verify_signature(binary() | block(), [libp2p_crypto:address()], binary(), pos_integer()) ->
    false | {true, [{libp2p_crypto:address(), binary()}]}.
verify_signature(#block{}=Block, ConsensusMembers, BinSigs, Threshold) ->
    BinBlock = erlang:term_to_binary(?MODULE:remove_signature(Block)),
    verify_signature(BinBlock, ConsensusMembers, BinSigs, Threshold);
verify_signature(Artifact, ConsensusMembers, BinSigs, Threshold) ->
    ValidSignatures = lists:foldl(
        fun({Addr, Sig}, Acc) ->
            case
                lists:member(Addr, ConsensusMembers)
                andalso (not lists:keymember(Addr, 1, Acc))
                andalso libp2p_crypto:verify(Artifact, Sig, libp2p_crypto:bin_to_pubkey(Addr))
            of
                true -> [{Addr, Sig} | Acc];
                false -> Acc
            end
        end
        ,[]
        ,erlang:binary_to_term(BinSigs)
    ),
    case length(ValidSignatures) >= Threshold of
        true ->
            %% at least N-F consensus members signed the block
            {true, ValidSignatures};
        false ->
            %% missing some signatures?
            false
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec payment_transactions(block()) -> [blockchain_txn_payment_v1:txn_payment()].
payment_transactions(Block) ->
    lists:filter(fun(Txn) -> blockchain_txn_payment_v1:is(Txn) end,
                 ?MODULE:transactions(Block)).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec coinbase_transactions(block()) -> [blockchain_txn_coinbase_v1:txn_coinbase()].
coinbase_transactions(Block) ->
    lists:filter(fun(Txn) -> blockchain_txn_coinbase_v1:is(Txn) end,
                 ?MODULE:transactions(Block)).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec add_gateway_transactions(block()) -> [blockchain_txn_add_gateway_v1:txn_add_gateway()].
add_gateway_transactions(Block) ->
    lists:filter(fun(Txn) -> blockchain_txn_add_gateway_v1:is(Txn) end,
                 ?MODULE:transactions(Block)).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec assert_location_transactions(block()) -> [blockchain_txn_assert_location_v1:txn_assert_location()].
assert_location_transactions(Block) ->
    lists:filter(fun(Txn) -> blockchain_txn_assert_location_v1:is(Txn) end,
                 ?MODULE:transactions(Block)).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec poc_request_transactions(block()) -> [blockchain_txn_poc_request_v1:txn_poc_request()].
poc_request_transactions(Block) ->
    lists:filter(fun blockchain_txn_poc_request_v1:is/1, ?MODULE:transactions(Block)).

%%--------------------------------------------------------------------
%% @doc
%% Version 1
%% @end
%%--------------------------------------------------------------------
-spec serialize(block()) -> binary().
serialize(Block) ->
    BinBlock = erlang:term_to_binary(Block),
    <<1, BinBlock/binary>>.

%%--------------------------------------------------------------------
%% @doc
%% Later _ could becomre 1, 2, 3 for different versions.
%% @end
%%--------------------------------------------------------------------
-spec deserialize(binary()) -> block().
deserialize(<<_:1/binary, Bin/binary>>) ->
    erlang:binary_to_term(Bin).

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Block = #block{
        prev_hash= <<>>,
        height=1,
        transactions=[],
        signature= <<>>,
        meta= #{}
    },
    ?assertEqual(Block, new(<<>>, 1, [], <<>>, #{})).

prev_hash_test() ->
    Hash = <<"hash">>,
    Block = new(Hash, 1, [], <<>>, #{}),
    ?assertEqual(Hash, prev_hash(Block)).

height_test() ->
    Height = 1,
    Block = new(<<>>, Height, [], <<>>, #{}),
    ?assertEqual(Height, height(Block)).

transactions_test() ->
    Txs = [1],
    Block = new(<<>>, 1, Txs, <<>>, #{}),
    ?assertEqual(Txs, transactions(Block)).

signature_test() ->
    Sig = <<"signature">>,
    Block = new(<<>>, 1, [], Sig, #{}),
    ?assertEqual(Sig, signature(Block)).

meta_test() ->
    Meta = #{1 => 1},
    Block = new(<<>>, 1, [], <<>>, Meta),
    ?assertEqual(Meta, meta(Block)).

remove_signature_test() ->
    Sig = <<"signature">>,
    Block = new(<<>>, 1, [], Sig, #{}),
    ?assertEqual(<<>>, signature(remove_signature(Block))).

sign_block_test() ->
    Sig = <<"signature">>,
    Block = new(<<>>, 1, [], <<>>, #{}),
    ?assertEqual(Sig, signature(sign_block(Sig, Block))).

new_genesis_block_test() ->
    Txs = [1, 2, 3],
    Block = new_genesis_block(Txs),
    ?assertEqual(<<0:256>>, prev_hash(Block)),
    ?assertEqual(1, height(Block)),
    ?assertEqual(Txs, transactions(Block)),
    ?assertEqual(<<>>, signature(Block)),
    ?assertEqual(#{}, meta(Block)).

is_genesis_test() ->
    ?assertEqual(true, is_genesis(new_genesis_block([]))),
    ?assertEqual(false, is_genesis(new(<<>>, 1, [], <<>>, #{}))).

is_block_test() ->
    ?assertEqual(true, is_block(new_genesis_block([]))),
    ?assertEqual(false, is_block(#{})).

verify_signature_test() ->
    Keys = generate_keys(10),
    [{Payer, {_, PayerPrivKey, _}}, {Recipient, _}|_] = Keys,
    Tx = blockchain_txn_payment_v1:new(Payer, Recipient, 2500, 10, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    SignedTx = blockchain_txn_payment_v1:sign(Tx, SigFun),
    Block0 = blockchain_block:new(<<>>, 2, [SignedTx], <<>>, #{}),
    BinBlock = erlang:term_to_binary(blockchain_block:remove_signature(Block0)),
    Signatures =
        lists:foldl(
            fun({A, {_, _, F}}, Acc) ->
                Sig = F(BinBlock),
                [{A, Sig}|Acc]
            end,
            [],
            Keys
        ),
    BinSigs = erlang:term_to_binary(Signatures),
    Block1 = blockchain_block:sign_block(BinSigs, Block0),
    ConsensusMembers = [Addr || {Addr, _} <- Keys],
    ?assertMatch({true, _}, verify_signature(Block1, ConsensusMembers, BinSigs, 7)),
    ?assertMatch(false, verify_signature(Block1, ConsensusMembers, BinSigs, 20)),
    ?assertMatch(false, verify_signature(Block1, [], BinSigs, 7)),
    ok.

serialize_deserialize_test() ->
    Block = new_genesis_block([]),
    ?assertEqual(Block, deserialize(serialize(Block))).

generate_keys(N) ->
    lists:foldl(
        fun(_, Acc) ->
            Keys = libp2p_crypto:generate_keys(ed25519),
            PrivKey = maps:get(secret, Keys),
            PubKey = maps:get(public, Keys),
            SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
            [{libp2p_crypto:pubkey_to_bin(PubKey), {PubKey, PrivKey, SigFun}}|Acc]
        end,
        [],
        lists:seq(1, N)
    ).

-endif.
