-module(blockchain_block).

-type block() :: blockchain_block_v1:block().
-type signature() :: {Signer::libp2p_crypto:pubkey_bin(), Signature::binary()}.
-type hash() :: <<_:256>>. %% SHA256 digest

-export_type([block/0, hash/0, signature/0]).

-callback new_genesis_block(blockchain_txn:txns()) -> block().
-callback prev_hash(block()) -> hash().
-callback hash_block(block()) -> hash().
-callback height(block()) -> non_neg_integer().
-callback time(block()) -> non_neg_integer().
-callback is_genesis(block()) -> boolean().
-callback signatures(block()) -> [signature()].
-callback set_signatures(block(), [signature()]) -> block().
-callback verify_signatures(Block::binary() | block(),
                            ConsensueMembers::[libp2p_crypto:pubkey_bin()],
                            Signatures::[signature()],
                            Threshold::pos_integer()) -> false |
                                                         {true, [{libp2p_crypto:pubkey_bin(), binary()}]}.

-include("pb/blockchain_block_pb.hrl").

-export([new_genesis_block/1, new_genesis_block/2,
         prev_hash/1,
         hash_block/1,
         height/1,
         time/1,
         is_genesis/1,
         type/1,
         signatures/1, set_signatures/2,
         verify_signatures/4,
         transactions/1,
         serialize/1,
         deserialize/1
        ]).

new_genesis_block(Transactions) ->
    new_genesis_block(blockchain_block_v1, Transactions).

new_genesis_block(Module, Transactions) ->
    Module:new_genesis_block(Transactions).

-spec prev_hash(block()) -> hash().
prev_hash(Block) ->
    (type(Block)):prev_hash(Block).

-spec hash_block(block()) -> hash().
hash_block(Block) ->
    (type(Block)):hash_block(Block).

-spec height(block()) -> non_neg_integer().
height(Block) ->
    (type(Block)):height(Block).

-spec time(block()) -> non_neg_integer().
time(Block) ->
    (type(Block)):time(Block).

-spec is_genesis(block()) -> boolean().
is_genesis(Block) ->
    (type(Block)):is_genesis(Block).

-spec serialize(block()) -> binary().
serialize(Block) ->
    WrappedBlock =
        case type(Block) of
            blockchain_block_v1 -> #blockchain_block_pb{block={v1, Block}};
            undefined -> erlang:error({unknown_block, Block})
        end,
    blockchain_block_pb:encode_msg(WrappedBlock).

-spec deserialize(binary()) -> block().
deserialize(Bin) ->
    case blockchain_block_pb:decode_msg(Bin, blockchain_block_pb) of
        #blockchain_block_pb{block={v1, Block}} -> Block
    end.

-spec signatures(block()) -> [signature()].
signatures(Block) ->
    (type(Block)):signatures(Block).

-spec set_signatures(block(), [signature()]) -> block().
set_signatures(Block, Signatures) ->
    (type(Block)):set_signatures(Block, Signatures).

-spec verify_signatures(Block::binary() | block(),
                        ConsensueMembers::[libp2p_crypto:pubkey_bin()],
                        Signatures::[signature()],
                        Threshold::pos_integer()) -> false |
                                                     {true, [{libp2p_crypto:pubkey_bin(), binary()}]}.
verify_signatures(Block, ConsensusMembers, Signatures, Threshold) ->
    (type(Block)):verify_signatures(Block, ConsensusMembers, Signatures, Threshold).

-spec transactions(block()) -> blockchain_txn:txns().
transactions(Block) ->
    (type(Block)):transactions(Block).

-spec type(block()) -> atom().
type(#blockchain_block_v1_pb{}) ->
    blockchain_block_v1;
type(_) ->
    undefined.
