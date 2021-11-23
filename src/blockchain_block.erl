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
-callback hbbft_round(block()) -> non_neg_integer().
-callback is_genesis(block()) -> boolean().
-callback signatures(block()) -> [signature()].
-callback set_signatures(block(), [signature()]) -> block().
-callback is_rescue_block(block()) -> boolean().
-callback is_election_block(block()) -> boolean().
-callback verified_signees(block()) -> [libp2p_crypto:pubkey_bin()].

%% arity 4 is for external users of this function who don't need to
%% care about whether or not something is a rescue block.
-callback verify_signatures(Block::binary() | block(),
                            ConsensueMembers::[libp2p_crypto:pubkey_bin()],
                            Signatures::[signature()],
                            Threshold::pos_integer()) ->
    false | {true, [{libp2p_crypto:pubkey_bin(), binary()}]}.
%% arity 5 is for blockchain core's validation routines, which might
%% need to do special processing for rescue blocks.
-callback verify_signatures(Block::binary() | block(),
                            ConsensueMembers::[libp2p_crypto:pubkey_bin()],
                            Signatures::[signature()],
                            Threshold::pos_integer(),
                            Key :: ignore | binary() | [binary()]) ->
    false | {true, [{libp2p_crypto:pubkey_bin(), binary()}], boolean()}.

-include_lib("helium_proto/include/blockchain_block_pb.hrl").

-behavior(blockchain_json).

-export([new_genesis_block/1, new_genesis_block/2,
         prev_hash/1,
         hash_block/1,
         height/1,
         time/1,
         hbbft_round/1,
         is_genesis/1,
         is_election_block/1,
         type/1,
         signatures/1, set_signatures/2,
         verify_signatures/4, verify_signatures/5,
         transactions/1,
         serialize/1,
         deserialize/1,
         is_rescue_block/1,
         to_json/2,
         verified_signees/1
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

-spec hbbft_round(block()) -> non_neg_integer().
hbbft_round(Block) ->
    (type(Block)):hbbft_round(Block).

-spec is_genesis(block()) -> boolean().
is_genesis(Block) ->
    (type(Block)):is_genesis(Block).

-spec is_election_block(block()) -> boolean().
is_election_block(Block) ->
    (type(Block)):is_election_block(Block).

-spec verified_signees(block()) -> [libp2p_crypto:pubkey_bin()].
verified_signees(Block) ->
    (type(Block)):verified_signees(Block).

-spec serialize(block()) -> binary().
serialize(Block) ->
    blockchain_block_pb:encode_msg(wrap_block(Block)).

-spec deserialize(binary()) -> block().
deserialize(Bin) ->
    unwrap_block(blockchain_block_pb:decode_msg(Bin, blockchain_block_pb)).

-spec is_rescue_block(block()) -> boolean().
is_rescue_block(Block) ->
    (type(Block)):is_rescue_block(Block).

%% @private wraps a block with it's version/type wrapper to ready it
%% for serialization.
-spec wrap_block(block()) -> #blockchain_block_pb{}.
wrap_block(Block) ->
    case type(Block) of
        blockchain_block_v1 -> #blockchain_block_pb{block={v1, Block}};
        undefined -> erlang:error({unknown_block, Block})
    end.

%% @private unwrap a protobuf typed block and return the underlying
%% block (which like all good things through the looking glass is also
%% a protobuf based block)
-spec unwrap_block(#blockchain_block_pb{}) -> block().
unwrap_block(#blockchain_block_pb{block={_, Block}}) ->
    Block.

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

verify_signatures(Block, ConsensusMembers, Signatures, Threshold, Key) ->
    (type(Block)):verify_signatures(Block, ConsensusMembers, Signatures, Threshold, Key).

-spec transactions(block()) -> blockchain_txn:txns().
transactions(Block) ->
    (type(Block)):transactions(Block).

-spec to_json(block(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Block, Opts) ->
    (type(Block)):to_json(Block, Opts).

-spec type(block()) -> atom().
type(#blockchain_block_v1_pb{}) ->
    blockchain_block_v1;
type(_) ->
    undefined.
