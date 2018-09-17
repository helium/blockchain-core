%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Block ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_block).

-export([
    new/4
    ,height/1
    ,transactions/1
    ,signature/1
    ,prev_hash/1
    ,remove_signature/1
    ,sign_block/2
    ,new_genesis_block/1
    ,is_genesis/1
    ,is_block/1
    ,hash_block/1
    ,verify_signature/4
    ,payment_transactions/1
    ,add_gateway_transactions/1
    ,assert_location_transactions/1
    ,save/3, load/2
    ,serialize/2
    ,deserialize/2
]).

-include("blockchain.hrl").

-record(block, {
    prev_hash :: blockchain_block:hash()
    ,height = 0 :: non_neg_integer()
    ,transactions = [] :: blockchain_transaction:transactions()
    ,signature :: binary()
}).

-type block() :: #block{}.
-type hash() :: <<_:256>>. %% SHA256 digest

-export_type([block/0, hash/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(hash(), non_neg_integer(), blockchain_transaction:transactions(), binary()) -> block().
new(PrevHash, Height, Transactions, Signature) ->
    #block{
        prev_hash=PrevHash
        ,height=Height
        ,transactions=Transactions
        ,signature=Signature
    }.

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
-spec transactions(block()) -> blockchain_transaction:transactions().
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
-spec prev_hash(block()) -> hash().
prev_hash(Block) ->
    Block#block.prev_hash.

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
-spec sign_block(block(), binary()) -> block().
sign_block(Block, Signature) ->
    Block#block{signature=Signature}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new_genesis_block(blockchain_transaction:transactions()) -> block().
new_genesis_block(Transactions) ->
    #block{prev_hash = <<0:256>>, height=1, transactions=Transactions, signature = <<>>}.

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
verify_signature(#block{}=Block, ConsensusMembers, Signature, Threshold) ->
    BinBlock = erlang:term_to_binary(?MODULE:remove_signature(Block)),
    verify_signature(BinBlock, ConsensusMembers, Signature, Threshold);
verify_signature(Artifact, ConsensusMembers, Signature, Threshold) ->
    ValidSignatures = lists:foldl(
        fun({Addr, Sig}, Acc) ->
            case
                lists:member(Addr, ConsensusMembers)
                andalso (not lists:keymember(Addr, 1, Acc))
                andalso libp2p_crypto:verify(Artifact, Sig, libp2p_crypto:address_to_pubkey(Addr))
            of
                true -> [{Addr, Sig} | Acc];
                false -> Acc
            end
        end
        ,[]
        ,erlang:binary_to_term(Signature)
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
-spec payment_transactions(block()) -> [blockchain_transaction:payment_txn()].
payment_transactions(Block) ->
    lists:filter(fun(Txn) ->blockchain_transaction:is_payment_txn(Txn) end
                 ,?MODULE:transactions(Block)).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec add_gateway_transactions(block()) -> [blockchain_transaction:add_gateway_txn()].
add_gateway_transactions(Block) ->
    lists:filter(fun(Txn) -> blockchain_transaction:is_add_gateway_txn(Txn) end
                 ,?MODULE:transactions(Block)).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec assert_location_transactions(block()) -> [blockchain_transaction:assert_location_transactions()].
assert_location_transactions(Block) ->
    lists:filter(fun(Txn) -> blockchain_transaction:is_assert_location_txn(Txn) end
                 ,?MODULE:transactions(Block)).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec save(hash(), block(), string()) -> ok | {error, any()}.
save(Hash, Block, BaseDir) ->
    Dir = filename:join(BaseDir, ?BLOCKS_DIR),
    BinBlock = ?MODULE:serialize(blockchain_util:serial_version(BaseDir), Block),
    File = filename:join(Dir, blockchain_util:serialize_hash(Hash)),
    blockchain_util:atomic_save(File, BinBlock).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec load(hash(), string()) -> block() | undefined.
load(Hash, BaseDir) ->
    Dir = filename:join(BaseDir, ?BLOCKS_DIR),
    File = filename:join(Dir, blockchain_util:serialize_hash(Hash)),
    case file:read_file(File) of
        {error, _Reason} ->
            undefined;
        {ok, Binary} ->
            ?MODULE:deserialize(blockchain_util:serial_version(BaseDir), Binary)
    end.
%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec serialize(blockchain_util:serial_version(), block()) -> binary().
serialize(_Version, Block) ->
    erlang:term_to_binary(Block).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec deserialize(blockchain_util:serial_version(), binary()) -> block().
deserialize(_Version, Bin) ->
    erlang:binary_to_term(Bin).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
