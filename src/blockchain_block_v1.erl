%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Block ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_block_v1).

-behavior(blockchain_block).

-export([
    new/1,
    prev_hash/1,
    height/1,
    transactions/1,
    signatures/1,
    time/1,
    hbbft_round/1,
    set_signatures/2,
    new_genesis_block/1,
    is_genesis/1,
    is/1,
    hash_block/1,
    verify_signatures/4,
    payment_transactions/1,
    coinbase_transactions/1,
    add_gateway_transactions/1,
    assert_location_transactions/1,
    poc_request_transactions/1
]).

-include("blockchain.hrl").
-include("pb/blockchain_block_v1_pb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type block() :: #blockchain_block_v1_pb{}.
-type block_map() :: #{prev_hash => binary(),
                       height => non_neg_integer(),
                       time => non_neg_integer(),
                       hbbft_round => non_neg_integer(),
                       transactions => blockchain_transactions:transactions(),
                       signatures => [blockchain_block:signature()]
                      }.

-export_type([block/0, block_map/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(block_map())-> block().
new(#{prev_hash := PrevHash,
      height := Height,
      time := Time,
      hbbft_round := HBBFTRound,
      transactions := Transactions,
      signatures := Signatures}) ->
    #blockchain_block_v1_pb{
       prev_hash=PrevHash,
       height=Height,
       transactions=[wrap_transaction(T) || T <- Transactions],
       signatures=[wrap_signature(S) || S <- Signatures],
       time=Time,
       hbbft_round=HBBFTRound
    }.

%% Since the proto file for a block includes the definitions of the
%% underlying protobufs for each transaction we break encapsulation
%% here and do no tuse the txn modules themselves for now.
-spec wrap_transaction(blockchain_transaction:transaction()) -> #blockchain_txn_v1_pb{}.
wrap_transaction(#blockchain_txn_assert_location_v1_pb{}=Txn) ->
    #blockchain_txn_v1_pb{txn={assert_location, Txn}};
wrap_transaction(#blockchain_txn_payment_v1_pb{}=Txn) ->
    #blockchain_txn_v1_pb{txn={payment, Txn}};
wrap_transaction(#blockchain_txn_create_htlc_v1_pb{}=Txn) ->
    #blockchain_txn_v1_pb{txn={create_htlc, Txn}};
wrap_transaction(#blockchain_txn_redeem_htlc_v1_pb{}=Txn) ->
    #blockchain_txn_v1_pb{txn={redeem_htlc, Txn}};
wrap_transaction(#blockchain_txn_add_gateway_v1_pb{}=Txn) ->
    #blockchain_txn_v1_pb{txn={add_gateway, Txn}};
wrap_transaction(#blockchain_txn_coinbase_v1_pb{}=Txn) ->
    #blockchain_txn_v1_pb{txn={coinbase, Txn}};
wrap_transaction(#blockchain_txn_consensus_group_v1_pb{}=Txn) ->
    #blockchain_txn_v1_pb{txn={consensus_group, Txn}};
wrap_transaction(#blockchain_txn_poc_request_v1_pb{}=Txn) ->
    #blockchain_txn_v1_pb{txn={poc_request, Txn}};
wrap_transaction(#blockchain_txn_poc_receipts_v1_pb{}=Txn) ->
    #blockchain_txn_v1_pb{txn={poc_receipts, Txn}};
wrap_transaction(#blockchain_txn_gen_gateway_v1_pb{}=Txn) ->
    #blockchain_txn_v1_pb{txn={gen_gateway, Txn}}.

unwrap_transaction(#blockchain_txn_v1_pb{txn={_, Txn}}) ->
    Txn.

-spec wrap_signature(blockchain_block:signature()) -> #blockchain_signature_v1_pb{}.
wrap_signature({Signer, Signature}) ->
    #blockchain_signature_v1_pb{signer=Signer, signature=Signature}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec prev_hash(block()) -> blockchain_block:hash().
prev_hash(Block) ->
    Block#blockchain_block_v1_pb.prev_hash.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec height(block()) -> non_neg_integer().
height(Block) ->
    Block#blockchain_block_v1_pb.height.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec time(block()) -> non_neg_integer().
time(Block) ->
    Block#blockchain_block_v1_pb.time.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec transactions(block()) -> blockchain_transactions:transactions().
transactions(Block) ->
    [unwrap_transaction(T) || T <- Block#blockchain_block_v1_pb.transactions].

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec signatures(block()) -> [blockchain_block:signature()].
signatures(Block) ->
    [{Signer, Sig} || #blockchain_signature_v1_pb{signer=Signer, signature=Sig}
                          <- Block#blockchain_block_v1_pb.signatures].

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec hbbft_round(block()) -> non_neg_integer().
hbbft_round(Block) ->
    Block#blockchain_block_v1_pb.hbbft_round.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec set_signatures(block(), [bockchain_block:signature()]) -> block().
set_signatures(Block, Signatures) ->
    Block#blockchain_block_v1_pb{signatures=[wrap_signature(S) || S <- Signatures]}.


%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new_genesis_block(blockchain_transactions:transactions()) -> block().
new_genesis_block(Transactions) ->
    ?MODULE:new(#{prev_hash => <<0:256>>,
                  height => 1,
                  time => 0,
                  transactions => Transactions,
                  signatures => [],
                  hbbft_round => 0}).

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
-spec is(block()) -> boolean().
is(Block) ->
    erlang:is_record(Block, blockchain_block_v1_pb).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec hash_block(block()) -> blockchain_block:hash().
hash_block(Block) ->
    EncodedBlock = blockchain_block:serialize(?MODULE:set_signatures(Block, [])),
    crypto:hash(sha256, EncodedBlock).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec verify_signatures(Block::binary() | block(),
                        ConsensueMembers::[libp2p_crypto:pubkey_bin()],
                        Signatures::[blockchain_block:signature()],
                        Threshold::pos_integer()) -> false | {true, [{libp2p_crypto:pubkey_bin(), binary()}]}.
verify_signatures(#blockchain_block_v1_pb{}=Block, ConsensusMembers, Signatures, Threshold) ->
    EncodedBlock = blockchain_block:serialize(?MODULE:set_signatures(Block, [])),
    verify_signatures(EncodedBlock, ConsensusMembers, Signatures, Threshold);
verify_signatures(Artifact, ConsensusMembers, Signatures, Threshold) ->
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
        end, [], Signatures),
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
    lists:filter(fun blockchain_txn_coinbase_v1:is/1, ?MODULE:transactions(Block)).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec add_gateway_transactions(block()) -> [blockchain_txn_add_gateway_v1:txn_add_gateway()].
add_gateway_transactions(Block) ->
    lists:filter(fun blockchain_txn_add_gateway_v1:is/1, ?MODULE:transactions(Block)).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec assert_location_transactions(block()) -> [blockchain_txn_assert_location_v1:txn_assert_location()].
assert_location_transactions(Block) ->
    lists:filter(fun blockchain_txn_assert_location_v1:is/1, ?MODULE:transactions(Block)).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec poc_request_transactions(block()) -> [blockchain_txn_poc_request_v1:txn_poc_request()].
poc_request_transactions(Block) ->
    lists:filter(fun blockchain_txn_poc_request_v1:is/1, ?MODULE:transactions(Block)).

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_merge(Overrides) ->
    new(maps:merge(
          #{ prev_hash => <<>>,
             height => 1,
             transactions => [],
             signatures => [],
             hbbft_round => 0,
             time => 0
           },
          Overrides)).

new_test() ->
    Block = #blockchain_block_v1_pb{
               prev_hash= <<>>,
               height=1,
               transactions=[],
               signatures= [],
               hbbft_round = 0,
               time = 0
              },
    ?assertEqual(Block, new_merge(#{})).

prev_hash_test() ->
    Hash = <<"hash">>,
    Block = new_merge(#{prev_hash => Hash}),
    ?assertEqual(Hash, prev_hash(Block)).

height_test() ->
    Height = 1,
    Block = new_merge(#{height => Height}),
    ?assertEqual(Height, height(Block)).

transactions_test() ->
    Txs = [blockchain_txn_add_gateway_v1:new(1, 1)],
    Block = new_merge(#{transactions => Txs}),
    ?assertEqual(Txs, transactions(Block)).

signatures_test() ->
    Sigs = [{<<"addr">>, <<"signature">>}],
    Block = new_merge(#{signatures => Sigs}),
    ?assertEqual(Sigs, signatures(Block)).

hbbft_round_test() ->
    HBBFTRound = 1,
    Block = new_merge(#{ hbbft_round => HBBFTRound}),
    ?assertEqual(HBBFTRound, hbbft_round(Block)).

time_test() ->
    Time = 1,
    Block = new_merge(#{ time => Time}),
    ?assertEqual(Time, time(Block)).

set_signatures_test() ->
    Sigs = [{<<"addr">>, <<"signature">>}],
    Block = new_merge(#{ signatures => Sigs}),
    ?assertEqual([], signatures(set_signatures(Block, []))).

new_genesis_test() ->
    Txs = [blockchain_txn_add_gateway_v1:new(1, 1),
           blockchain_txn_add_gateway_v1:new(2, 2),
           blockchain_txn_add_gateway_v1:new(3, 3)],
    Block = new_genesis_block(Txs),
    ?assertEqual(<<0:256>>, prev_hash(Block)),
    ?assertEqual(1, height(Block)),
    ?assertEqual(Txs, transactions(Block)),
    ?assertEqual([], signatures(Block)),
    ?assertEqual(0, time(Block)),
    ?assertEqual(0, hbbft_round(Block)).

is_genesis_test() ->
    ?assertEqual(true, is_genesis(new_genesis_block([]))),
    ?assertEqual(false, is_genesis(new_merge(#{prev_hash => <<>>}))).

is_test() ->
    ?assertEqual(true, is(new_genesis_block([]))),
    ?assertEqual(false, is(#{})).

verify_signature_test() ->
    Keys = generate_keys(10),
    [{Payer, {_, PayerPrivKey, _}}, {Recipient, _}|_] = Keys,
    Tx = blockchain_txn_payment_v1:new(Payer, Recipient, 2500, 10, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    SignedTx = blockchain_txn_payment_v1:sign(Tx, SigFun),
    Block0 = new_merge(#{ transactions => [SignedTx]}),
    EncodedBlock = blockchain_block:serialize(set_signatures(Block0, [])),
    Signatures =
        lists:foldl(
            fun({A, {_, _, F}}, Acc) ->
                Sig = F(EncodedBlock),
                [{A, Sig}|Acc]
            end,
            [],
            Keys
        ),
    Block1 = ?MODULE:set_signatures(Block0, Signatures),
    ConsensusMembers = [Addr || {Addr, _} <- Keys],
    ?assertMatch({true, _}, verify_signatures(Block1, ConsensusMembers, Signatures, 7)),
    ?assertMatch(false, verify_signatures(Block1, ConsensusMembers, Signatures, 20)),
    ?assertMatch(false, verify_signatures(Block1, [], Signatures, 7)),
    ok.

generate_keys(N) ->
    lists:foldl(
        fun(_, Acc) ->
            #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
            SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
            [{libp2p_crypto:pubkey_to_bin(PubKey), {PubKey, PrivKey, SigFun}}|Acc]
        end,
        [],
        lists:seq(1, N)
    ).

-endif.
