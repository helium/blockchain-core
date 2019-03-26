%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Proof of Coverage Receipts ==
%%%-------------------------------------------------------------------
-module(blockchain_txn_poc_receipts_v1).

-behavior(blockchain_txn).

-include("pb/blockchain_txn_poc_receipts_v1_pb.hrl").

-export([
    new/4,
    hash/1,
    onion_key_hash/1,
    challenger/1,
    secret/1,
    path/1,
    fee/1,
    signature/1,
    sign/2,
    is_valid/3,
    absorb/3,
    create_secret_hash/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type txn_poc_receipts() :: #blockchain_txn_poc_receipts_v1_pb{}.

-export_type([txn_poc_receipts/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(libp2p_crypto:pubkey_bin(), binary(), binary(),
          blockchain_poc_element_v1:path()) -> txn_poc_receipts().
new(Challenger, Secret, OnionKeyHash, Path) ->
    #blockchain_txn_poc_receipts_v1_pb{
        challenger=Challenger,
        secret=Secret,
        onion_key_hash=OnionKeyHash,
        path=Path,
        fee=0,
        signature = <<>>
    }.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec hash(txn_poc_receipts()) -> blockchain_txn:hash().
hash(Txn) ->
    BaseTxn = Txn#blockchain_txn_poc_receipts_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_poc_receipts_v1_pb:encode_msg(BaseTxn),
    crypto:hash(sha256, EncodedTxn).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec challenger(txn_poc_receipts()) -> libp2p_crypto:pubkey_bin().
challenger(Txn) ->
    Txn#blockchain_txn_poc_receipts_v1_pb.challenger.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec secret(txn_poc_receipts()) -> binary().
secret(Txn) ->
    Txn#blockchain_txn_poc_receipts_v1_pb.secret.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec onion_key_hash(txn_poc_receipts()) -> binary().
onion_key_hash(Txn) ->
    Txn#blockchain_txn_poc_receipts_v1_pb.onion_key_hash.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec path(txn_poc_receipts()) -> blockchain_poc_element_v1:path().
path(Txn) ->
    Txn#blockchain_txn_poc_receipts_v1_pb.path.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec fee(txn_poc_receipts()) -> 0.
fee(_Txn) ->
    0.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec signature(txn_poc_receipts()) -> binary().
signature(Txn) ->
    Txn#blockchain_txn_poc_receipts_v1_pb.signature.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec sign(txn_poc_receipts(), libp2p_crypto:sig_fun()) -> txn_poc_receipts().
sign(Txn, SigFun) ->
    BaseTxn = Txn#blockchain_txn_poc_receipts_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_poc_receipts_v1_pb:encode_msg(BaseTxn),
    Txn#blockchain_txn_poc_receipts_v1_pb{signature=SigFun(EncodedTxn)}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_valid(txn_poc_receipts(),
               blockchain_block:block(),
               blockchain_ledger_v1:ledger()) -> ok | {error, any()}.
is_valid(Txn, _Block, Ledger) ->
    Challenger = ?MODULE:challenger(Txn),
    Signature = ?MODULE:signature(Txn),
    PubKey = libp2p_crypto:bin_to_pubkey(Challenger),
    BaseTxn = Txn#blockchain_txn_poc_receipts_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_poc_receipts_v1_pb:encode_msg(BaseTxn),
    case libp2p_crypto:verify(EncodedTxn, Signature, PubKey) of
        false ->
            {error, bad_signature};
        true ->
            case ?MODULE:path(Txn) =:= [] of
                true ->
                    {error, empty_path};
                false ->
                    case blockchain_ledger_v1:find_poc(?MODULE:onion_key_hash(Txn), Ledger) of
                        {error, _}=Error ->
                            Error;
                        {ok, PoCs} ->
                            Secret = ?MODULE:secret(Txn),
                            case blockchain_ledger_poc_v1:find_valid(PoCs, Challenger, Secret) of
                                {error, _} ->
                                    {error, poc_not_found};
                                {ok, _PoC} ->
                                    Blockchain = blockchain_worker:blockchain(),
                                    case blockchain_ledger_v1:find_gateway_info(Challenger, Ledger) of
                                        {error, _Reason}=Error ->
                                            Error;
                                        {ok, GwInfo} ->
                                            LastChallenge = blockchain_ledger_gateway_v1:last_poc_challenge(GwInfo),
                                            case blockchain:get_block(LastChallenge, Blockchain) of
                                                {error, _}=Error ->
                                                    Error;
                                                {ok, Block1} ->
                                                    BlockHash = blockchain_block:hash_block(Block1),
                                                    Entropy = <<Secret/binary, BlockHash/binary, Challenger/binary>>,
                                                    {ok, OldLedger} = blockchain:ledger_at(blockchain_block:height(Block1), Blockchain),
                                                    {Target, Gateways} = blockchain_poc_path:target(Entropy, OldLedger, Challenger),
                                                    {ok, Path} = blockchain_poc_path:build(Target, Gateways),
                                                    N = erlang:length(Path),
                                                    [<<IV:16/integer-unsigned-little, _/binary>> | Hashes] = blockchain_txn_poc_receipts_v1:create_secret_hash(Entropy, N+1),
                                                    OnionList = lists:zip([libp2p_crypto:bin_to_pubkey(P) || P <- Path], Hashes),
                                                    {_Onion, Layers} = blockchain_poc_packet:build(libp2p_crypto:keys_from_bin(Secret), IV, OnionList),
                                                    LayerHashes = [ crypto:hash(sha256, L) || L <- Layers ],
                                                    %% verify each recipt and witness
                                                    Witnesses = lists:foldl(
                                                        fun(Elem, Acc) ->
                                                            blockchain_poc_path_element_v1:witnesses(Elem) ++ Acc
                                                        end,
                                                        [],
                                                        ?MODULE:path(Txn)
                                                    ),
                                                    ValidWitnesses = lists:map(
                                                        fun(Witness) ->
                                                            blockchain_poc_witness_v1:is_valid(Witness) andalso
                                                            lists:member(blockchain_poc_witness_v1:packet_hash(Witness), LayerHashes)
                                                        end,
                                                        Witnesses
                                                    ),
                                                    Receipts = lists:foldl(
                                                        fun(undefined, Acc)->
                                                            Acc;
                                                        (Elem, Acc) ->
                                                            [blockchain_poc_path_element_v1:receipt(Elem)|Acc]
                                                        end,
                                                        [],
                                                        ?MODULE:path(Txn)
                                                    ),
                                                    ValidReceipts = lists:map(
                                                        fun(Receipt) ->
                                                            GW = blockchain_poc_receipt_v1:gateway(Receipt),
                                                            {_, Data} = lists:keyfind(libp2p_crypto:bin_to_pubkey(GW), 1, OnionList),
                                                            blockchain_poc_receipt_v1:is_valid(Receipt) andalso
                                                            blockchain_poc_receipt_v1:data(Receipt) == Data andalso
                                                            blockchain_poc_receipt_v1:origin(Receipt) == expected_origin(libp2p_crypto:bin_to_pubkey(GW), OnionList)
                                                        end,
                                                        Receipts
                                                    ),
                                                    case lists:all(fun(T) -> T == true end, ValidWitnesses ++ ValidReceipts) of
                                                        true -> ok;
                                                        _ -> {error, invalid_receipt_or_witness}
                                                    end
                                            end
                                    end
                            end
                    end
            end
    end.


%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb(txn_poc_receipts(),
             blockchain_block:block(),
             blockchain_ledger_v1:ledger()) -> ok | {error, any()}.
absorb(_Txn, _Block, _Ledger) ->
    % TODO: Update score here
    ok.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec create_secret_hash(binary(), non_neg_integer()) -> [binary()].
create_secret_hash(Secret, X) when X > 0 ->
    create_secret_hash(Secret, X, []).

-spec create_secret_hash(binary(), non_neg_integer(), [binary()]) -> [binary()].
create_secret_hash(_Secret, 0, Acc) ->
    Acc;
create_secret_hash(Secret, X, []) ->
    Bin = crypto:hash(sha256, Secret),
    <<Hash:4/binary, _/binary>> = Bin,
    create_secret_hash(Bin, X-1, [Hash]);
create_secret_hash(Secret, X, Acc) ->
    <<Hash:4/binary, _/binary>> = crypto:hash(sha256, Secret),
    create_secret_hash(Secret, X-1, [Hash|Acc]).


expected_origin(GW, [{GW, _}|_]) ->
    p2p;
expected_origin(_, _) ->
    radio.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Tx = #blockchain_txn_poc_receipts_v1_pb{
        challenger = <<"challenger">>,
        secret = <<"secret">>,
        onion_key_hash = <<"onion">>,
        path=[],
        fee = 0,
        signature = <<>>
    },
    ?assertEqual(Tx, new(<<"challenger">>,  <<"secret">>, <<"onion">>, [])).

onion_key_hash_test() ->
    Tx = new(<<"challenger">>,  <<"secret">>, <<"onion">>, []),
    ?assertEqual(<<"onion">>, onion_key_hash(Tx)).

challenger_test() ->
    Tx = new(<<"challenger">>,  <<"secret">>, <<"onion">>, []),
    ?assertEqual(<<"challenger">>, challenger(Tx)).

secret_test() ->
    Tx = new(<<"challenger">>,  <<"secret">>, <<"onion">>, []),
    ?assertEqual(<<"secret">>, secret(Tx)).

path_test() ->
    Tx = new(<<"challenger">>,  <<"secret">>, <<"onion">>, []),
    ?assertEqual([], path(Tx)).

fee_test() ->
    Tx = new(<<"challenger">>,  <<"secret">>, <<"onion">>, []),
    ?assertEqual(0, fee(Tx)).

signature_test() ->
    Tx = new(<<"challenger">>,  <<"secret">>, <<"onion">>, []),
    ?assertEqual(<<>>, signature(Tx)).

sign_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Challenger = libp2p_crypto:pubkey_to_bin(PubKey),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx0 = new(Challenger,  <<"secret">>, <<"onion">>, []),
    Tx1 = sign(Tx0, SigFun),
    Sig = signature(Tx1),
    EncodedTx1 = blockchain_txn_poc_receipts_v1_pb:encode_msg(Tx1#blockchain_txn_poc_receipts_v1_pb{signature = <<>>}),
    ?assert(libp2p_crypto:verify(EncodedTx1, Sig, PubKey)).

create_secret_hash_test() ->
    Secret = crypto:strong_rand_bytes(8),
    Members = create_secret_hash(Secret, 10),
    ?assertEqual(10, erlang:length(Members)),

    Members2 = create_secret_hash(Secret, 10),
    ?assertEqual(10, erlang:length(Members2)),

    lists:foreach(
        fun(M) ->
            ?assert(lists:member(M, Members2))
        end,
        Members
    ),
    ok.

-endif.
