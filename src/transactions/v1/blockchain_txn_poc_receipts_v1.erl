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
    is_valid/2,
    absorb/2,
    create_secret_hash/2,
    connections/1,
    deltas/1
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
          blockchain_poc_path_element_v1:path()) -> txn_poc_receipts().
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
-spec path(txn_poc_receipts()) -> blockchain_poc_path_element_v1:path().
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
-spec is_valid(txn_poc_receipts(), blockchain:blockchain()) -> ok | {error, any()}.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
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
                                    case blockchain_ledger_v1:find_gateway_info(Challenger, Ledger) of
                                        {error, _Reason}=Error ->
                                            Error;
                                        {ok, GwInfo} ->
                                            LastChallenge = blockchain_ledger_gateway_v1:last_poc_challenge(GwInfo),
                                            case blockchain:get_block(LastChallenge, Chain) of
                                                {error, _}=Error ->
                                                    Error;
                                                {ok, Block1} ->
                                                    BlockHash = blockchain_block:hash_block(Block1),
                                                    Entropy = <<Secret/binary, BlockHash/binary, Challenger/binary>>,
                                                    {ok, OldLedger} = blockchain:ledger_at(blockchain_block:height(Block1), Chain),
                                                    {Target, Gateways} = blockchain_poc_path:target(Entropy, OldLedger, Challenger),
                                                    {ok, Path} = blockchain_poc_path:build(Entropy, Target, Gateways),
                                                    N = erlang:length(Path),
                                                    [<<IV:16/integer-unsigned-little, _/binary>> | LayerData] = blockchain_txn_poc_receipts_v1:create_secret_hash(Entropy, N+1),
                                                    OnionList = lists:zip([libp2p_crypto:bin_to_pubkey(P) || P <- Path], LayerData),
                                                    {_Onion, Layers} = blockchain_poc_packet:build(libp2p_crypto:keys_from_bin(Secret), IV, OnionList),
                                                    %% no witness will exist with the first layer hash
                                                    [_|LayerHashes] = [crypto:hash(sha256, L) || L <- Layers],
                                                    validate(Txn, Path, LayerData, LayerHashes, OldLedger)
                                            end
                                    end
                            end
                    end
            end
    end.

-spec connections(Txn :: txn_poc_receipts()) -> [{Transmitter :: libp2p_crypto:pubkey_bin(), Receiver :: libp2p_crypto:pubkey_bin(),
                                                  RSSI :: integer(), Timestamp :: non_neg_integer()}].
connections(Txn) ->
    Paths = ?MODULE:path(Txn),
    TaggedPaths = lists:zip(lists:seq(1, length(Paths)), Paths),
    lists:foldl(fun({PathPos, PathElement}, Acc) ->
                        case blockchain_poc_path_element_v1:receipt(PathElement) of
                            undefined -> [];
                            Receipt ->
                                case blockchain_poc_receipt_v1:origin(Receipt) of
                                    radio ->
                                        {_, PrevElem} = lists:keyfind(PathPos - 1, 1, TaggedPaths),
                                        [{blockchain_poc_path_element_v1:challengee(PrevElem), blockchain_poc_receipt_v1:gateway(Receipt),
                                          blockchain_poc_receipt_v1:signal(Receipt), blockchain_poc_receipt_v1:timestamp(Receipt)}];
                                    _ ->
                                        []
                                end
                        end ++
                        lists:map(fun(Witness) ->
                                          {blockchain_poc_path_element_v1:challengee(PathElement),
                                          blockchain_poc_witness_v1:gateway(Witness),
                                          blockchain_poc_witness_v1:signal(Witness),
                                          blockchain_poc_witness_v1:timestamp(Witness)}
                                  end, blockchain_poc_path_element_v1:witnesses(PathElement)) ++ Acc
                end, [], TaggedPaths).

%%--------------------------------------------------------------------
%% @doc Return a gateway => {alpha, beta} map after looking at a single poc rx txn.
%% Alpha and Beta are the shaping parameters for
%% the beta distribution curve.
%%
%% An increment in alpha implies that we have gained more confidence in a hotspot
%% being active and has succesffully either been a witness or sent a receipt. The
%% actual increment values are debatable and have been put here for testing, although
%% they do seem to behave well.
%%
%% An increment in beta implies we gain confidence in a hotspot not doing it's job correctly.
%% This should be rare and only happen when we are certain that a particular hotspot in the path
%% has not done it's job.
%%
%% @end
%%--------------------------------------------------------------------
-spec deltas(txn_poc_receipts()) -> #{libp2p_crypto:pubkey_bin() => {float(), float()}}.
deltas(Txn) ->
    Path = blockchain_txn_poc_receipts_v1:path(Txn),
    Length = length(Path),
    lists:foldl(fun({N, Element}, Acc) ->
                        Challengee = blockchain_poc_path_element_v1:challengee(Element),
                        Receipt = blockchain_poc_path_element_v1:receipt(Element),
                        Witnesses = blockchain_poc_path_element_v1:witnesses(Element),
                        NextElements = lists:sublist(Path, N+1, Length),

                        HasContinued = lists:any(fun(E) ->
                                                         blockchain_poc_path_element_v1:receipt(E) /= undefined orelse
                                                         blockchain_poc_path_element_v1:witnesses(E) /= []
                                                 end,
                                                 NextElements),

                        Val = case {HasContinued, Receipt, Witnesses} of
                                  {true, undefined, _} ->
                                      %% path continued, no receipt, don't care about witnesses
                                      {0.8, 0};
                                  {true, Receipt, _} when Receipt /= undefined ->
                                      %% path continued, receipt, don't care about witnesses
                                      {1, 0};
                                  {false, undefined, Wxs} when length(Wxs) > 0 ->
                                      %% path broke, no receipt, witnesses
                                      {0.9, 0};
                                  {false, Receipt, []} when Receipt /= undefined ->
                                      %% path broke, receipt, no witnesses
                                      %% not enough information
                                      {0.6, 0.4};
                                  {false, Receipt, Wxs} when Receipt /= undefined andalso length(Wxs) > 0 ->
                                      %% path broke, receipt, witnesses
                                      {0.9, 0};
                                  {false, _, _} ->
                                      %% path broke, you fucked up
                                      {0, 1}
                              end,
                        maps:put(Challengee, Val, Acc)
                end,
                #{},
                lists:zip(lists:seq(1, Length), Path)).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
 -spec absorb(txn_poc_receipts(), blockchain:blockchain()) -> ok | {error, any()}.
absorb(Txn, Chain) ->
     LastOnionKeyHash = ?MODULE:onion_key_hash(Txn),
     Challenger = ?MODULE:challenger(Txn),
     Ledger = blockchain:ledger(Chain),
     case blockchain_ledger_v1:delete_poc(LastOnionKeyHash, Challenger, Ledger) of
         {error, _}=Error ->
             Error;
         ok ->
             maps:fold(fun(Gateway, Delta, _Acc) ->
                               blockchain_ledger_v1:update_gateway_score(Gateway, Delta, Ledger)
                       end,
                       ok,
                       ?MODULE:deltas(Txn))
     end.

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

%% @doc Validate the proof of coverage receipt path.
%%
%% The proof of coverage receipt path consists of a list of `poc path elements',
%% each of which corresponds to a layer of the onion-encrypted PoC packet. This
%% path element records who the layer was intended for, if we got a receipt from
%% them and any other peers that witnessed this packet but were unable to decrypt
%% it. This function verifies that all the receipts and witnesses appear in the
%% expected order, and satisfy all the validity checks.
%%
%% Because the first packet is delivered over p2p, it cannot have witnesses and
%% the receipt must indicate the origin was `p2p'. All subsequent packets must
%% have the receipt origin as `radio'. The final layer has the packet composed
%% entirely of padding, so there cannot be a valid receipt, but there can be
%% witnesses (as evidence that the final recipient transmitted it).
-spec validate(txn_poc_receipts(), list(),
               [binary(), ...], [binary(), ...], blockchain_ledger_v1:ledger()) -> ok | {error, atom()}.
validate(Txn, Path, LayerData, LayerHashes, OldLedger) ->
    lists:foldl(
        fun(_, {error, _} = Error) ->
            Error;
        ({Elem, Gateway, {LayerDatum, LayerHash}}, _Acc) ->
            case blockchain_poc_path_element_v1:challengee(Elem) == Gateway of
                true ->
                    IsFirst = Elem == hd(?MODULE:path(Txn)),
                    Receipt = blockchain_poc_path_element_v1:receipt(Elem),
                    ExpectedOrigin = case IsFirst of
                        true -> p2p;
                        false -> radio
                    end,
                    %% check the receipt
                    case
                        Receipt == undefined orelse
                        (blockchain_poc_receipt_v1:is_valid(Receipt) andalso
                         blockchain_poc_receipt_v1:gateway(Receipt) == Gateway andalso
                         blockchain_poc_receipt_v1:data(Receipt) == LayerDatum andalso
                         blockchain_poc_receipt_v1:origin(Receipt) == ExpectedOrigin)
                    of
                        true ->
                            %% ok the receipt looks good, check the witnesses
                            Witnesses = blockchain_poc_path_element_v1:witnesses(Elem),
                            case erlang:length(Witnesses) > 5 of
                                true ->
                                    {error, too_many_witnesses};
                                false ->
                                    %% all the witnesses should have the right LayerHash
                                    %% and be valid
                                    case
                                        lists:all(
                                          fun(Witness) ->
                                                  %% the witnesses should have an asserted location
                                                  %% at the point when the request was mined!
                                                  WitnessGateway = blockchain_poc_witness_v1:gateway(Witness),
                                                  case blockchain_ledger_v1:find_gateway_info(WitnessGateway, OldLedger) of
                                                      {error, _} ->
                                                          false;
                                                      {ok, _} when Gateway == WitnessGateway ->
                                                          false;
                                                      {ok, GWInfo} ->
                                                          blockchain_ledger_gateway_v1:location(GWInfo) /= undefined andalso
                                                          blockchain_poc_witness_v1:is_valid(Witness) andalso
                                                          blockchain_poc_witness_v1:packet_hash(Witness) == LayerHash
                                                  end
                                          end,
                                          Witnesses
                                         )
                                    of
                                        true -> ok;
                                        false -> {error, invalid_witness}
                                    end
                            end;
                        false ->
                              {error, invalid_receipt}
                    end;
                _ ->
                    {error, receipt_not_in_order}
            end
        end,
        ok,
        lists:zip3(?MODULE:path(Txn), Path, lists:zip(LayerData, LayerHashes))
    ).


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
