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

-define(MAX_CHALLENGE_HEIGHT, 30).

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
    {ok, Height} = blockchain:height(Chain),
    POCOnionKeyHash = ?MODULE:onion_key_hash(Txn),
    case libp2p_crypto:verify(EncodedTxn, Signature, PubKey) of
        false ->
            {error, bad_signature};
        true ->
            case ?MODULE:path(Txn) =:= [] of
                true ->
                    {error, empty_path};
                false ->
                    case blockchain_ledger_v1:find_poc(POCOnionKeyHash, Ledger) of
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
                                                    case LastChallenge + ?MAX_CHALLENGE_HEIGHT >= Height of
                                                        false ->
                                                            {error, challenge_too_old};
                                                        true ->

                                                            case lists:any(fun(T) ->
                                                                                   blockchain_txn:type(T) == blockchain_txn_poc_request_v1 andalso
                                                                                   blockchain_txn_poc_request_v1:onion_key_hash(T) == POCOnionKeyHash
                                                                           end,
                                                                           blockchain_block:transactions(Block1)) of
                                                                false ->
                                                                    {error, onion_key_hash_mismatch};
                                                                true ->
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
    element(1, lists:foldl(fun({N, Element}, {Acc, true}) ->
                                   Challengee = blockchain_poc_path_element_v1:challengee(Element),
                                   Receipt = blockchain_poc_path_element_v1:receipt(Element),
                                   Witnesses = blockchain_poc_path_element_v1:witnesses(Element),
                                   NextElements = lists:sublist(Path, N+1, Length),

                                   HasContinued = lists:any(fun(E) ->
                                                                    blockchain_poc_path_element_v1:receipt(E) /= undefined orelse
                                                                    blockchain_poc_path_element_v1:witnesses(E) /= []
                                                            end,
                                                            NextElements),

                                   {Val, Continue} = case {HasContinued, Receipt, Witnesses} of
                                                         {true, undefined, _} ->
                                                             %% path continued, no receipt, don't care about witnesses
                                                             {{0.8, 0}, true};
                                                         {true, Receipt, _} when Receipt /= undefined ->
                                                             %% path continued, receipt, don't care about witnesses
                                                             {{1, 0}, true};
                                                         {false, undefined, Wxs} when length(Wxs) > 0 ->
                                                             %% path broke, no receipt, witnesses
                                                             {{0.9, 0}, true};
                                                         {false, Receipt, []} when Receipt /= undefined ->
                                                             %% path broke, receipt, no witnesses
                                                             case blockchain_poc_receipt_v1:origin(Receipt) of
                                                                 p2p ->
                                                                     %% you really did nothing here other than be online
                                                                     {{0, 0}, false};
                                                                 radio ->
                                                                     %% not enough information to decide who screwed up
                                                                     %% but you did receive a packet over the radio, so you
                                                                     %% get partial credit
                                                                     {{0.2, 0}, false}
                                                             end;
                                                         {false, Receipt, Wxs} when Receipt /= undefined andalso length(Wxs) > 0 ->
                                                             %% path broke, receipt, witnesses
                                                             %% likely the next hop broke the path
                                                             {{0.9, 0}, true};
                                                         {false, _, _} ->
                                                             %% path broke, you killed it
                                                             {{0, 1}, false}
                                                     end,
                                   {maps:put(Challengee, Val, Acc), Continue};
                              (_, Acc) ->
                                   Acc
                           end,
                           {#{}, true},
                           lists:zip(lists:seq(1, Length), Path))).

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
    Bin = <<Hash:4/binary, _/binary>> = crypto:hash(sha256, Secret),
    create_secret_hash(Bin, X-1, [Hash|Acc]).

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

ensure_unique_layer_test() ->
    Secret = crypto:strong_rand_bytes(8),
    Members = create_secret_hash(Secret, 10),
    ?assertEqual(10, erlang:length(Members)),
    ?assertEqual(10, sets:size(sets:from_list(Members))),
    ok.

delta_test() ->
    Txn1 = {blockchain_txn_poc_receipts_v1_pb,<<0,204,147,232,227,
                                     104,48,10,89,191,213,
                                     220,156,247,39,4,246,
                                     40,8,193,185,175,5,
                                     208,126,91,215,111,
                                     141,90,119,71,231>>,
                                   <<0,201,25,199,216,181,227,92,214,12,79,44,45,185,47,174,
                                     28,132,56,66,68,196,208,216,43,149,64,241,168,51,110,51,
                                     35,4,233,140,20,220,149,22,162,28,128,192,133,197,186,
                                     141,149,171,82,115,49,226,124,61,123,82,21,137,219,31,
                                     36,83,35,235,29,186,67,221,143,140,76,206,34,156,219,61,
                                     177,221,173,219,9,175,144,100,164,64,194,145,77,133,221,
                                     1,116,224,26,111>>,
                                   <<52,133,11,165,254,181,31,111,44,254,206,154,135,35,54,
                                     167,14,158,241,251,146,211,37,216,74,5,155,30,83,15,71,
                                     230>>,
                                   [{blockchain_poc_path_element_v1_pb,<<0,9,200,149,134,170,
                                                                         57,53,172,93,186,
                                                                         133,184,248,19,17,
                                                                         200,29,68,34,178,
                                                                         163,18,6,5,108,133,
                                                                         193,227,93,150,213,
                                                                         202>>,
                                                                       {blockchain_poc_receipt_v1_pb,<<0,9,200,149,134,170,57,53,
                                                                                                       172,93,186,133,184,248,19,
                                                                                                       17,200,29,68,34,178,163,
                                                                                                       18,6,5,108,133,193,227,93,
                                                                                                       150,213,202>>,
                                                                                                     1559877129383244295,0,<<"¹iL°">>,p2p,
                                                                                                     <<48,70,2,33,0,217,37,19,89,145,117,223,105,61,224,47,21,
                                                                                                       186,70,121,58,177,136,162,136,233,91,88,154,35,17,146,
                                                                                                       208,231,151,185,42,2,33,0,250,13,239,26,238,115,191,
                                                                                                       227,134,183,158,69,182,218,35,177,105,16,106,248,43,
                                                                                                       194,85,6,14,177,156,240,115,29,26,244>>},
                                                                       [{blockchain_poc_witness_v1_pb,<<0,255,224,164,229,255,
                                                                                                        212,249,14,85,123,64,214,
                                                                                                        144,213,28,119,55,37,79,
                                                                                                        139,35,223,118,73,174,84,
                                                                                                        176,61,72,178,93,225>>,
                                                                                                      1559877129690010396,-100,
                                                                                                      <<190,98,143,107,210,247,58,35,243,145,253,75,154,98,137,
                                                                                                        169,27,21,124,182,91,204,142,209,231,48,36,75,189,31,10,
                                                                                                        12>>,
                                                                                                      <<48,70,2,33,0,154,20,188,182,95,219,195,134,109,228,153,
                                                                                                        163,223,255,181,209,99,12,226,211,190,33,206,56,248,
                                                                                                        110,226,188,45,222,199,41,2,33,0,191,209,131,38,158,80,
                                                                                                        186,0,83,89,195,66,33,253,73,202,166,94,45,182,34,83,
                                                                                                        212,139,208,20,247,18,129,166,115,75>>},
                                                                        {blockchain_poc_witness_v1_pb,<<0,71,168,192,199,234,41,
                                                                                                        88,199,70,13,204,140,162,
                                                                                                        81,219,121,139,105,175,32,
                                                                                                        54,19,222,205,207,14,196,
                                                                                                        85,23,98,138,48>>,
                                                                                                      1559877129741830137,-114,
                                                                                                      <<190,98,143,107,210,247,58,35,243,145,253,75,154,98,137,
                                                                                                        169,27,21,124,182,91,204,142,209,231,48,36,75,189,31,10,
                                                                                                        12>>,
                                                                                                      <<48,69,2,32,48,117,203,55,100,63,136,137,160,101,153,
                                                                                                        46,150,254,86,89,126,133,192,19,100,43,198,102,153,
                                                                                                        51,76,128,69,15,22,253,2,33,0,174,129,136,118,217,49,
                                                                                                        213,54,216,12,232,123,64,136,117,202,196,176,192,60,
                                                                                                        215,107,236,94,144,132,117,25,124,2,162,217>>}]},
                                    {blockchain_poc_path_element_v1_pb,<<0,6,61,143,237,45,92,
                                                                         89,215,194,214,159,
                                                                         148,159,15,149,171,5,
                                                                         60,123,56,210,115,
                                                                         148,137,112,214,203,
                                                                         19,167,151,59,231>>,
                                                                       undefined,[]},
                                    {blockchain_poc_path_element_v1_pb,<<0,255,224,164,229,
                                                                         255,212,249,14,85,
                                                                         123,64,214,144,213,
                                                                         28,119,55,37,79,139,
                                                                         35,223,118,73,174,84,
                                                                         176,61,72,178,93,225>>,
                                                                       undefined,[]},
                                    {blockchain_poc_path_element_v1_pb,<<0,161,84,140,52,184,
                                                                         64,197,30,38,228,69,
                                                                         212,138,78,94,150,86,
                                                                         14,196,135,172,132,
                                                                         179,44,100,163,148,
                                                                         133,216,102,26,217>>,
                                                                       undefined,[]},
                                    {blockchain_poc_path_element_v1_pb,<<0,60,26,0,36,43,184,
                                                                         65,10,240,179,213,18,
                                                                         186,225,110,243,229,
                                                                         8,29,80,227,21,159,
                                                                         135,199,63,62,152,
                                                                         142,182,164,132>>,
                                                                       undefined,[]}],
                                   0,
                                   <<48,68,2,32,66,214,80,14,125,229,121,33,206,184,193,120,
                                     107,30,128,108,247,154,105,39,139,243,203,9,23,233,211,
                                     68,107,87,32,190,2,32,75,48,155,125,70,136,232,20,249,
                                     82,88,105,243,224,203,233,105,64,89,252,27,229,9,67,82,
                                     169,155,141,39,240,199,170>>},
    Deltas1 = deltas(Txn1),
    ?assertEqual(2, maps:size(Deltas1)),
    ?assertEqual({0.9, 0}, maps:get(<<0,9,200,149,134,170,
                                      57,53,172,93,186,
                                      133,184,248,19,17,
                                      200,29,68,34,178,
                                      163,18,6,5,108,133,
                                      193,227,93,150,213,
                                      202>>, Deltas1)),
    ?assertEqual({0, 1}, maps:get(<<0,6,61,143,237,45,92,
                                    89,215,194,214,159,
                                    148,159,15,149,171,5,
                                    60,123,56,210,115,
                                    148,137,112,214,203,
                                    19,167,151,59,231>>, Deltas1)),

    Txn2 = {blockchain_txn_poc_receipts_v1_pb,<<0,42,86,148,184,241,
                                     136,107,91,166,143,
                                     11,99,141,21,167,53,
                                     124,79,25,248,11,244,
                                     137,254,67,9,89,5,
                                     196,60,10,89>>,
                                   <<0,208,36,169,112,236,54,243,225,201,49,126,37,15,242,
                                     130,209,171,150,172,135,225,29,242,94,106,120,200,46,22,
                                     112,252,36,4,84,26,36,136,145,187,81,221,103,43,17,210,
                                     209,115,66,147,123,120,135,11,124,47,186,110,69,9,124,
                                     212,20,132,69,129,122,84,85,77,157,109,117,199,36,253,
                                     39,186,219,30,19,69,13,88,254,209,98,9,234,253,190,117,
                                     5,74,117,226,154,132>>,
                                   <<133,151,51,218,162,244,31,61,160,5,25,162,51,203,225,
                                     163,29,74,49,97,95,96,137,160,115,224,82,218,68,167,140,
                                     214>>,
                                   [{blockchain_poc_path_element_v1_pb,<<0,9,200,149,134,170,
                                                                         57,53,172,93,186,
                                                                         133,184,248,19,17,
                                                                         200,29,68,34,178,
                                                                         163,18,6,5,108,133,
                                                                         193,227,93,150,213,
                                                                         202>>,
                                                                       {blockchain_poc_receipt_v1_pb,<<0,9,200,149,134,170,57,53,
                                                                                                       172,93,186,133,184,248,19,
                                                                                                       17,200,29,68,34,178,163,
                                                                                                       18,6,5,108,133,193,227,93,
                                                                                                       150,213,202>>,
                                                                                                     1559873483553381242,0,
                                                                                                     <<159,19,84,196>>,
                                                                                                     p2p,
                                                                                                     <<48,70,2,33,0,153,154,250,73,189,245,26,255,142,249,74,
                                                                                                       125,218,56,15,139,11,160,98,148,183,243,8,122,14,28,
                                                                                                       252,174,146,211,193,12,2,33,0,148,146,164,218,43,82,
                                                                                                       227,203,163,135,150,97,219,34,120,111,79,146,111,178,
                                                                                                       73,162,213,110,20,194,84,105,144,149,69,27>>},
                                                                       []},
                                    {blockchain_poc_path_element_v1_pb,<<0,6,61,143,237,45,92,
                                                                         89,215,194,214,159,
                                                                         148,159,15,149,171,5,
                                                                         60,123,56,210,115,
                                                                         148,137,112,214,203,
                                                                         19,167,151,59,231>>,
                                                                       undefined,[]},
                                    {blockchain_poc_path_element_v1_pb,<<0,255,224,164,229,
                                                                         255,212,249,14,85,
                                                                         123,64,214,144,213,
                                                                         28,119,55,37,79,139,
                                                                         35,223,118,73,174,84,
                                                                         176,61,72,178,93,225>>,
                                                                       undefined,[]},
                                    {blockchain_poc_path_element_v1_pb,<<0,161,84,140,52,184,
                                                                         64,197,30,38,228,69,
                                                                         212,138,78,94,150,86,
                                                                         14,196,135,172,132,
                                                                         179,44,100,163,148,
                                                                         133,216,102,26,217>>,
                                                                       undefined,[]},
                                    {blockchain_poc_path_element_v1_pb,<<0,60,26,0,36,43,184,
                                                                         65,10,240,179,213,18,
                                                                         186,225,110,243,229,
                                                                         8,29,80,227,21,159,
                                                                         135,199,63,62,152,
                                                                         142,182,164,132>>,
                                                                       undefined,[]}],
                                   0,
                                   <<48,69,2,33,0,144,159,179,22,243,45,80,229,164,228,175,
                                     145,254,135,219,217,43,218,169,42,27,11,34,221,200,181,
                                     186,138,169,42,68,125,2,32,14,97,141,132,20,18,29,252,
                                     33,160,230,129,117,193,154,19,250,66,205,193,31,14,103,
                                     241,245,177,232,84,88,88,174,33>>},
    Deltas2 = deltas(Txn2),
    ?assertEqual(1, maps:size(Deltas2)),
    ?assertEqual({0, 0}, maps:get(<<0,9,200,149,134,170,57,53,172,93,186,133,184,248,19,17,200,29,68,34,178,163,18,6,5,108,133,193,227,93,150,213,202>>, Deltas2)),
    ok.


-endif.
