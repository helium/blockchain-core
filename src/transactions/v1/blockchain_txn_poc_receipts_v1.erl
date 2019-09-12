%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Proof of Coverage Receipts ==
%%%-------------------------------------------------------------------
-module(blockchain_txn_poc_receipts_v1).

-behavior(blockchain_txn).

-include("pb/blockchain_txn_poc_receipts_v1_pb.hrl").
-include("blockchain_vars.hrl").

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
    deltas/1,
    check_path_continuation/1
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type txn_poc_receipts() :: #blockchain_txn_poc_receipts_v1_pb{}.
-type deltas() :: [{libp2p_crypto:pubkey_bin(), {float(), float()}}].

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
                        {error, Reason}=Error ->
                            lager:error("poc_receipts error find_poc, poc_onion_key_hash: ~p, reason: ~p", [POCOnionKeyHash, Reason]),
                            Error;
                        {ok, PoCs} ->
                            Secret = ?MODULE:secret(Txn),
                            case blockchain_ledger_poc_v1:find_valid(PoCs, Challenger, Secret) of
                                {error, _} ->
                                    {error, poc_not_found};
                                {ok, PoC} ->
                                    case blockchain_ledger_v1:find_gateway_info(Challenger, Ledger) of
                                        {error, Reason}=Error ->
                                            lager:error("poc_receipts error find_gateway_info, challenger: ~p, reason: ~p", [Challenger, Reason]),
                                            Error;
                                        {ok, GwInfo} ->
                                            LastChallenge = blockchain_ledger_gateway_v2:last_poc_challenge(GwInfo),
                                            case blockchain:get_block(LastChallenge, Chain) of
                                                {error, Reason}=Error ->
                                                    lager:error("poc_receipts error get_block, last_challenge: ~p, reason: ~p", [LastChallenge, Reason]),
                                                    Error;
                                                {ok, Block1} ->
                                                    PoCInterval = blockchain_utils:challenge_interval(Ledger),
                                                    case LastChallenge + PoCInterval >= Height of
                                                        false ->
                                                            {error, challenge_too_old};
                                                        true ->

                                                            case lists:any(fun(T) ->
                                                                                   blockchain_txn:type(T) == blockchain_txn_poc_request_v1 andalso
                                                                                   blockchain_txn_poc_request_v1:onion_key_hash(T) == POCOnionKeyHash andalso
                                                                                   blockchain_txn_poc_request_v1:block_hash(T) == blockchain_ledger_poc_v1:block_hash(PoC)
                                                                           end,
                                                                           blockchain_block:transactions(Block1)) of
                                                                false ->
                                                                    {error, onion_key_hash_mismatch};
                                                                true ->
                                                                    BlockHash = blockchain_ledger_poc_v1:block_hash(PoC),
                                                                    Entropy = <<Secret/binary, BlockHash/binary, Challenger/binary>>,
                                                                    {ok, OldLedger} = blockchain:ledger_at(blockchain_block:height(Block1), Chain),
                                                                    {Target, Gateways} = blockchain_poc_path:target(Entropy, OldLedger, Challenger),
                                                                    {ok, Path} = blockchain_poc_path:build(Entropy, Target, Gateways, LastChallenge, OldLedger),
                                                                    N = erlang:length(Path),
                                                                    [<<IV:16/integer-unsigned-little, _/binary>> | LayerData] = blockchain_txn_poc_receipts_v1:create_secret_hash(Entropy, N+1),
                                                                    OnionList = lists:zip([libp2p_crypto:bin_to_pubkey(P) || P <- Path], LayerData),
                                                                    {_Onion, Layers} = blockchain_poc_packet:build(libp2p_crypto:keys_from_bin(Secret), IV, OnionList, BlockHash),
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
%% @doc Return a list of {gateway, {alpha, beta}} two tuples after
%% looking at a single poc rx txn. Alpha and Beta are the shaping parameters for
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
-spec deltas(txn_poc_receipts()) -> deltas().
deltas(Txn) ->
    Path = blockchain_txn_poc_receipts_v1:path(Txn),
    Length = length(Path),
    lists:reverse(element(1, lists:foldl(fun({N, Element}, {Acc, true}) ->
                                   Challengee = blockchain_poc_path_element_v1:challengee(Element),
                                   Receipt = blockchain_poc_path_element_v1:receipt(Element),
                                   Witnesses = blockchain_poc_path_element_v1:witnesses(Element),
                                   NextElements = lists:sublist(Path, N+1, Length),
                                   HasContinued = check_path_continuation(NextElements),
                                   {Val, Continue} = assign_alpha_beta(HasContinued, Receipt, Witnesses),
                                   {set_deltas(Challengee, Val, Acc), Continue};
                              (_, Acc) ->
                                   Acc
                           end,
                           {[], true},
                           lists:zip(lists:seq(1, Length), Path)))).

-spec check_path_continuation(Elements :: [blockchain_poc_path_element_v1:poc_element()]) -> boolean().
check_path_continuation(Elements) ->
    lists:any(fun(E) ->
                      blockchain_poc_path_element_v1:receipt(E) /= undefined orelse
                      blockchain_poc_path_element_v1:witnesses(E) /= []
              end,
              Elements).

-spec assign_alpha_beta(HasContinued :: boolean(),
                        Receipt :: undefined | blockchain_poc_receipt_v1:poc_receipt(),
                        Witnesses :: [blockchain_poc_witness_v1:poc_witness()]) -> {{float(), 0 | 1}, boolean()}.
assign_alpha_beta(HasContinued, Receipt, Witnesses) ->
    case {HasContinued, Receipt, Witnesses} of
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
    end.

-spec set_deltas(Challengee :: libp2p_crypto:pubkey_bin(),
                       {A :: float(), B :: 0 | 1},
                       Deltas :: deltas()) -> deltas().
set_deltas(Challengee, {A, B}, Deltas) ->
    [{Challengee, {A, B}} | Deltas].

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
 -spec absorb(txn_poc_receipts(), blockchain:blockchain()) -> ok | {error, any()}.
absorb(Txn, Chain) ->
     LastOnionKeyHash = ?MODULE:onion_key_hash(Txn),
     Challenger = ?MODULE:challenger(Txn),
     Ledger = blockchain:ledger(Chain),

     case blockchain:config(?poc_version, Ledger) of
         {error, not_found} ->
             %% Older poc version, don't add witnesses
             ok;
         {ok, POCVersion} when POCVersion > 1 ->
             %% Insert the witnesses for gateways in the path into ledger
             Path = blockchain_txn_poc_receipts_v1:path(Txn),
             ok = insert_witnesses(Path, Ledger)
     end,

     case blockchain_ledger_v1:delete_poc(LastOnionKeyHash, Challenger, Ledger) of
         {error, _}=Error ->
             Error;
         ok ->
             lists:foldl(fun({Gateway, Delta}, _Acc) ->
                               blockchain_ledger_v1:update_gateway_score(Gateway, Delta, Ledger)
                       end,
                       ok,
                       ?MODULE:deltas(Txn))
     end.

%%--------------------------------------------------------------------
%% @doc
%% Insert witnesses for gateways in the path into the ledger
%% @end
%%--------------------------------------------------------------------
-spec insert_witnesses(blockchain_poc_path_element_v1:path(), blockchain_ledger_v1:ledger()) -> ok.
insert_witnesses(Path, Ledger) ->
    Length = length(Path),
    lists:foreach(fun({N, Element}) ->
                          Challengee = blockchain_poc_path_element_v1:challengee(Element),
                          Witnesses = blockchain_poc_path_element_v1:witnesses(Element),
                          %% TODO check these witnesses have valid RSSI/timestamps
                          WitnessAddresses0 = [ blockchain_poc_witness_v1:gateway(W) || W <- Witnesses ],
                          NextElements = lists:sublist(Path, N+1, Length),
                          WitnessAddresses = case check_path_continuation(NextElements) of
                                                 true ->
                                                     %% the next hop is also a witness for this
                                                     [blockchain_poc_path_element_v1:challengee(hd(NextElements)) | WitnessAddresses0];
                                                 false ->
                                                     WitnessAddresses0
                                             end,
                          blockchain_ledger_v1:add_gateway_witnesses(Challengee, WitnessAddresses, Ledger)
                  end, lists:zip(lists:seq(1, Length), Path)).

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
    <<Hash:2/binary, _/binary>> = Bin,
    create_secret_hash(Bin, X-1, [Hash]);
create_secret_hash(Secret, X, Acc) ->
    Bin = <<Hash:2/binary, _/binary>> = crypto:hash(sha256, Secret),
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
    Result = lists:foldl(
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
                                                                     blockchain_ledger_gateway_v2:location(GWInfo) /= undefined andalso
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
                                       case Receipt == undefined of
                                           true ->
                                               lager:error("Receipt undefined, ExpectedOrigin: ~p, LayerDatum: ~p, Gateway: ~p",
                                                           [Receipt, ExpectedOrigin, LayerDatum, Gateway]);
                                           false ->
                                               lager:error("Origin: ~p, ExpectedOrigin: ~p, Data: ~p, LayerDatum: ~p, ReceiptGateway: ~p, Gateway: ~p",
                                                           [blockchain_poc_receipt_v1:origin(Receipt),
                                                            ExpectedOrigin,
                                                            blockchain_poc_receipt_v1:data(Receipt),
                                                            LayerDatum,
                                                            blockchain_poc_receipt_v1:gateway(Receipt),
                                                            Gateway])
                                       end,
                                       {error, invalid_receipt}
                               end;
                           _ ->
                               {error, receipt_not_in_order}
                       end
               end,
               ok,
               lists:zip3(?MODULE:path(Txn), Path, lists:zip(LayerData, LayerHashes))
              ),
    %% clean up ledger context
    blockchain_ledger_v1:delete_context(OldLedger),
    Result.


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
    Txn1 = {blockchain_txn_poc_receipts_v1_pb,<<"a">>,<<"b">>,<<"c">>,
                                   [{blockchain_poc_path_element_v1_pb,<<"first">>,
                                                                       {blockchain_poc_receipt_v1_pb,<<"d">>,
                                                                                                     123,0,<<"e">>,p2p,
                                                                                                     <<"f">>},
                                                                       [{blockchain_poc_witness_v1_pb,<<"g">>,
                                                                                                      456,-100,
                                                                                                      <<"h">>,
                                                                                                      <<"i">>},
                                                                        {blockchain_poc_witness_v1_pb,<<"j">>,
                                                                                                      789,-114,
                                                                                                      <<"k">>,
                                                                                                      <<"l">>}]},
                                    {blockchain_poc_path_element_v1_pb,<<"second">>,
                                                                       undefined,[]},
                                    {blockchain_poc_path_element_v1_pb,<<"m">>,
                                                                       undefined,[]},
                                    {blockchain_poc_path_element_v1_pb,<<"n">>,
                                                                       undefined,[]},
                                    {blockchain_poc_path_element_v1_pb,<<"i">>,
                                                                       undefined,[]}],
                                   0,
                                   <<"impala">>},
    Deltas1 = deltas(Txn1),
    ?assertEqual(2, length(Deltas1)),
    ?assertEqual({0.9, 0}, proplists:get_value(<<"first">>, Deltas1)),
    ?assertEqual({0, 1}, proplists:get_value(<<"second">>, Deltas1)),

    Txn2 = {blockchain_txn_poc_receipts_v1_pb,<<"foo">>,
                                   <<"bar">>,
                                   <<"baz">>,
                                   [{blockchain_poc_path_element_v1_pb,<<"first">>,
                                                                       {blockchain_poc_receipt_v1_pb,<<"a">>,
                                                                                                     123,0,
                                                                                                     <<1,2,3,4>>,
                                                                                                     p2p,
                                                                                                     <<"b">>},
                                                                       []},
                                    {blockchain_poc_path_element_v1_pb,<<"c">>,
                                                                       undefined,[]},
                                    {blockchain_poc_path_element_v1_pb,<<"d">>,
                                                                       undefined,[]},
                                    {blockchain_poc_path_element_v1_pb,<<"e">>,
                                                                       undefined,[]},
                                    {blockchain_poc_path_element_v1_pb,<<"f">>,
                                                                       undefined,[]}],
                                   0,
                                   <<"g">>},
    Deltas2 = deltas(Txn2),
    ?assertEqual(1, length(Deltas2)),
    ?assertEqual({0, 0}, proplists:get_value(<<"first">>, Deltas2)),
    ok.

duplicate_delta_test() ->
    Txn = {blockchain_txn_poc_receipts_v1_pb,<<"foo">>,
                                   <<"bar">>,
                                   <<"baz">>,
                                   [{blockchain_poc_path_element_v1_pb,<<"first">>,
                                                                       {blockchain_poc_receipt_v1_pb,<<"a">>,
                                                                                                     1559953989978238892,0,<<"§Úi½">>,p2p,
                                                                                                     <<"b">>},
                                                                       []},
                                    {blockchain_poc_path_element_v1_pb,<<"second">>,
                                                                       undefined,
                                                                       [{blockchain_poc_witness_v1_pb,<<"fourth">>,
                                                                                                      1559953991034558678,-100,
                                                                                                      <<>>,
                                                                                                      <<>>},
                                                                        {blockchain_poc_witness_v1_pb,<<"first">>,
                                                                                                      1559953991035078007,-72,
                                                                                                      <<>>,
                                                                                                      <<>>}]},
                                    {blockchain_poc_path_element_v1_pb,<<"third">>,
                                                                       undefined,[]},
                                    {blockchain_poc_path_element_v1_pb,<<"second">>,
                                                                       undefined,
                                                                       [{blockchain_poc_witness_v1_pb,<<"fourth">>,
                                                                                                      1559953992074400943,-100,
                                                                                                      <<>>,
                                                                                                      <<>>},
                                                                        {blockchain_poc_witness_v1_pb,<<"first">>,
                                                                                                      1559953992075156868,-84,
                                                                                                      <<>>,
                                                                                                      <<>>}]}],
                                   0,
                                   <<"gg">>},

    Deltas = deltas(Txn),
    ?assertEqual(4, length(Deltas)),
    SecondDeltas = proplists:get_all_values(<<"second">>, Deltas),
    ?assertEqual(2, length(SecondDeltas)),
    {SecondAlphas, _} = lists:unzip(SecondDeltas),
    ?assert(lists:sum(SecondAlphas) > 1),
    ok.

-endif.
