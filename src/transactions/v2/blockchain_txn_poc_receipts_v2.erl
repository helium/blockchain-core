%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Proof of Coverage Receipts V2 ==
%%%-------------------------------------------------------------------
-module(blockchain_txn_poc_receipts_v2).

-behavior(blockchain_txn).

-include_lib("helium_proto/include/blockchain_txn_poc_receipts_v2_pb.hrl").
-include("blockchain_vars.hrl").
-include("blockchain_utils.hrl").

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
         deltas/2,
         hex_poc_id/1,
         print/1
        ]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type txn_poc_receipts() :: #blockchain_txn_poc_receipts_v2_pb{}.
-type deltas() :: [{libp2p_crypto:pubkey_bin(), {float(), float()}}].

-type connections() :: [{Transmitter :: libp2p_crypto:pubkey_bin(),
                         Receiver :: libp2p_crypto:pubkey_bin(),
                         RSSI :: integer(), RxTime :: non_neg_integer()}].

-export_type([txn_poc_receipts/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(libp2p_crypto:pubkey_bin(), binary(), binary(),
          blockchain_poc_path_element_v2:poc_path()) -> txn_poc_receipts().
new(Challenger, Secret, OnionKeyHash, Path) ->
    #blockchain_txn_poc_receipts_v2_pb{
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
    BaseTxn = Txn#blockchain_txn_poc_receipts_v2_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_poc_receipts_v2_pb:encode_msg(BaseTxn),
    crypto:hash(sha256, EncodedTxn).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec challenger(txn_poc_receipts()) -> libp2p_crypto:pubkey_bin().
challenger(Txn) ->
    Txn#blockchain_txn_poc_receipts_v2_pb.challenger.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec secret(txn_poc_receipts()) -> binary().
secret(Txn) ->
    Txn#blockchain_txn_poc_receipts_v2_pb.secret.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec onion_key_hash(txn_poc_receipts()) -> binary().
onion_key_hash(Txn) ->
    Txn#blockchain_txn_poc_receipts_v2_pb.onion_key_hash.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec path(txn_poc_receipts()) -> blockchain_poc_path_element_v2:poc_path().
path(Txn) ->
    Txn#blockchain_txn_poc_receipts_v2_pb.path.

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
    Txn#blockchain_txn_poc_receipts_v2_pb.signature.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec sign(txn_poc_receipts(), libp2p_crypto:sig_fun()) -> txn_poc_receipts().
sign(Txn, SigFun) ->
    BaseTxn = Txn#blockchain_txn_poc_receipts_v2_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_poc_receipts_v2_pb:encode_msg(BaseTxn),
    Txn#blockchain_txn_poc_receipts_v2_pb{signature=SigFun(EncodedTxn)}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_valid(txn_poc_receipts(), blockchain:blockchain()) -> ok | {error, any()}.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    HexPOCID = ?MODULE:hex_poc_id(Txn),
    Challenger = ?MODULE:challenger(Txn),
    Path = ?MODULE:path(Txn),
    POCOnionKeyHash = ?MODULE:onion_key_hash(Txn),
    Secret = ?MODULE:secret(Txn),
    BaseTxn = Txn#blockchain_txn_poc_receipts_v2_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_poc_receipts_v2_pb:encode_msg(BaseTxn),
    Signature = ?MODULE:signature(Txn),
    PubKey = libp2p_crypto:bin_to_pubkey(Challenger),

    case check_signature(EncodedTxn, Signature, PubKey, HexPOCID) of
        {error, _}=E1 -> E1;
        {ok, true} ->
            case check_empty_path(Path, HexPOCID) of
                {error, _}=E2 -> E2;
                {ok, false} ->
                    case check_find_poc(POCOnionKeyHash, Ledger, HexPOCID) of
                        {error, _}=E3 -> E3;
                        {ok, PoCs} ->
                            case check_valid_poc(PoCs, Challenger, Secret, HexPOCID) of
                                {error, _}=E4 -> E4;
                                {ok, PoC} ->
                                    case check_gw_info(Challenger, Ledger, HexPOCID) of
                                        {error, _}=E5 -> E5;
                                        {ok, GwInfo} ->
                                            case check_last_challenge_block(GwInfo, Chain, Ledger, HexPOCID) of
                                                {error, _}=E6 -> E6;
                                                {ok, LastChallengeBlock} ->
                                                    check_path(LastChallengeBlock, PoC, Challenger, Secret, POCOnionKeyHash, Txn, Chain)
                                            end
                                    end
                            end
                    end
            end
    end.

-spec absorb(txn_poc_receipts(), blockchain:blockchain()) -> ok | {error, any()}.
absorb(Txn, Chain) ->
    LastOnionKeyHash = ?MODULE:onion_key_hash(Txn),
    Challenger = ?MODULE:challenger(Txn),
    Secret = ?MODULE:secret(Txn),
    Ledger = blockchain:ledger(Chain),
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
    HexPOCID = ?MODULE:hex_poc_id(Txn),

    try
        %% get these to make sure we're not replaying.
        {ok, PoCs} = blockchain_ledger_v1:find_poc(LastOnionKeyHash, Ledger),
        {ok, _PoC} = blockchain_ledger_poc_v2:find_valid(PoCs, Challenger, Secret),
        {ok, GwInfo} = blockchain_ledger_v1:find_gateway_info(Challenger, Ledger),
        LastChallenge = blockchain_ledger_gateway_v2:last_poc_challenge(GwInfo),
        PoCInterval = blockchain_utils:challenge_interval(Ledger),
        case LastChallenge + PoCInterval >= Height of
            false ->
                {error, challenge_too_old};
            true ->
                %% Find upper and lower time bounds for this poc txn and use those to clamp
                %% witness timestamps being inserted in the ledger
                case get_lower_and_upper_bounds(Secret, LastOnionKeyHash, Challenger, Ledger, Chain) of
                    {error, _}=E ->
                        E;
                    {ok, {Lower, Upper}} ->
                        %% Insert the witnesses for gateways in the path into ledger
                        Path = blockchain_txn_poc_receipts_v2:path(Txn),
                        ok = insert_witnesses(Path, Lower, Upper, LastOnionKeyHash, Ledger),

                        case blockchain_ledger_v1:delete_poc(LastOnionKeyHash, Challenger, Ledger) of
                            {error, _}=Error1 ->
                                Error1;
                            ok ->
                                lists:foldl(fun({Gateway, Delta}, _Acc) ->
                                                    blockchain_ledger_v1:update_gateway_score(Gateway, Delta, Ledger)
                                            end,
                                            ok,
                                            ?MODULE:deltas(Txn, Ledger))
                        end
                end
        end
    catch What:Why:Stacktrace ->
              lager:error([{poc_id, HexPOCID}], "poc receipt calculation failed: ~p ~p ~p", [What, Why, Stacktrace]),
              {error, state_missing}
    end.

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

-spec connections(Txn :: txn_poc_receipts()) -> connections().
connections(Txn) ->
    Paths = ?MODULE:path(Txn),
    TaggedPaths = lists:zip(lists:seq(1, length(Paths)), Paths),
    lists:foldl(fun({PathPos, PathElement}, Acc) ->
                        case blockchain_poc_path_element_v2:receipt(PathElement) of
                            undefined -> [];
                            Receipt ->
                                case blockchain_poc_receipt_v2:origin(Receipt) of
                                    radio ->
                                        {_, PrevElem} = lists:keyfind(PathPos - 1, 1, TaggedPaths),
                                        [{blockchain_poc_path_element_v2:challengee(PrevElem), blockchain_poc_receipt_v2:gateway(Receipt),
                                          blockchain_poc_receipt_v2:signal(Receipt), blockchain_poc_receipt_v2:rx_time(Receipt)}];
                                    _ ->
                                        []
                                end
                        end ++
                        lists:map(fun(Witness) ->
                                          {blockchain_poc_path_element_v2:challengee(PathElement),
                                           blockchain_poc_witness_v2:gateway(Witness),
                                           blockchain_poc_witness_v2:signal(Witness),
                                           blockchain_poc_witness_v2:rx_time(Witness)}
                                  end, blockchain_poc_path_element_v2:witnesses(PathElement)) ++ Acc
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
-spec deltas(Txn :: txn_poc_receipts(),
             Ledger :: blockchain_ledger_v1:ledger()) -> deltas().
deltas(Txn, Ledger) ->
    Path = blockchain_txn_poc_receipts_v2:path(Txn),
    Length = length(Path),
    lists:reverse(element(1, lists:foldl(fun({ElementPos, Element}, {Acc, true}) ->
                                                 Challengee = blockchain_poc_path_element_v2:challengee(Element),
                                                 NextElements = lists:sublist(Path, ElementPos+1, Length),
                                                 HasContinued = blockchain_poc_path_element_v2:check_path_continuation(NextElements),
                                                 {Val, Continue} = calculate_alpha_beta(HasContinued, Element, Ledger),
                                                 {set_deltas(Challengee, Val, Acc), Continue};
                                            (_, Acc) ->
                                                 Acc
                                         end,
                                         {[], true},
                                         lists:zip(lists:seq(1, Length), Path)))).

-spec hex_poc_id(txn_poc_receipts()) -> string().
hex_poc_id(Txn) ->
    #{secret := _OnionPrivKey, public := OnionPubKey} = libp2p_crypto:keys_from_bin(?MODULE:secret(Txn)),
    <<POCID:10/binary, _/binary>> = libp2p_crypto:pubkey_to_bin(OnionPubKey),
    blockchain_utils:bin_to_hex(POCID).

print(#blockchain_txn_poc_receipts_v2_pb{
         challenger=Challenger,
         onion_key_hash=OnionKeyHash,
         path=Path
        }=Txn) ->
    io_lib:format("type=poc_receipts_v2 hash=~p challenger=~p onion=~p path:\n\t~s",
                  [?TO_B58(?MODULE:hash(Txn)),
                   ?TO_ANIMAL_NAME(Challenger),
                   ?TO_B58(OnionKeyHash),
                   print_path(Path)]).

print_path(Path) ->
    string:join(lists:map(fun(Element) ->
                                  blockchain_poc_path_element_v2:print(Element)
                          end,
                          Path), "\n\t").

%% ------------------------------------------------------------------
%% Internal Functions
%% ------------------------------------------------------------------
-spec calculate_alpha_beta(HasContinued :: boolean(),
                           Element :: blockchain_poc_path_element_v2:poc_element(),
                           Ledger :: blockchain_ledger_v1:ledger()) -> {{float(), 0 | 1}, boolean()}.
calculate_alpha_beta(HasContinued, Element, Ledger) ->
    Receipt = blockchain_poc_path_element_v2:receipt(Element),
    Witnesses = blockchain_poc_path_element_v2:witnesses(Element),
    case {HasContinued, Receipt, Witnesses} of
        {true, undefined, _} ->
            %% path continued, no receipt, don't care about witnesses
            {{0.8, 0}, true};
        {true, Receipt, _} when Receipt /= undefined ->
            %% path continued, receipt, don't care about witnesses
            {{1, 0}, true};
        {false, undefined, Wxs} when length(Wxs) > 0 ->
            %% path broke, no receipt, witnesses
            {blockchain_poc_path_element_v2:calculate_witness_quality(Element, Ledger), true};
        {false, Receipt, []} when Receipt /= undefined ->
            %% path broke, receipt, no witnesses
            case blockchain_poc_receipt_v2:origin(Receipt) of
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
            {blockchain_poc_path_element_v2:calculate_witness_quality(Element, Ledger), true};
        {false, _, _} ->
            %% path broke, you killed it
            {{0, 1}, false}
    end.

-spec get_lower_and_upper_bounds(Secret :: binary(),
                                 OnionKeyHash :: binary(),
                                 Challenger :: libp2p_crypto:pubkey_bin(),
                                 Ledger :: blockchain_ledger_v1:ledger(),
                                 Chain :: blockchain:blockchain()) -> {error, any()} | {ok, {non_neg_integer(), non_neg_integer()}}.
get_lower_and_upper_bounds(Secret, OnionKeyHash, Challenger, Ledger, Chain) ->
    case blockchain_ledger_v1:find_poc(OnionKeyHash, Ledger) of
        {error, Reason}=Error0 ->
            lager:error("poc_receipts error find_poc, poc_onion_key_hash: ~p, reason: ~p", [OnionKeyHash, Reason]),
            Error0;
        {ok, PoCs} ->
            case blockchain_ledger_poc_v2:find_valid(PoCs, Challenger, Secret) of
                {error, _}=Error1 ->
                    Error1;
                {ok, _PoC} ->
                    case blockchain_ledger_v1:find_gateway_info(Challenger, Ledger) of
                        {error, Reason}=Error2 ->
                            lager:error("poc_receipts error find_gateway_info, challenger: ~p, reason: ~p", [Challenger, Reason]),
                            Error2;
                        {ok, GwInfo} ->
                            LastChallenge = blockchain_ledger_gateway_v2:last_poc_challenge(GwInfo),
                            case blockchain:get_block(LastChallenge, Chain) of
                                {error, Reason}=Error3 ->
                                    lager:error("poc_receipts error get_block, last_challenge: ~p, reason: ~p", [LastChallenge, Reason]),
                                    Error3;
                                {ok, Block1} ->
                                    {ok, HH} = blockchain_ledger_v1:current_height(Ledger),
                                    case blockchain:get_block(HH, Chain) of
                                        {error, _}=Error4 ->
                                            Error4;
                                        {ok, B} ->
                                            %% Convert lower and upper bounds to be in nanoseconds
                                            LowerBound = blockchain_block:time(Block1) * 1000000000,
                                            UpperBound = blockchain_block:time(B) * 1000000000,
                                            {ok, {LowerBound, UpperBound}}
                                    end
                            end
                    end
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Insert witnesses for gateways in the path into the ledger
%% @end
%%--------------------------------------------------------------------
-spec insert_witnesses(Path :: blockchain_poc_path_element_v2:poc_path(),
                       LowerTimeBound :: non_neg_integer(),
                       UpperTimeBound :: non_neg_integer(),
                       LastOnionKeyHash :: binary(),
                       Ledger :: blockchain_ledger_v1:ledger()) -> ok.
insert_witnesses(Path, LowerTimeBound, UpperTimeBound, LastOnionKeyHash, Ledger) ->
    %% Get configured allowed total path witnesses
    TotalAllowedWitnesses = blockchain:config(?total_allowed_witnesses, Ledger),
    %% Get total eligible witnesses in path
    TotalEligibleWitnesses = blockchain_poc_path_element_v2:total_eligible_witnesses(Path, Ledger),

    case TotalEligibleWitnesses =< TotalAllowedWitnesses of
        true ->
            %% no need to squish, allow adding to ledger
            insert_witnesses_(Path, LowerTimeBound, UpperTimeBound, LastOnionKeyHash, Ledger);
        false ->
            ToSquish = TotalAllowedWitnesses - TotalEligibleWitnesses,
            NewPath = blockchain_poc_path_element_v2:squish(Path, LastOnionKeyHash, ToSquish),
            insert_witnesses_(NewPath, LowerTimeBound, UpperTimeBound, LastOnionKeyHash, Ledger)
    end.

-spec insert_witnesses_(Path :: blockchain_poc_path_element_v2:poc_path(),
                        LowerTimeBound :: non_neg_integer(),
                        UpperTimeBound :: non_neg_integer(),
                        LastOnionKeyHash :: binary(),
                        Ledger :: blockchain_ledger_v1:ledger()) -> ok.
insert_witnesses_(Path, LowerTimeBound, UpperTimeBound, _LastOnionKeyHash, Ledger) ->
    Length = length(Path),
    lists:foreach(fun({N, Element}) ->
                          Challengee = blockchain_poc_path_element_v2:challengee(Element),
                          Witnesses = blockchain_poc_path_element_v2:witnesses(Element),
                          %% TODO check these witnesses have valid RSSI
                          WitnessInfo0 = lists:foldl(fun(Witness, Acc) ->
                                                             TS = case blockchain_poc_witness_v2:rx_time(Witness) of
                                                                      T when T < LowerTimeBound ->
                                                                          LowerTimeBound;
                                                                      T when T > UpperTimeBound ->
                                                                          UpperTimeBound;
                                                                      T ->
                                                                          T
                                                                  end,
                                                             [{blockchain_poc_witness_v2:signal(Witness), TS, blockchain_poc_witness_v2:gateway(Witness)} | Acc]
                                                     end,
                                                     [],
                                                     Witnesses),
                          NextElements = lists:sublist(Path, N+1, Length),
                          WitnessInfo = case blockchain_poc_path_element_v2:check_path_continuation(NextElements) of
                                            true ->
                                                %% the next hop is also a witness for this
                                                NextHopElement = hd(NextElements),
                                                NextHopAddr = blockchain_poc_path_element_v2:challengee(NextHopElement),
                                                case blockchain_poc_path_element_v2:receipt(NextHopElement) of
                                                    undefined ->
                                                        %% There is no receipt from the next hop
                                                        %% We clamp to LowerTimeBound as best-effort
                                                        [{undefined, LowerTimeBound, NextHopAddr} | WitnessInfo0];
                                                    NextHopReceipt ->
                                                        [{blockchain_poc_receipt_v2:signal(NextHopReceipt),
                                                          blockchain_poc_receipt_v2:rx_time(NextHopReceipt),
                                                          NextHopAddr} | WitnessInfo0]
                                                end;
                                            false ->
                                                WitnessInfo0
                                        end,
                          blockchain_ledger_v1:add_gateway_witnesses(Challengee, WitnessInfo, Ledger)
                  end, lists:zip(lists:seq(1, Length), Path)).

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
-spec validate(Txn :: txn_poc_receipts(),
               Path :: blockchain_poc_path_v4:path(),
               LayerData :: [binary(), ...],
               LayerHashes :: [binary(), ...],
               OldLedger :: blockchain_ledger_v1:ledger()) -> ok | {error, atom()}.
validate(Txn, Path, LayerData, LayerHashes, OldLedger) ->
    TxnPath = ?MODULE:path(Txn),
    TxnPathLength = length(TxnPath),
    RebuiltPathLength = length(Path),
    ZippedLayers = lists:zip(LayerData, LayerHashes),
    ZippedLayersLength = length(ZippedLayers),
    HexPOCID = ?MODULE:hex_poc_id(Txn),
    lager:info([{poc_id, HexPOCID}], "starting poc receipt validation..."),

    case TxnPathLength == RebuiltPathLength of
        false ->
            HumanTxnPath = [element(2, erl_angry_purple_tiger:animal_name(libp2p_crypto:bin_to_b58(blockchain_poc_path_element_v2:challengee(E)))) || E <- TxnPath],
            HumanRebuiltPath = [element(2, erl_angry_purple_tiger:animal_name(libp2p_crypto:bin_to_b58(A))) || A <- Path],
            lager:error([{poc_id, HexPOCID}], "TxnPathLength: ~p, RebuiltPathLength: ~p", [TxnPathLength, RebuiltPathLength]),
            lager:error([{poc_id, HexPOCID}], "TxnPath: ~p", [HumanTxnPath]),
            lager:error([{poc_id, HexPOCID}], "RebuiltPath: ~p", [HumanRebuiltPath]),
            {error, path_length_mismatch};
        true ->
            %% Now check whether layers are of equal length
            case TxnPathLength == ZippedLayersLength of
                false ->
                    lager:error([{poc_id, HexPOCID}], "TxnPathLength: ~p, ZippedLayersLength: ~p", [TxnPathLength, ZippedLayersLength]),
                    {error, zip_layer_length_mismatch};
                true ->
                    Result = lists:foldl(
                               fun(_, {error, _} = Error) ->
                                       Error;
                                  ({Elem, Gateway, {LayerDatum, LayerHash}}, _Acc) ->
                                       case blockchain_poc_path_element_v2:challengee(Elem) == Gateway of
                                           true ->
                                               IsFirst = Elem == hd(?MODULE:path(Txn)),
                                               Receipt = blockchain_poc_path_element_v2:receipt(Elem),
                                               ExpectedOrigin = case IsFirst of
                                                                    true -> p2p;
                                                                    false -> radio
                                                                end,
                                               %% check the receipt
                                               case
                                                   Receipt == undefined orelse
                                                   (blockchain_poc_receipt_v2:is_valid(Receipt) andalso
                                                    blockchain_poc_receipt_v2:gateway(Receipt) == Gateway andalso
                                                    blockchain_poc_receipt_v2:data(Receipt) == LayerDatum andalso
                                                    blockchain_poc_receipt_v2:origin(Receipt) == ExpectedOrigin)
                                               of
                                                   true ->
                                                       %% ok the receipt looks good, check the witnesses
                                                       Witnesses = blockchain_poc_path_element_v2:witnesses(Elem),
                                                       case erlang:length(Witnesses) > 5 of
                                                           true ->
                                                               {error, too_many_witnesses};
                                                           false ->
                                                               %% check there are no duplicates in witnesses list
                                                               WitnessGateways = [blockchain_poc_witness_v2:gateway(W) || W <- Witnesses],
                                                               case length(WitnessGateways) == length(lists:usort(WitnessGateways)) of
                                                                   false ->
                                                                       {error, duplicate_witnesses};
                                                                   true ->
                                                                       check_witness_layerhash(Witnesses, Gateway, LayerHash, OldLedger)
                                                               end
                                                       end;
                                                   false ->
                                                       case Receipt == undefined of
                                                           true ->
                                                               lager:error([{poc_id, HexPOCID}],
                                                                           "Receipt undefined, ExpectedOrigin: ~p, LayerDatum: ~p, Gateway: ~p",
                                                                           [Receipt, ExpectedOrigin, LayerDatum, Gateway]);
                                                           false ->
                                                               lager:error([{poc_id, HexPOCID}],
                                                                           "Origin: ~p, ExpectedOrigin: ~p, Data: ~p, LayerDatum: ~p, ReceiptGateway: ~p, Gateway: ~p",
                                                                           [blockchain_poc_receipt_v2:origin(Receipt),
                                                                            ExpectedOrigin,
                                                                            blockchain_poc_receipt_v2:data(Receipt),
                                                                            LayerDatum,
                                                                            blockchain_poc_receipt_v2:gateway(Receipt),
                                                                            Gateway])
                                                       end,
                                                       {error, invalid_receipt}
                                               end;
                                           _ ->
                                               lager:error([{poc_id, HexPOCID}], "receipt not in order"),
                                               {error, receipt_not_in_order}
                                       end
                               end,
                               ok,
                               lists:zip3(TxnPath, Path, ZippedLayers)
                              ),
                    %% clean up ledger context
                    blockchain_ledger_v1:delete_context(OldLedger),
                    Result

            end
    end.

check_witness_layerhash(Witnesses, Gateway, LayerHash, OldLedger) ->
    %% all the witnesses should have the right LayerHash
    %% and be valid
    case
        lists:all(
          fun(Witness) ->
                  %% the witnesses should have an asserted location
                  %% at the point when the request was mined!
                  WitnessGateway = blockchain_poc_witness_v2:gateway(Witness),
                  case blockchain_ledger_v1:find_gateway_info(WitnessGateway, OldLedger) of
                      {error, _} ->
                          false;
                      {ok, _} when Gateway == WitnessGateway ->
                          false;
                      {ok, GWInfo} ->
                          blockchain_ledger_gateway_v2:location(GWInfo) /= undefined andalso
                          blockchain_poc_witness_v2:is_valid(Witness) andalso
                          blockchain_poc_witness_v2:packet_hash(Witness) == LayerHash
                  end
          end,
          Witnesses
         )
    of
        true -> ok;
        false -> {error, invalid_witness}
    end.

-spec check_signature(EncodedTxn :: binary(),
                      Signature :: binary(),
                      PubKey :: libp2p_crypto:pubkey(),
                      HexPOCID :: string()) -> {error, bad_signature} | {ok, true}.
check_signature(EncodedTxn, Signature, PubKey, HexPOCID) ->
    case libp2p_crypto:verify(EncodedTxn, Signature, PubKey) of
        false ->
            lager:error([{poc_id, HexPOCID}], "poc_receipts error bad_signature"),
            {error, bad_signature};
        true ->
            {ok, true}
    end.

-spec check_empty_path(Path :: blockchain_poc_path_element_v2:poc_path(),
                       HexPOCID :: string()) -> {error, empty_path} | {ok, false}.
check_empty_path(Path, HexPOCID) ->
    case Path =:= [] of
        true ->
            lager:error([{poc_id, HexPOCID}], "poc_receipts error empty_path"),
            {error, empty_path};
        false ->
            {ok, false}
    end.

-spec check_find_poc(POCOnionKeyHash :: binary(),
                     Ledger :: blockchain_ledger_v1:ledger(),
                     HexPOCID :: string()) -> {error, any()} | {ok, blockchain_ledger_poc_v2:pocs()}.
check_find_poc(POCOnionKeyHash, Ledger, HexPOCID) ->
    case blockchain_ledger_v1:find_poc(POCOnionKeyHash, Ledger) of
        {error, Reason}=Error ->
            lager:error([{poc_id, HexPOCID}],
                        "poc_receipts error find_poc, poc_onion_key_hash: ~p, reason: ~p",
                        [POCOnionKeyHash, Reason]),
            Error;
        {ok, _PoCs}=Res ->
            Res
    end.

-spec check_valid_poc(PoCs :: blockchain_ledger_poc_v2:pocs(),
                      Challenger :: libp2p_crypto:pubkey_bin(),
                      Secret :: binary(),
                      HexPOCID :: string()) -> {error, poc_not_found} | {ok, blockchain_ledger_poc_v2:poc()}.
check_valid_poc(PoCs, Challenger, Secret, HexPOCID) ->
    case blockchain_ledger_poc_v2:find_valid(PoCs, Challenger, Secret) of
        {error, _} ->
            lager:error([{poc_id, HexPOCID}],
                        "poc_receipts error invalid_poc, challenger: ~p, pocs: ~p",
                        [Challenger, PoCs]),
            {error, poc_not_found};
        {ok, _PoC}=Res ->
            Res
    end.

-spec check_gw_info(Challenger :: libp2p_crypto:pubkey_bin(),
                    Ledger :: blockchain_ledger_v1:ledger(),
                    HexPOCID :: string()) -> {error, any()} | {ok, blockchain_ledger_gateway_v2:gateway()}.
check_gw_info(Challenger, Ledger, HexPOCID) ->
    case blockchain_ledger_v1:find_gateway_info(Challenger, Ledger) of
        {error, Reason}=Error ->
            lager:error([{poc_id, HexPOCID}],
                        "poc_receipts error find_gateway_info, challenger: ~p, reason: ~p",
                        [Challenger, Reason]),
            Error;
        {ok, _GwInfo}=Res ->
            Res
    end.

-spec check_last_challenge_block(GwInfo :: blockchain_ledger_gateway_v2:gateway(),
                                 Chain :: blockchain:blockchain(),
                                 Ledger :: blockchain_ledger_v1:ledger(),
                                 HexPOCID :: string()) -> {error, any()} | {ok, blockchain_block:block()}.
check_last_challenge_block(GwInfo, Chain, Ledger, HexPOCID) ->
    LastChallenge = blockchain_ledger_gateway_v2:last_poc_challenge(GwInfo),
    case blockchain:get_block(LastChallenge, Chain) of
        {error, Reason}=Error ->
            lager:error([{poc_id, HexPOCID}],
                        "poc_receipts error get_block, last_challenge: ~p, reason: ~p",
                        [LastChallenge, Reason]),
            Error;
        {ok, LastChallengeBlock} ->
            {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
            PoCInterval = blockchain_utils:challenge_interval(Ledger),
            case LastChallenge + PoCInterval >= Height of
                false ->
                    {error, challenge_too_old};
                true ->
                    {ok, LastChallengeBlock}
            end
    end.

-spec check_path(LastChallengeBlock :: blockchain_block:block(),
                 PoC :: blockchain_ledger_poc_v2:poc(),
                 Challenger :: libp2p_crypto:pubkey_bin(),
                 Secret :: binary(),
                 POCOnionKeyHash :: binary(),
                 Txn :: txn_poc_receipts(),
                 Chain :: blockchain:blockchain()) -> {error, any()} | ok.
check_path(LastChallengeBlock, PoC, Challenger, Secret, POCOnionKeyHash, Txn, Chain) ->
    CheckFun = fun(T) ->
                       blockchain_txn:type(T) == blockchain_txn_poc_request_v2 andalso
                       blockchain_txn_poc_request_v2:onion_key_hash(T) == POCOnionKeyHash andalso
                       blockchain_txn_poc_request_v2:block_hash(T) == blockchain_ledger_poc_v2:block_hash(PoC)
               end,
    case lists:any(CheckFun, blockchain_block:transactions(LastChallengeBlock)) of
        false ->
            {error, onion_key_hash_mismatch};
        true ->
            %% Note there are 2 block hashes here; one is the block hash encoded into the original
            %% PoC request used to establish a lower bound on when that PoC request was made,
            %% and one is the block hash at which the PoC was absorbed onto the chain.
            %%
            %% The first, mediated via a chain var, is mixed with the ECDH derived key for each layer
            %% of the onion to ensure that nodes cannot decrypt the onion layer if they are not synced
            %% with the chain.
            %%
            %% The second of these is combined with the PoC secret to produce the combined entropy
            %% from both the chain and from the PoC requester.
            %%
            %% Keeping these distinct and using them for their intended purpose is important.
            PrePoCBlockHash = blockchain_ledger_poc_v2:block_hash(PoC),
            PoCAbsorbedAtBlockHash  = blockchain_block:hash_block(LastChallengeBlock),
            Entropy = <<Secret/binary, PoCAbsorbedAtBlockHash/binary, Challenger/binary>>,
            {ok, OldLedger} = blockchain:ledger_at(blockchain_block:height(LastChallengeBlock), Chain),

            Vars = blockchain_utils:vars_binary_keys_to_atoms(blockchain_ledger_v1:all_vars(OldLedger)),
            %% Find the original target
            {ok, {Target, TargetRandState}} = blockchain_poc_target_v3:target(Challenger, Entropy, OldLedger, Vars),
            %% Path building phase
            Time = blockchain_block:time(LastChallengeBlock),
            Path = blockchain_poc_path_v4:build(Target, TargetRandState, OldLedger, Time, Vars),

            N = erlang:length(Path),
            [<<IV:16/integer-unsigned-little, _/binary>> | LayerData] = blockchain_txn_poc_receipts_v2:create_secret_hash(Entropy, N+1),
            OnionList = lists:zip([libp2p_crypto:bin_to_pubkey(P) || P <- Path], LayerData),
            {_Onion, Layers} = blockchain_poc_packet:build(libp2p_crypto:keys_from_bin(Secret), IV, OnionList, PrePoCBlockHash, OldLedger),
            %% no witness will exist with the first layer hash
            [_|LayerHashes] = [crypto:hash(sha256, L) || L <- Layers],
            StartV = erlang:monotonic_time(millisecond),
            Ret = validate(Txn, Path, LayerData, LayerHashes, OldLedger),
            maybe_log_duration(receipt_validation, StartV),
            Ret
    end.


maybe_log_duration(Type, Start) ->
    case application:get_env(blockchain, log_validation_times, false) of
        true ->
            End = erlang:monotonic_time(millisecond),
            lager:info("~p took ~p ms", [Type, End - Start]);
        _ -> ok
    end.

-spec set_deltas(Challengee :: libp2p_crypto:pubkey_bin(),
                 {A :: float(), B :: 0 | 1},
                 Deltas :: deltas()) -> deltas().
set_deltas(Challengee, {A, B}, Deltas) ->
    [{Challengee, {A, B}} | Deltas].


%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Tx = #blockchain_txn_poc_receipts_v2_pb{
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
    EncodedTx1 = blockchain_txn_poc_receipts_v2_pb:encode_msg(Tx1#blockchain_txn_poc_receipts_v2_pb{signature = <<>>}),
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

-endif.
