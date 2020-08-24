%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Proof of Coverage Receipts ==
%%%-------------------------------------------------------------------
-module(blockchain_txn_poc_receipts_v1).

-behavior(blockchain_txn).

-behavior(blockchain_json).
-include("blockchain_json.hrl").

-include_lib("helium_proto/include/blockchain_txn_poc_receipts_v1_pb.hrl").
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
    deltas/1, deltas/2,
    check_path_continuation/1,
    print/1,
    to_json/2,
    hex_poc_id/1,
    good_quality_witnesses/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type txn_poc_receipts() :: #blockchain_txn_poc_receipts_v1_pb{}.
-type deltas() :: [{libp2p_crypto:pubkey_bin(), {float(), float()}}].
-type frequencies() :: [float()].

-define(FREQS, [903.9, 904.1, 904.3, 904.5, 904.7, 904.9, 905.1, 905.3]).

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
                    case check_is_valid_poc(Txn, Chain, true) of
                        ok -> ok;
                        {ok, _} ->
                            ok;
                        Error -> Error
                    end
            end
    end.

-spec check_is_valid_poc(Txn :: txn_poc_receipts(),
                         Chain :: blockchain:blockchain(),
                         RunValidation :: boolean()) -> ok | {ok, frequencies()} | {error, any()}.
check_is_valid_poc(Txn, Chain, RunValidation) ->
    Ledger = blockchain:ledger(Chain),
    Challenger = ?MODULE:challenger(Txn),
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
    POCOnionKeyHash = ?MODULE:onion_key_hash(Txn),
    HexPOCID = ?MODULE:hex_poc_id(Txn),
    StartPre = erlang:monotonic_time(millisecond),

    case blockchain_ledger_v1:find_poc(POCOnionKeyHash, Ledger) of
        {error, Reason}=Error ->
            lager:warning([{poc_id, HexPOCID}],
                          "poc_receipts error find_poc, poc_onion_key_hash: ~p, reason: ~p",
                          [POCOnionKeyHash, Reason]),
            Error;
        {ok, PoCs} ->
            Secret = ?MODULE:secret(Txn),
            case blockchain_ledger_poc_v2:find_valid(PoCs, Challenger, Secret) of
                {error, _} ->
                    {error, poc_not_found};
                {ok, PoC} ->
                    case blockchain_gateway_cache:get(Challenger, Ledger) of
                        {error, Reason}=Error ->
                            lager:warning([{poc_id, HexPOCID}],
                                          "poc_receipts error find_gateway_info, challenger: ~p, reason: ~p",
                                          [Challenger, Reason]),
                            Error;
                        {ok, GwInfo} ->
                            LastChallenge = blockchain_ledger_gateway_v2:last_poc_challenge(GwInfo),
                            %% lager:info("gw last ~p ~p ~p", [LastChallenge, HexPOCID, GwInfo]),
                            case blockchain:get_block(LastChallenge, Chain) of
                                {error, Reason}=Error ->
                                    lager:warning([{poc_id, HexPOCID}],
                                                  "poc_receipts error get_block, last_challenge: ~p, reason: ~p",
                                                  [LastChallenge, Reason]),
                                    Error;
                                {ok, Block1} ->
                                    PoCInterval = blockchain_utils:challenge_interval(Ledger),
                                    case LastChallenge + PoCInterval >= Height of
                                        false ->
                                            lager:info("challenge too old ~p ~p", [Challenger, GwInfo]),
                                            {error, challenge_too_old};
                                        true ->
                                            Condition = case blockchain:config(?poc_version, Ledger) of
                                                            {ok, POCVersion} when POCVersion > 1 ->
                                                                fun(T) ->
                                                                        blockchain_txn:type(T) == blockchain_txn_poc_request_v1 andalso
                                                                        blockchain_txn_poc_request_v1:onion_key_hash(T) == POCOnionKeyHash andalso
                                                                        blockchain_txn_poc_request_v1:block_hash(T) == blockchain_ledger_poc_v2:block_hash(PoC)
                                                                end;
                                                            _ ->
                                                                fun(T) ->
                                                                        blockchain_txn:type(T) == blockchain_txn_poc_request_v1 andalso
                                                                        blockchain_txn_poc_request_v1:onion_key_hash(T) == POCOnionKeyHash
                                                                end
                                                        end,
                                            case lists:any(Condition, blockchain_block:transactions(Block1)) of
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
                                                    PoCAbsorbedAtBlockHash  = blockchain_block:hash_block(Block1),
                                                    Entropy = <<Secret/binary, PoCAbsorbedAtBlockHash/binary, Challenger/binary>>,
                                                    maybe_log_duration(prelude, StartPre),
                                                    StartLA = erlang:monotonic_time(millisecond),
                                                    {ok, OldLedger} = blockchain:ledger_at(blockchain_block:height(Block1), Chain),
                                                    maybe_log_duration(ledger_at, StartLA),
                                                    Vars = vars(OldLedger),
                                                    Path = case blockchain:config(?poc_version, OldLedger) of
                                                               {ok, V} when V >= 8 ->
                                                                   %% Targeting phase
                                                                   StartFT = erlang:monotonic_time(millisecond),
                                                                   %% Find the original target
                                                                   {ok, {Target, TargetRandState}} = blockchain_poc_target_v3:target(Challenger, Entropy, OldLedger, Vars),
                                                                   maybe_log_duration(target, StartFT),
                                                                   %% Path building phase
                                                                   StartB = erlang:monotonic_time(millisecond),
                                                                   Time = blockchain_block:time(Block1),
                                                                   RetB = blockchain_poc_path_v4:build(Target, TargetRandState, OldLedger, Time, Vars),
                                                                   maybe_log_duration(build, StartB),
                                                                   RetB;

                                                               {ok, V} when V >= 7 ->
                                                                   StartFT = erlang:monotonic_time(millisecond),
                                                                   %% If we make it to this point, we are bound to have a target.
                                                                   {ok, Target} = blockchain_poc_target_v2:target_v2(Entropy, OldLedger, Vars),
                                                                   maybe_log_duration(target, StartFT),
                                                                   StartB = erlang:monotonic_time(millisecond),
                                                                   Time = blockchain_block:time(Block1),
                                                                   RetB = blockchain_poc_path_v3:build(Target, OldLedger, Time, Entropy, Vars),
                                                                   maybe_log_duration(build, StartB),
                                                                   RetB;

                                                               {ok, V} when V >= 4 ->
                                                                   StartS = erlang:monotonic_time(millisecond),
                                                                   GatewayScoreMap = blockchain_utils:score_gateways(OldLedger),
                                                                   maybe_log_duration(scored, StartS),

                                                                   Time = blockchain_block:time(Block1),
                                                                   {ChallengerGw, _} = maps:get(Challenger, GatewayScoreMap),
                                                                   ChallengerLoc = blockchain_ledger_gateway_v2:location(ChallengerGw),
                                                                   {ok, OldHeight} = blockchain_ledger_v1:current_height(OldLedger),
                                                                   StartFT = erlang:monotonic_time(millisecond),
                                                                   GatewayScores = blockchain_poc_target_v2:filter(GatewayScoreMap, Challenger, ChallengerLoc, OldHeight, Vars),
                                                                   %% If we make it to this point, we are bound to have a target.
                                                                   {ok, Target} = blockchain_poc_target_v2:target(Entropy, GatewayScores, Vars),
                                                                   maybe_log_duration(filter_target, StartFT),
                                                                   StartB = erlang:monotonic_time(millisecond),

                                                                   RetB = case blockchain:config(?poc_typo_fixes, Ledger) of
                                                                              {ok, true} ->
                                                                                  blockchain_poc_path_v2:build(Target, GatewayScores, Time, Entropy, Vars);
                                                                              _ ->
                                                                                  blockchain_poc_path_v2:build(Target, GatewayScoreMap, Time, Entropy, Vars)
                                                                          end,
                                                                   maybe_log_duration(build, StartB),
                                                                   RetB;
                                                               _ ->
                                                                   {Target, Gateways} = blockchain_poc_path:target(Entropy, OldLedger, Challenger),
                                                                   {ok, P} = blockchain_poc_path:build(Entropy, Target, Gateways, LastChallenge, OldLedger),
                                                                   P
                                                           end,
                                                    N = erlang:length(Path),
                                                    [<<IV:16/integer-unsigned-little, _/binary>> | LayerData] = blockchain_txn_poc_receipts_v1:create_secret_hash(Entropy, N+1),
                                                    OnionList = lists:zip([libp2p_crypto:bin_to_pubkey(P) || P <- Path], LayerData),
                                                    {_Onion, Layers} = case blockchain:config(?poc_typo_fixes, Ledger) of
                                                                           {ok, true} ->
                                                                               blockchain_poc_packet:build(libp2p_crypto:keys_from_bin(Secret), IV, OnionList, PrePoCBlockHash, OldLedger);
                                                                           _ ->
                                                                               blockchain_poc_packet:build(libp2p_crypto:keys_from_bin(Secret), IV, OnionList, PrePoCBlockHash, Ledger)
                                                                       end,
                                                    %% no witness will exist with the first layer hash
                                                    [_|LayerHashes] = [crypto:hash(sha256, L) || L <- Layers],
                                                    StartV = erlang:monotonic_time(millisecond),

                                                    case blockchain:config(?poc_version, OldLedger) of
                                                        {ok, POCVer} when POCVer >= 9 ->
                                                            Freqs = lists:map(fun(Layer) ->
                                                                                      <<IntData:16/integer-unsigned-little>> = Layer,
                                                                                      lists:nth((IntData rem 8) + 1, ?FREQS)
                                                                              end, LayerData),
                                                            %% We are on poc v9, check whether we need to run
                                                            %% validation (presumably invoked from is_valid)
                                                            case RunValidation of
                                                                false ->
                                                                    %% no need to run validations
                                                                    {ok, Freqs};
                                                                true ->
                                                                    %% run validations
                                                                    Ret = validate(Txn, Path, LayerData, LayerHashes, OldLedger),
                                                                    maybe_log_duration(receipt_validation, StartV),
                                                                    case Ret of
                                                                        ok ->
                                                                            {ok, Freqs};
                                                                        {error, _}=E -> E
                                                                    end
                                                            end;
                                                        _ ->
                                                            %% We are not on poc v9, just do old behavior
                                                            Ret = validate(Txn, Path, LayerData, LayerHashes, OldLedger),
                                                            maybe_log_duration(receipt_validation, StartV),
                                                            Ret
                                                    end
                                            end
                                    end
                            end
                    end
            end
    end.

maybe_log_duration(Type, Start) ->
    case application:get_env(blockchain, log_validation_times, false) of
        true ->
            End = erlang:monotonic_time(millisecond),
            lager:info("~p took ~p ms", [Type, End - Start]);
        _ -> ok
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
-spec deltas(Txn :: txn_poc_receipts()) -> deltas().
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

-spec deltas(Txn :: txn_poc_receipts(),
             Chain :: blockchain:blockchain()) -> deltas().
deltas(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    case blockchain:config(?poc_version, Ledger) of
        {ok, V} when V >= 9 ->
            %% do the new thing
            calculate_delta(Txn, Chain, true);
        _ ->
            calculate_delta(Txn, Chain, false)
    end.

-spec calculate_delta(Txn :: txn_poc_receipts(),
                      Chain :: blockchain:blockchain(),
                      CalcFreq :: boolean()) -> deltas().
calculate_delta(Txn, Chain, true) ->
    Ledger = blockchain:ledger(Chain),
    Path = blockchain_txn_poc_receipts_v1:path(Txn),
    Length = length(Path),

    {ok, Frequencies} = check_is_valid_poc(Txn, Chain, false),

    lists:reverse(element(1, lists:foldl(fun({ElementPos, Element}, {Acc, true}) ->
                                                 Challengee = blockchain_poc_path_element_v1:challengee(Element),
                                                 NextElements = lists:sublist(Path, ElementPos+1, Length),
                                                 HasContinued = check_path_continuation(NextElements),

                                                 {PreviousElement, ReceiptFreq, WitnessFreq} =
                                                 case ElementPos of
                                                     1 ->
                                                         {undefined, 0.0, hd(Frequencies)};
                                                     _ ->
                                                         {lists:nth(ElementPos - 1, Path), lists:nth(ElementPos - 1, Frequencies), lists:nth(ElementPos, Frequencies)}
                                                 end,

                                                 {Val, Continue} = calculate_alpha_beta(HasContinued, Element, PreviousElement, ReceiptFreq, WitnessFreq, Ledger),
                                                 {set_deltas(Challengee, Val, Acc), Continue};
                              (_, Acc) ->
                                   Acc
                           end,
                           {[], true},
                           lists:zip(lists:seq(1, Length), Path))));
calculate_delta(Txn, Chain, false) ->
    Ledger = blockchain:ledger(Chain),
    Path = blockchain_txn_poc_receipts_v1:path(Txn),
    Length = length(Path),
    lists:reverse(element(1, lists:foldl(fun({ElementPos, Element}, {Acc, true}) ->
                                                 Challengee = blockchain_poc_path_element_v1:challengee(Element),
                                                 NextElements = lists:sublist(Path, ElementPos+1, Length),
                                                 HasContinued = check_path_continuation(NextElements),
                                                 {Val, Continue} = calculate_alpha_beta(HasContinued, Element, Ledger),
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
            %% likely the next hop broke the path
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
            {{0.9, 0}, true};
        {false, _, _} ->
            %% path broke, you killed it
            {{0, 1}, false}
    end.

-spec calculate_alpha_beta(HasContinued :: boolean(),
                           Element :: blockchain_poc_path_element_v1:poc_element(),
                           PreviousElement :: blockchain_poc_path_element_v1:poc_element(),
                           ReceiptFreq :: float(),
                           WitnessFreq :: float(),
                           Ledger :: blockchain_ledger_v1:ledger()) -> {{float(), 0 | 1}, boolean()}.
calculate_alpha_beta(HasContinued, Element, PreviousElement, ReceiptFreq, WitnessFreq, Ledger) ->
    Receipt = valid_receipt(PreviousElement, Element, ReceiptFreq, Ledger),
    Witnesses = valid_witnesses(Element, WitnessFreq, Ledger),
    allocate_alpha_beta(HasContinued, Element, Receipt, Witnesses, Ledger).

-spec calculate_alpha_beta(HasContinued :: boolean(),
                           Element :: blockchain_poc_path_element_v1:poc_element(),
                           Ledger :: blockchain_ledger_v1:ledger()) -> {{float(), 0 | 1}, boolean()}.
calculate_alpha_beta(HasContinued, Element, Ledger) ->
    Receipt = blockchain_poc_path_element_v1:receipt(Element),
    Witnesses = blockchain_poc_path_element_v1:witnesses(Element),
    allocate_alpha_beta(HasContinued, Element, Receipt, Witnesses, Ledger).

-spec allocate_alpha_beta(HasContinued :: boolean(),
                          Element :: blockchain_poc_path_element_v1:poc_element(),
                          Receipt :: blockchain_poc_receipt_v1:poc_receipt(),
                          Witnesses :: [blockchain_poc_witness_v1:poc_witness()],
                          Ledger :: blockchain_ledger_v1:ledger()) -> {{float(), 0 | 1}, boolean()}.
allocate_alpha_beta(HasContinued, Element, Receipt, Witnesses, Ledger) ->
    case {HasContinued, Receipt, Witnesses} of
        {true, undefined, _} ->
            %% path continued, no receipt, don't care about witnesses
            {{0.8, 0}, true};
        {true, Receipt, _} when Receipt /= undefined ->
            %% path continued, receipt, don't care about witnesses
            {{1, 0}, true};
        {false, undefined, Wxs} when length(Wxs) > 0 ->
            %% path broke, no receipt, witnesses
            {calculate_witness_quality(Element, Ledger), true};
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
            {calculate_witness_quality(Element, Ledger), true};
        {false, _, _} ->
            %% path broke, you killed it
            {{0, 1}, false}
    end.

-spec calculate_witness_quality(Element :: blockchain_poc_path_element_v1:poc_element(),
                                Ledger :: blockchain_ledger_v1:ledger()) -> {float(), 0}.
calculate_witness_quality(Element, Ledger) ->
    case blockchain_poc_path_element_v1:receipt(Element) of
        undefined ->
            %% no poc receipt
            case good_quality_witnesses(Element, Ledger) of
                [] ->
                    %% Either the witnesses are too close or the RSSIs are too high
                    %% no alpha bump
                    {0, 0};
                _ ->
                    %% high alpha bump, but not as high as when there is a receipt
                    {0.7, 0}
            end;
        _Receipt ->
            %% element has a receipt
            case good_quality_witnesses(Element, Ledger) of
                [] ->
                    %% Either the witnesses are too close or the RSSIs are too high
                    %% no alpha bump
                    {0, 0};
                _ ->
                    %% high alpha bump
                    {0.9, 0}
            end
    end.

-spec set_deltas(Challengee :: libp2p_crypto:pubkey_bin(),
                 {A :: float(), B :: 0 | 1},
                 Deltas :: deltas()) -> deltas().
set_deltas(Challengee, {A, B}, Deltas) ->
    [{Challengee, {A, B}} | Deltas].

-spec good_quality_witnesses(Element :: blockchain_poc_path_element_v1:poc_element(),
                             Ledger :: blockchain_ledger_v1:ledger()) -> [blockchain_poc_witness_v1:poc_witness()].
good_quality_witnesses(Element, Ledger) ->
    Challengee = blockchain_poc_path_element_v1:challengee(Element),
    Witnesses = blockchain_poc_path_element_v1:witnesses(Element),
    {ok, ParentRes} = blockchain_ledger_v1:config(?poc_v4_parent_res, Ledger),
    {ok, ExclusionCells} = blockchain_ledger_v1:config(?poc_v4_exclusion_cells, Ledger),

    {ok, ChallengeeGw} = blockchain_gateway_cache:get(Challengee, Ledger),
    ChallengeeLoc = blockchain_ledger_gateway_v2:location(ChallengeeGw),
    ChallengeeParentIndex = h3:parent(ChallengeeLoc, ParentRes),

    %% Good quality witnesses
    lists:filter(fun(Witness) ->
                                 WitnessPubkeyBin = blockchain_poc_witness_v1:gateway(Witness),
                                 {ok, WitnessGw} = blockchain_gateway_cache:get(WitnessPubkeyBin, Ledger),
                                 WitnessGwLoc = blockchain_ledger_gateway_v2:location(WitnessGw),
                                 WitnessParentIndex = h3:parent(WitnessGwLoc, ParentRes),
                                 WitnessRSSI = blockchain_poc_witness_v1:signal(Witness),
                                 FreeSpacePathLoss = blockchain_utils:free_space_path_loss(WitnessGwLoc, ChallengeeLoc),
                                 %% Check that the witness is far
                                 try h3:grid_distance(WitnessParentIndex, ChallengeeParentIndex) >= ExclusionCells of
                                     Res -> Res
                                 catch
                                     %% Grid distance may badarg because of pentagonal distortion or
                                     %% non matching resolutions or just being too far.
                                     %% In either of those cases, we assume that the gateway
                                     %% is potentially legitimate to be a target.
                                     _:_ -> true
                                 end andalso
                                 %% Check that the RSSI seems reasonable
                                 (WitnessRSSI =< FreeSpacePathLoss)
                         end,
                         Witnesses).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
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
        {ok, GwInfo} = blockchain_gateway_cache:get(Challenger, Ledger, false),
        LastChallenge = blockchain_ledger_gateway_v2:last_poc_challenge(GwInfo),
        PoCInterval = blockchain_utils:challenge_interval(Ledger),
        case LastChallenge + PoCInterval >= Height of
            false ->
                lager:info("challenge too old ~p ~p", [Challenger, GwInfo]),
                {error, challenge_too_old};
            true ->
                case blockchain:config(?poc_version, Ledger) of
                    {error, not_found} ->
                        %% Older poc version, don't add witnesses
                        ok;
                    {ok, POCVersion} when POCVersion > 1 ->
                        %% Find upper and lower time bounds for this poc txn and use those to clamp
                        %% witness timestamps being inserted in the ledger
                        case get_lower_and_upper_bounds(Secret, LastOnionKeyHash, Challenger, Ledger, Chain) of
                            {error, _}=E ->
                                E;
                            {ok, {Lower, Upper}} ->
                                %% Insert the witnesses for gateways in the path into ledger
                                Path = blockchain_txn_poc_receipts_v1:path(Txn),
                                ok = insert_witnesses(Path, Lower, Upper, Ledger)
                        end
                end,

                case blockchain_ledger_v1:delete_poc(LastOnionKeyHash, Challenger, Ledger) of
                    {error, _}=Error1 ->
                        Error1;
                    ok ->
                        case blockchain:config(?poc_version, Ledger) of
                            {ok, V} when V > 4 ->
                                lists:foldl(fun({Gateway, Delta}, _Acc) ->
                                                    blockchain_ledger_v1:update_gateway_score(Gateway, Delta, Ledger)
                                            end,
                                            ok,
                                            ?MODULE:deltas(Txn, Chain));
                            _ ->
                                lists:foldl(fun({Gateway, Delta}, _Acc) ->
                                                    blockchain_ledger_v1:update_gateway_score(Gateway, Delta, Ledger)
                                            end,
                                            ok,
                                            ?MODULE:deltas(Txn))
                        end
                end
        end
    catch What:Why:Stacktrace ->
            lager:warning([{poc_id, HexPOCID}], "poc receipt calculation failed: ~p ~p ~p", [What, Why, Stacktrace]),
            {error, state_missing}
    end.

-spec get_lower_and_upper_bounds(Secret :: binary(),
                                 OnionKeyHash :: binary(),
                                 Challenger :: libp2p_crypto:pubkey_bin(),
                                 Ledger :: blockchain_ledger_v1:ledger(),
                                 Chain :: blockchain:blockchain()) -> {error, any()} | {ok, {non_neg_integer(), non_neg_integer()}}.
get_lower_and_upper_bounds(Secret, OnionKeyHash, Challenger, Ledger, Chain) ->
    case blockchain_ledger_v1:find_poc(OnionKeyHash, Ledger) of
        {error, Reason}=Error0 ->
            lager:warning("poc_receipts error find_poc, poc_onion_key_hash: ~p, reason: ~p", [OnionKeyHash, Reason]),
            Error0;
        {ok, PoCs} ->
            case blockchain_ledger_poc_v2:find_valid(PoCs, Challenger, Secret) of
                {error, _}=Error1 ->
                    Error1;
                {ok, _PoC} ->
                    case blockchain_gateway_cache:get(Challenger, Ledger) of
                        {error, Reason}=Error2 ->
                            lager:warning("poc_receipts error find_gateway_info, challenger: ~p, reason: ~p", [Challenger, Reason]),
                            Error2;
                        {ok, GwInfo} ->
                            LastChallenge = blockchain_ledger_gateway_v2:last_poc_challenge(GwInfo),
                            case blockchain:get_block(LastChallenge, Chain) of
                                {error, Reason}=Error3 ->
                                    lager:warning("poc_receipts error get_block, last_challenge: ~p, reason: ~p", [LastChallenge, Reason]),
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
-spec insert_witnesses(Path :: blockchain_poc_path_element_v1:path(),
                       LowerTimeBound :: non_neg_integer(),
                       UpperTimeBound :: non_neg_integer(),
                       Ledger :: blockchain_ledger_v1:ledger()) -> ok.
insert_witnesses(Path, LowerTimeBound, UpperTimeBound, Ledger) ->
    Length = length(Path),
    lists:foreach(fun({N, Element}) ->
                          Challengee = blockchain_poc_path_element_v1:challengee(Element),
                          Witnesses = blockchain_poc_path_element_v1:witnesses(Element),
                          %% TODO check these witnesses have valid RSSI
                          WitnessInfo0 = lists:foldl(fun(Witness, Acc) ->
                                                             TS = case blockchain_poc_witness_v1:timestamp(Witness) of
                                                                      T when T < LowerTimeBound ->
                                                                          LowerTimeBound;
                                                                      T when T > UpperTimeBound ->
                                                                          UpperTimeBound;
                                                                      T ->
                                                                          T
                                                                  end,
                                                             [{blockchain_poc_witness_v1:signal(Witness), TS, blockchain_poc_witness_v1:gateway(Witness)} | Acc]
                                                     end,
                                                     [],
                                                     Witnesses),
                          NextElements = lists:sublist(Path, N+1, Length),
                          WitnessInfo = case check_path_continuation(NextElements) of
                                                 true ->
                                                     %% the next hop is also a witness for this
                                                     NextHopElement = hd(NextElements),
                                                     NextHopAddr = blockchain_poc_path_element_v1:challengee(NextHopElement),
                                                     case blockchain_poc_path_element_v1:receipt(NextHopElement) of
                                                         undefined ->
                                                             %% There is no receipt from the next hop
                                                             %% We clamp to LowerTimeBound as best-effort
                                                             [{undefined, LowerTimeBound, NextHopAddr} | WitnessInfo0];
                                                         NextHopReceipt ->
                                                             [{blockchain_poc_receipt_v1:signal(NextHopReceipt),
                                                               blockchain_poc_receipt_v1:timestamp(NextHopReceipt),
                                                               NextHopAddr} | WitnessInfo0]
                                                     end;
                                                 false ->
                                                     WitnessInfo0
                                             end,
                          blockchain_ledger_v1:add_gateway_witnesses(Challengee, WitnessInfo, Ledger)
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
    TxnPath = ?MODULE:path(Txn),
    TxnPathLength = length(TxnPath),
    RebuiltPathLength = length(Path),
    ZippedLayers = lists:zip(LayerData, LayerHashes),
    ZippedLayersLength = length(ZippedLayers),
    HexPOCID = ?MODULE:hex_poc_id(Txn),
    lager:debug([{poc_id, HexPOCID}], "starting poc receipt validation..."),

    case TxnPathLength == RebuiltPathLength of
        false ->
            HumanTxnPath = [element(2, erl_angry_purple_tiger:animal_name(libp2p_crypto:bin_to_b58(blockchain_poc_path_element_v1:challengee(E)))) || E <- TxnPath],
            HumanRebuiltPath = [element(2, erl_angry_purple_tiger:animal_name(libp2p_crypto:bin_to_b58(A))) || A <- Path],
            lager:warning([{poc_id, HexPOCID}], "TxnPathLength: ~p, RebuiltPathLength: ~p", [TxnPathLength, RebuiltPathLength]),
            lager:warning([{poc_id, HexPOCID}], "TxnPath: ~p", [HumanTxnPath]),
            lager:warning([{poc_id, HexPOCID}], "RebuiltPath: ~p", [HumanRebuiltPath]),
            {error, path_length_mismatch};
        true ->
            %% Now check whether layers are of equal length
            case TxnPathLength == ZippedLayersLength of
                false ->
                    lager:warning([{poc_id, HexPOCID}], "TxnPathLength: ~p, ZippedLayersLength: ~p", [TxnPathLength, ZippedLayersLength]),
                    {error, zip_layer_length_mismatch};
                true ->
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
                                                               case blockchain_ledger_v1:config(?poc_version, OldLedger) of
                                                                   {ok, V} when V > 1 ->
                                                                       %% check there are no duplicates in witnesses list
                                                                       WitnessGateways = [blockchain_poc_witness_v1:gateway(W) || W <- Witnesses],
                                                                       case length(WitnessGateways) == length(lists:usort(WitnessGateways)) of
                                                                           false ->
                                                                               {error, duplicate_witnesses};
                                                                           true ->
                                                                               check_witness_layerhash(Witnesses, Gateway, LayerHash, OldLedger)
                                                                       end;
                                                                   _ ->
                                                                       check_witness_layerhash(Witnesses, Gateway, LayerHash, OldLedger)
                                                               end
                                                       end;
                                                   false ->
                                                       case Receipt == undefined of
                                                           true ->
                                                               lager:warning([{poc_id, HexPOCID}],
                                                                             "Receipt undefined, ExpectedOrigin: ~p, LayerDatum: ~p, Gateway: ~p",
                                                                             [Receipt, ExpectedOrigin, LayerDatum, Gateway]);
                                                           false ->
                                                               lager:warning([{poc_id, HexPOCID}],
                                                                             "Origin: ~p, ExpectedOrigin: ~p, Data: ~p, LayerDatum: ~p, ReceiptGateway: ~p, Gateway: ~p",
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
                                               lager:warning([{poc_id, HexPOCID}], "receipt not in order"),
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

print(#blockchain_txn_poc_receipts_v1_pb{
         challenger=Challenger,
         onion_key_hash=OnionKeyHash,
         path=Path
        }=Txn) ->
    io_lib:format("type=poc_receipts_v1 hash=~p challenger=~p onion=~p path:\n\t~s",
                  [?TO_B58(?MODULE:hash(Txn)),
                   ?TO_ANIMAL_NAME(Challenger),
                   ?TO_B58(OnionKeyHash),
                   print_path(Path)]).

print_path(Path) ->
    string:join(lists:map(fun(Element) ->
                                  blockchain_poc_path_element_v1:print(Element)
                          end,
                          Path), "\n\t").

-spec to_json(txn_poc_receipts(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Txn, _Opts) ->
    #{
      type => <<"poc_receipts_v1">>,
      hash => ?BIN_TO_B64(hash(Txn)),
      secret => ?BIN_TO_B64(secret(Txn)),
      onion_key_hash => ?BIN_TO_B64(onion_key_hash(Txn)),
      path => [blockchain_poc_path_element_v1:to_json(E, []) || E <- path(Txn)],
      fee => fee(Txn),
      challenger => ?BIN_TO_B58(challenger(Txn))
     }.


check_witness_layerhash(Witnesses, Gateway, LayerHash, OldLedger) ->
    %% all the witnesses should have the right LayerHash
    %% and be valid
    case
        lists:all(
          fun(Witness) ->
                  %% the witnesses should have an asserted location
                  %% at the point when the request was mined!
                  WitnessGateway = blockchain_poc_witness_v1:gateway(Witness),
                  case blockchain_gateway_cache:get(WitnessGateway, OldLedger) of
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
    end.

-spec hex_poc_id(txn_poc_receipts()) -> string().
hex_poc_id(Txn) ->
    #{secret := _OnionPrivKey, public := OnionPubKey} = libp2p_crypto:keys_from_bin(?MODULE:secret(Txn)),
    <<POCID:10/binary, _/binary>> = libp2p_crypto:pubkey_to_bin(OnionPubKey),
    blockchain_utils:bin_to_hex(POCID).

vars(Ledger) ->
    blockchain_utils:vars_binary_keys_to_atoms(
      maps:from_list(blockchain_ledger_v1:snapshot_vars(Ledger))).

valid_receipt(undefined, _Element, _Freq, _Ledger) ->
    %% first hop in the path, cannot be validated.
    undefined;
valid_receipt(PreviousElement, Element, Freq, Ledger) ->
    case blockchain_poc_path_element_v1:receipt(Element) of
        undefined ->
            %% nothing to validate
            undefined;
        Receipt ->
            {ok, Source} = blockchain_gateway_cache:get(blockchain_poc_path_element_v1:challengee(PreviousElement), Ledger),
            {ok, Destination} = blockchain_gateway_cache:get(blockchain_poc_path_element_v1:challengee(Element), Ledger),
            SourceLoc = blockchain_ledger_gateway_v2:location(Source),
            DestinationLoc = blockchain_ledger_gateway_v2:location(Destination),
            {ok, ExclusionCells} = blockchain_ledger_v1:config(?poc_v4_exclusion_cells, Ledger),
            {ok, ParentRes} = blockchain_ledger_v1:config(?poc_v4_parent_res, Ledger),
            SourceParentIndex = h3:parent(SourceLoc, ParentRes),
            DestinationParentIndex = h3:parent(DestinationLoc, ParentRes),
            try h3:grid_distance(SourceParentIndex, DestinationParentIndex) >= ExclusionCells of
                true ->
                    FreeSpacePathLoss = blockchain_utils:free_space_path_loss(SourceLoc, DestinationLoc),
                    RSSI = blockchain_poc_receipt_v1:signal(Receipt),
                    SNR = blockchain_poc_receipt_v1:snr(Receipt),
                    case RSSI < FreeSpacePathLoss of
                        false ->
                            %% RSSI is impossibly high discard this receipt
                            lager:warning("receipt ~p -> ~p rejected at height ~p for RSSI ~p above FSPL ~p with SNR ~p",
                                          [?TO_ANIMAL_NAME(blockchain_poc_path_element_v1:challengee(PreviousElement)),
                                           ?TO_ANIMAL_NAME(blockchain_poc_path_element_v1:challengee(Element)),
                                           element(2, blockchain_ledger_v1:current_height(Ledger)),
                                           RSSI, FreeSpacePathLoss, SNR]),
                            undefined;
                        true ->
                            case blockchain:config(?data_aggregation_version, Ledger) of
                                {ok, 1} ->
                                    {LowerBound, _} = calculate_rssi_bounds_from_snr(SNR),
                                    case RSSI >= LowerBound of
                                        true ->
                                            case abs(blockchain_poc_receipt_v1:frequency(Receipt) - Freq) =< 0.001 of
                                                true ->
                                                    lager:info("receipt ok"),
                                                    Receipt;
                                                false ->
                                                    lager:warning("receipt ~p -> ~p rejected at height ~p for frequency ~p /= ~p RSSI ~p SNR ~p",
                                                                  [?TO_ANIMAL_NAME(blockchain_poc_path_element_v1:challengee(PreviousElement)),
                                                                   ?TO_ANIMAL_NAME(blockchain_poc_path_element_v1:challengee(Element)),
                                                                   element(2, blockchain_ledger_v1:current_height(Ledger)),
                                                                   blockchain_poc_receipt_v1:frequency(Receipt), Freq,
                                                                   RSSI, SNR]),
                                                    undefined
                                            end;
                                        false ->
                                            lager:warning("receipt ~p -> ~p rejected at height ~p for RSSI ~p below lower bound ~p with SNR ~p",
                                                          [?TO_ANIMAL_NAME(blockchain_poc_path_element_v1:challengee(PreviousElement)),
                                                           ?TO_ANIMAL_NAME(blockchain_poc_path_element_v1:challengee(Element)),
                                                           element(2, blockchain_ledger_v1:current_height(Ledger)),
                                                           RSSI, LowerBound, SNR]),
                                            undefined
                                    end;
                                _ ->
                                    %% SNR not collected, nothing else we can check
                                    Receipt
                            end
                    end;
                false ->
                    %% too close
                    undefined
            catch
                _:_ ->
                    %% pentagonal distortion
                    undefined
            end
    end.

valid_witnesses(Element, Freq, Ledger) ->
    {ok, Source} = blockchain_gateway_cache:get(blockchain_poc_path_element_v1:challengee(Element), Ledger),

    Witnesses = blockchain_poc_path_element_v1:witnesses(Element),

    lists:filter(fun(Witness) ->
                         {ok, Destination} = blockchain_gateway_cache:get(blockchain_poc_witness_v1:gateway(Witness), Ledger),
                         SourceLoc = blockchain_ledger_gateway_v2:location(Source),
                         DestinationLoc = blockchain_ledger_gateway_v2:location(Destination),
                         {ok, ExclusionCells} = blockchain_ledger_v1:config(?poc_v4_exclusion_cells, Ledger),
                         {ok, ParentRes} = blockchain_ledger_v1:config(?poc_v4_parent_res, Ledger),
                         SourceParentIndex = h3:parent(SourceLoc, ParentRes),
                         DestinationParentIndex = h3:parent(DestinationLoc, ParentRes),
                         try h3:grid_distance(SourceParentIndex, DestinationParentIndex) >= ExclusionCells of
                             true ->
                                 FreeSpacePathLoss = blockchain_utils:free_space_path_loss(SourceLoc, DestinationLoc),
                                 RSSI = blockchain_poc_witness_v1:signal(Witness),
                                 SNR = blockchain_poc_witness_v1:snr(Witness),

                                 case RSSI < FreeSpacePathLoss of
                                     false ->
                                         %% RSSI is impossibly high discard this witness
                                         lager:warning("witness ~p -> ~p rejected at height ~p for RSSI ~p above FSPL ~p with SNR ~p",
                                                       [?TO_ANIMAL_NAME(blockchain_poc_path_element_v1:challengee(Element)),
                                                        ?TO_ANIMAL_NAME(blockchain_poc_witness_v1:gateway(Witness)),
                                                        element(2, blockchain_ledger_v1:current_height(Ledger)),
                                                        RSSI, FreeSpacePathLoss, SNR]),
                                         false;
                                     true ->
                                         case blockchain:config(?data_aggregation_version, Ledger) of
                                             {ok, 1} ->
                                                 {LowerBound, _} = calculate_rssi_bounds_from_snr(SNR),
                                                 case RSSI >= LowerBound of
                                                     true ->
                                                         case abs(blockchain_poc_witness_v1:frequency(Witness) - Freq) =< 0.001 of
                                                             true ->
                                                                 lager:info("witness ok"),
                                                                 true;
                                                             false ->
                                                                 lager:warning("witness ~p -> ~p rejected at height ~p for frequency ~p /= ~p RSSI ~p SNR ~p",
                                                                               [?TO_ANIMAL_NAME(blockchain_poc_path_element_v1:challengee(Element)),
                                                                                ?TO_ANIMAL_NAME(blockchain_poc_witness_v1:gateway(Witness)),
                                                                                element(2, blockchain_ledger_v1:current_height(Ledger)),
                                                                                blockchain_poc_witness_v1:frequency(Witness), Freq,
                                                                                RSSI, SNR]),
                                                                 false
                                                         end;
                                                     false ->
                                                         lager:warning("witness ~p -> ~p rejected at height ~p for RSSI ~p below lower bound ~p with SNR ~p",
                                                                       [?TO_ANIMAL_NAME(blockchain_poc_path_element_v1:challengee(Element)),
                                                                        ?TO_ANIMAL_NAME(blockchain_poc_witness_v1:gateway(Witness)),
                                                                        element(2, blockchain_ledger_v1:current_height(Ledger)),
                                                                        RSSI, LowerBound, SNR]),
                                                         false
                                                 end;
                                             _ ->
                                                 %% SNR not collected, nothing else we can check
                                                 true
                                         end
                                 end;
                             false ->
                                 %% too close
                                 false
                         catch _:_ ->
                                   %% pentagonal distortion
                                   false
                         end
                 end, Witnesses).

scale_unknown_snr(UnknownSNR) ->
    Diffs = lists:map(fun(K) -> {math:sqrt(math:pow(UnknownSNR - K, 2.0)), K} end, maps:keys(?SNR_CURVE)),
    {ScaleFactor, Key} = hd(lists:sort(Diffs)),
    {Low, High} = maps:get(Key, ?SNR_CURVE),
    {Low + (Low * ScaleFactor), High + (High * ScaleFactor)}.

calculate_rssi_bounds_from_snr(SNR) ->
    %% keef says rounding up hurts the least
    CeilSNR = ceil(SNR),
    case maps:get(CeilSNR, ?SNR_CURVE, undefined) of
        undefined ->
            scale_unknown_snr(CeilSNR);
        V ->
            V
    end.


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
                                                                                                     <<"f">>, 10.1, 912.4},
                                                                       [{blockchain_poc_witness_v1_pb,<<"g">>,
                                                                                                      456,-100,
                                                                                                      <<"h">>,
                                                                                                      <<"i">>, 10.1, 912.4},
                                                                        {blockchain_poc_witness_v1_pb,<<"j">>,
                                                                                                      789,-114,
                                                                                                      <<"k">>,
                                                                                                      <<"l">>, 10.1, 912.4}]},
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
                                                                                                     <<"b">>, 10.1, 912.4},
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
                                                                                                     <<"b">>, 10.1, 912.4},
                                                                       []},
                                    {blockchain_poc_path_element_v1_pb,<<"second">>,
                                                                       undefined,
                                                                       [{blockchain_poc_witness_v1_pb,<<"fourth">>,
                                                                                                      1559953991034558678,-100,
                                                                                                      <<>>,
                                                                                                      <<>>, 10.1, 912.4},
                                                                        {blockchain_poc_witness_v1_pb,<<"first">>,
                                                                                                      1559953991035078007,-72,
                                                                                                      <<>>,
                                                                                                      <<>>, 10.1, 912.4}]},
                                    {blockchain_poc_path_element_v1_pb,<<"third">>,
                                                                       undefined,[]},
                                    {blockchain_poc_path_element_v1_pb,<<"second">>,
                                                                       undefined,
                                                                       [{blockchain_poc_witness_v1_pb,<<"fourth">>,
                                                                                                      1559953992074400943,-100,
                                                                                                      <<>>,
                                                                                                      <<>>, 10.1, 912.4},
                                                                        {blockchain_poc_witness_v1_pb,<<"first">>,
                                                                                                      1559953992075156868,-84,
                                                                                                      <<>>,
                                                                                                      <<>>, 10.1, 912.4}]}],
                                   0,
                                   <<"gg">>},

    Deltas = deltas(Txn),
    ?assertEqual(4, length(Deltas)),
    SecondDeltas = proplists:get_all_values(<<"second">>, Deltas),
    ?assertEqual(2, length(SecondDeltas)),
    {SecondAlphas, _} = lists:unzip(SecondDeltas),
    ?assert(lists:sum(SecondAlphas) > 1),
    ok.

to_json_test() ->
    Txn = {blockchain_txn_poc_receipts_v1_pb,<<"a">>,<<"b">>,<<"c">>,
           [{blockchain_poc_path_element_v1_pb,<<"first">>,
             {blockchain_poc_receipt_v1_pb,<<"d">>,
              123,0,<<"e">>,p2p,
              <<"f">>, 10.1, 912.4},
             [{blockchain_poc_witness_v1_pb,<<"g">>,
               456,-100,
               <<"h">>,
               <<"i">>, 10.1, 912.4},
              {blockchain_poc_witness_v1_pb,<<"j">>,
               789,-114,
               <<"k">>,
               <<"l">>, 10.1, 912.4}]},
            {blockchain_poc_path_element_v1_pb,<<"second">>, undefined,[]},
            {blockchain_poc_path_element_v1_pb,<<"m">>, undefined,[]},
            {blockchain_poc_path_element_v1_pb,<<"n">>, undefined,[]},
            {blockchain_poc_path_element_v1_pb,<<"i">>, undefined,[]}],
           0,
           <<"impala">>},
    Json = to_json(Txn, []),

    ?assert(lists:all(fun(K) -> maps:is_key(K, Json) end,
                      [type, hash, secret, onion_key_hash, path, fee, challenger])).


-endif.
