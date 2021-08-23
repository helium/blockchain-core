%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Proof of Coverage Receipts ==
%%%-------------------------------------------------------------------
-module(blockchain_txn_poc_receipts_v1).

-behavior(blockchain_txn).
-behavior(blockchain_json).

-include("blockchain_json.hrl").
-include("blockchain_caps.hrl").
-include("blockchain_vars.hrl").
-include("blockchain_utils.hrl").
-include_lib("helium_proto/include/blockchain_txn_poc_receipts_v1_pb.hrl").

-export([
    new/4,
    new/5,
    hash/1,
    onion_key_hash/1,
    challenger/1,
    secret/1,
    path/1,
    fee/1,
    fee_payer/2,
    request_block_hash/1,
    signature/1,
    sign/2,
    is_valid/2,
    absorb/2,
    create_secret_hash/2,
    connections/1,
    deltas/1, deltas/2,
    check_path_continuation/1,
    print/1,
    json_type/0,
    to_json/2,
    poc_id/1,
    good_quality_witnesses/2,
    valid_witnesses/3,
    tagged_witnesses/3,
    get_channels/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type txn_poc_receipts() :: #blockchain_txn_poc_receipts_v1_pb{}.
-type deltas() :: [{libp2p_crypto:pubkey_bin(), {float(), float()}}].
-type tagged_witnesses() :: [{IsValid :: boolean(), InvalidReason :: binary(), Witness :: blockchain_poc_witness_v1:witness()}].

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

-spec new(libp2p_crypto:pubkey_bin(), binary(), binary(), binary(),
          blockchain_poc_path_element_v1:path()) -> txn_poc_receipts().
new(Challenger, Secret, OnionKeyHash, BlockHash, Path) ->
    #blockchain_txn_poc_receipts_v1_pb{
        challenger=Challenger,
        secret=Secret,
        onion_key_hash=OnionKeyHash,
        path=Path,
        fee=0,
        request_block_hash=BlockHash,
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

-spec fee_payer(txn_poc_receipts(), blockchain_ledger_v1:ledger()) -> libp2p_crypto:pubkey_bin() | undefined.
fee_payer(_Txn, _Ledger) ->
    undefined.

request_block_hash(Txn) ->
    Txn#blockchain_txn_poc_receipts_v1_pb.request_block_hash.

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
-spec is_valid(txn_poc_receipts(), blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
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
            case blockchain_gateway_cache:get(Challenger, Ledger) of
                {error, _Reason}=Error ->
                    Error;
                {ok, ChallengerGWInfo} ->
                    %% check the challenger is allowed to issue POCs
                    case blockchain_ledger_gateway_v2:is_valid_capability(ChallengerGWInfo, ?GW_CAPABILITY_POC_CHALLENGER, Ledger) of
                        false -> {error, {challenger_not_allowed, blockchain_ledger_gateway_v2:mode(ChallengerGWInfo)}};
                        true ->
                            case ?MODULE:path(Txn) =:= [] of
                                true ->
                                    {error, empty_path};
                                false ->
                                    case check_is_valid_poc(Txn, Chain) of
                                        ok -> ok;
                                        {ok, _} ->
                                            ok;
                                        Error -> Error
                                    end
                            end
                    end
            end
    end.

-spec check_is_valid_poc(Txn :: txn_poc_receipts(),
                         Chain :: blockchain:blockchain()) -> ok | {ok, [non_neg_integer(), ...]} | {error, any()}.
check_is_valid_poc(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Challenger = ?MODULE:challenger(Txn),
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
    POCOnionKeyHash = ?MODULE:onion_key_hash(Txn),
    POCID = ?MODULE:poc_id(Txn),
    StartPre = erlang:monotonic_time(millisecond),

    case blockchain_ledger_v1:find_poc(POCOnionKeyHash, Ledger) of
        {error, Reason}=Error ->
            lager:warning([{poc_id, POCID}],
                          "poc_receipts error find_poc, poc_onion_key_hash: ~p, reason: ~p",
                          [POCOnionKeyHash, Reason]),
            Error;
        {ok, PoCs} ->
            Secret = ?MODULE:secret(Txn),
            case blockchain_ledger_poc_v2:find_valid(PoCs, Challenger, Secret) of
                {error, _} ->
                    {error, poc_not_found};
                {ok, PoC} ->
                    {ok, LastChallenge} = blockchain_ledger_v1:find_gateway_last_challenge(Challenger, Ledger),
                    case blockchain:get_block(LastChallenge, Chain) of
                        {error, Reason}=Error ->
                            lager:warning([{poc_id, POCID}],
                                          "poc_receipts error get_block, last_challenge: ~p, reason: ~p",
                                          [LastChallenge, Reason]),
                            Error;
                        {ok, Block1} ->
                            PoCInterval = blockchain_utils:challenge_interval(Ledger),
                            case LastChallenge + PoCInterval >= Height of
                                false ->
                                    lager:info("challenge too old ~p ~p", [Challenger, LastChallenge]),
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
                                                           GatewayScores = blockchain_poc_target_v2:filter(GatewayScoreMap, Challenger, ChallengerLoc, OldHeight, Vars, Ledger),
                                                           %% If we make it to this point, we are bound to have a target.
                                                           {ok, Target} = blockchain_poc_target_v2:target(Entropy, GatewayScores, Vars),
                                                           maybe_log_duration(filter_target, StartFT),
                                                           StartB = erlang:monotonic_time(millisecond),

                                                           RetB = case blockchain:config(?poc_typo_fixes, Ledger) of
                                                                      {ok, true} ->
                                                                          blockchain_poc_path_v2:build(Target, GatewayScores, Time, Entropy, Vars, Ledger);
                                                                      _ ->
                                                                          blockchain_poc_path_v2:build(Target, GatewayScoreMap, Time, Entropy, Vars, Ledger)
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
                                                    Channels = lists:map(fun(Layer) ->
                                                                                 <<IntData:16/integer-unsigned-little>> = Layer,
                                                                                 IntData rem 8
                                                                         end, LayerData),
                                                    %% We are on poc v9
                                                    %% %% run validations
                                                    Ret = case POCVer >= 10 of
                                                              true ->
                                                                  %% check the block hash in the receipt txn is correct
                                                                  case PoCAbsorbedAtBlockHash == ?MODULE:request_block_hash(Txn) of
                                                                      true ->
                                                                          validate(Txn, Path, LayerData, LayerHashes, OldLedger);
                                                                      false ->
                                                                          blockchain_ledger_v1:delete_context(OldLedger),
                                                                          {error, bad_poc_request_block_hash}
                                                                  end;
                                                              false ->
                                                                  validate(Txn, Path, LayerData, LayerHashes, OldLedger)
                                                          end,
                                                    maybe_log_duration(receipt_validation, StartV),
                                                    case Ret of
                                                        ok ->
                                                            {ok, Channels};
                                                        {error, _}=E -> E
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

    {ok, Channels} = get_channels(Txn, Chain),

    lists:reverse(element(1, lists:foldl(fun({ElementPos, Element}, {Acc, true}) ->
                                                 Challengee = blockchain_poc_path_element_v1:challengee(Element),
                                                 NextElements = lists:sublist(Path, ElementPos+1, Length),
                                                 HasContinued = check_path_continuation(NextElements),

                                                 {PreviousElement, ReceiptChannel, WitnessChannel} =
                                                 case ElementPos of
                                                     1 ->
                                                         {undefined, 0, hd(Channels)};
                                                     _ ->
                                                         {lists:nth(ElementPos - 1, Path), lists:nth(ElementPos - 1, Channels), lists:nth(ElementPos, Channels)}
                                                 end,

                                                 {Val, Continue} = calculate_alpha_beta(HasContinued, Element, PreviousElement, ReceiptChannel, WitnessChannel, Ledger),
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
                           ReceiptChannel :: non_neg_integer(),
                           WitnessChannel :: non_neg_integer(),
                           Ledger :: blockchain_ledger_v1:ledger()) -> {{float(), 0 | 1}, boolean()}.
calculate_alpha_beta(HasContinued, Element, PreviousElement, ReceiptChannel, WitnessChannel, Ledger) ->
    Receipt = valid_receipt(PreviousElement, Element, ReceiptChannel, Ledger),
    Witnesses = valid_witnesses(Element, WitnessChannel, Ledger),
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

    {ok, ChallengeeLoc} = blockchain_ledger_v1:find_gateway_location(Challengee, Ledger),
    ChallengeeParentIndex = h3:parent(ChallengeeLoc, ParentRes),

    case blockchain:config(?poc_version, Ledger) of
        {ok, V} when V >= 9 ->
            Witnesses;
        _ ->
            %% Continue doing the filtering till poc >= 8
            %% Good quality witnesses
            lists:filter(fun(Witness) ->
                                 WitnessPubkeyBin = blockchain_poc_witness_v1:gateway(Witness),
                                 {ok, WitnessGwLoc} = blockchain_ledger_v1:find_gateway_location(WitnessPubkeyBin, Ledger),
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
                         Witnesses)
    end.

%% Iterate over all poc_path elements and for each path element calls a given
%% callback function with reason tagged witnesses and valid receipt.
tagged_path_elements_fold(Fun, Acc0, Txn, Ledger, Chain) ->
    case get_channels(Txn, Chain) of
        {ok, Channels} ->
            Path = ?MODULE:path(Txn),
            lists:foldl(fun({ElementPos, Element}, Acc) ->
                                {PreviousElement, ReceiptChannel, WitnessChannel} =
                                case ElementPos of
                                    1 ->
                                        {undefined, 0, hd(Channels)};
                                    _ ->
                                        {lists:nth(ElementPos - 1, Path), lists:nth(ElementPos - 1, Channels), lists:nth(ElementPos, Channels)}
                                end,

                                FilteredReceipt = valid_receipt(PreviousElement, Element, ReceiptChannel, Ledger),
                                TaggedWitnesses = tagged_witnesses(Element, WitnessChannel, Ledger),

                                Fun(Element, {TaggedWitnesses, FilteredReceipt}, Acc)
                        end, Acc0, lists:zip(lists:seq(1, length(Path)), Path));
        {error, request_block_hash_not_found} -> []
    end.


%% Iterate over all poc_path elements and for each path element calls a given
%% callback function with the valid witnesses and valid receipt.
valid_path_elements_fold(Fun, Acc0, Txn, Ledger, Chain) ->
    Path = ?MODULE:path(Txn),
    {ok, Channels} = get_channels(Txn, Chain),
    lists:foldl(fun({ElementPos, Element}, Acc) ->
                        {PreviousElement, ReceiptChannel, WitnessChannel} =
                            case ElementPos of
                                1 ->
                                    {undefined, 0, hd(Channels)};
                                _ ->
                                    {lists:nth(ElementPos - 1, Path), lists:nth(ElementPos - 1, Channels), lists:nth(ElementPos, Channels)}
                            end,

                        FilteredReceipt = valid_receipt(PreviousElement, Element, ReceiptChannel, Ledger),
                        FilteredWitnesses = valid_witnesses(Element, WitnessChannel, Ledger),

                        Fun(Element, {FilteredWitnesses, FilteredReceipt}, Acc)
                end, Acc0, lists:zip(lists:seq(1, length(Path)), Path)).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
 -spec absorb(txn_poc_receipts(), blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
absorb(Txn, Chain) ->
    LastOnionKeyHash = ?MODULE:onion_key_hash(Txn),
    Challenger = ?MODULE:challenger(Txn),
    Secret = ?MODULE:secret(Txn),
    Ledger = blockchain:ledger(Chain),
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
    POCID = ?MODULE:poc_id(Txn),

    try
        %% get these to make sure we're not replaying.
        {ok, PoCs} = blockchain_ledger_v1:find_poc(LastOnionKeyHash, Ledger),
        {ok, _PoC} = blockchain_ledger_poc_v2:find_valid(PoCs, Challenger, Secret),
        {ok, LastChallenge} = blockchain_ledger_v1:find_gateway_last_challenge(Challenger, Ledger),
        PoCInterval = blockchain_utils:challenge_interval(Ledger),
        case LastChallenge + PoCInterval >= Height of
            false ->
                lager:info("challenge too old ~p ~p", [Challenger, LastChallenge]),
                {error, challenge_too_old};
            true ->
                case blockchain:config(?poc_version, Ledger) of
                    {error, not_found} ->
                        %% Older poc version, don't add witnesses
                        ok;
                    {ok, POCVersion} when POCVersion >= 9 ->
                        %% Add filtered witnesses with poc-v9
                        ok = valid_path_elements_fold(fun(Element, {FilteredWitnesses, FilteredReceipt}, _) ->
                                                              Challengee = blockchain_poc_path_element_v1:challengee(Element),
                                                              case FilteredReceipt of
                                                                  undefined ->
                                                                      ok = blockchain_ledger_v1:insert_witnesses(Challengee, FilteredWitnesses, Ledger);
                                                                  FR ->
                                                                      ok = blockchain_ledger_v1:insert_witnesses(Challengee, FilteredWitnesses ++ [FR], Ledger)
                                                              end
                                                      end, ok, Txn, Ledger, Chain);
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

                case blockchain:config(?poc_version, Ledger) of
                    {ok, V} when V >= 9 ->
                        %% This isn't ideal, but we need to do delta calculation _before_ we delete the poc
                        %% as new calculate_delta calls back into check_is_valid_poc
                        lists:foreach(fun({Gateway, Delta}) ->
                                              blockchain_ledger_v1:update_gateway_score(Gateway, Delta, Ledger)
                                      end,
                                      ?MODULE:deltas(Txn, Chain)),
                        blockchain_ledger_v1:delete_poc(LastOnionKeyHash, Challenger, Ledger);
                    _ ->
                        %% continue doing the old behavior
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
        end
    catch What:Why:Stacktrace ->
              lager:error([{poc_id, POCID}], "poc receipt calculation failed: ~p ~p ~p",
                          [What, Why, Stacktrace]),
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
            lager:error("poc_receipts error find_poc, poc_onion_key_hash: ~p, reason: ~p",
                        [OnionKeyHash, Reason]),
            Error0;
        {ok, PoCs} ->
            case blockchain_ledger_poc_v2:find_valid(PoCs, Challenger, Secret) of
                {error, _}=Error1 ->
                    Error1;
                {ok, _PoC} ->
                    case blockchain_ledger_v1:find_gateway_last_challenge(Challenger, Ledger) of
                        {error, Reason}=Error2 ->
                            lager:warning("poc_receipts error find_gateway_info, challenger: ~p, reason: ~p",
                                        [Challenger, Reason]),
                            Error2;
                        {ok, LastChallenge} ->
                            case blockchain:get_block(LastChallenge, Chain) of
                                {error, Reason}=Error3 ->
                                    lager:warning("poc_receipts error get_block, last_challenge: ~p, reason: ~p",
                                                [LastChallenge, Reason]),
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
    POCID = ?MODULE:poc_id(Txn),
    lager:debug([{poc_id, POCID}], "starting poc receipt validation..."),

    case TxnPathLength == RebuiltPathLength of
        false ->
            HumanTxnPath = [element(2, erl_angry_purple_tiger:animal_name(libp2p_crypto:bin_to_b58(blockchain_poc_path_element_v1:challengee(E)))) || E <- TxnPath],
            HumanRebuiltPath = [element(2, erl_angry_purple_tiger:animal_name(libp2p_crypto:bin_to_b58(A))) || A <- Path],
            lager:warning([{poc_id, POCID}], "TxnPathLength: ~p, RebuiltPathLength: ~p",
                          [TxnPathLength, RebuiltPathLength]),
            lager:warning([{poc_id, POCID}], "TxnPath: ~p", [HumanTxnPath]),
            lager:warning([{poc_id, POCID}], "RebuiltPath: ~p", [HumanRebuiltPath]),
            blockchain_ledger_v1:delete_context(OldLedger),
            {error, path_length_mismatch};
        true ->
            %% Now check whether layers are of equal length
            case TxnPathLength == ZippedLayersLength of
                false ->
                    lager:error([{poc_id, POCID}], "TxnPathLength: ~p, ZippedLayersLength: ~p",
                                [TxnPathLength, ZippedLayersLength]),
                    blockchain_ledger_v1:delete_context(OldLedger),
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
                                                   (blockchain_poc_receipt_v1:is_valid(Receipt, OldLedger) andalso
                                                    blockchain_poc_receipt_v1:gateway(Receipt) == Gateway andalso
                                                    blockchain_poc_receipt_v1:data(Receipt) == LayerDatum andalso
                                                    blockchain_poc_receipt_v1:origin(Receipt) == ExpectedOrigin)
                                               of
                                                   true ->
                                                       %% ok the receipt looks good, check the witnesses
                                                       Witnesses = blockchain_poc_path_element_v1:witnesses(Elem),
                                                       PerHopMaxWitnesses = blockchain_utils:poc_per_hop_max_witnesses(OldLedger),
                                                       case erlang:length(Witnesses) > PerHopMaxWitnesses of
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
                                                               lager:warning([{poc_id, POCID}],
                                                                             "Receipt undefined, ExpectedOrigin: ~p, LayerDatum: ~p, Gateway: ~p",
                                                                             [Receipt, ExpectedOrigin, LayerDatum, Gateway]);
                                                           false ->
                                                               lager:warning([{poc_id, POCID}],
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
                                               lager:error([{poc_id, POCID}], "receipt not in order"),
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

json_type() ->
    <<"poc_receipts_v1">>.

-spec to_json(Txn :: txn_poc_receipts(),
              Opts :: blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Txn, Opts) ->
    PathElems =
    case {lists:keyfind(ledger, 1, Opts), lists:keyfind(chain, 1, Opts)} of
        {{ledger, Ledger}, {chain, Chain}} ->
            case blockchain:config(?poc_version, Ledger) of
                {ok, POCVersion} when POCVersion >= 10 ->
                    FoldedPath =
                        tagged_path_elements_fold(fun(Elem, {TaggedWitnesses, ValidReceipt}, Acc) ->
                                                     ElemOpts = [{tagged_witnesses, TaggedWitnesses},
                                                                 {valid_receipt, ValidReceipt}],
                                                     [{Elem, ElemOpts} | Acc]
                                             end, [], Txn, Ledger, Chain),
                    lists:reverse(FoldedPath);
                _ ->
                    %% Older poc version, don't add validity
                    [{Elem, []} || Elem <- path(Txn)]
            end;
        {_, _} ->
            [{Elem, []} || Elem <- path(Txn)]
    end,
    #{
      type => ?MODULE:json_type(),
      hash => ?BIN_TO_B64(hash(Txn)),
      secret => ?BIN_TO_B64(secret(Txn)),
      onion_key_hash => ?BIN_TO_B64(onion_key_hash(Txn)),
      request_block_hash => ?MAYBE_B64(request_block_hash(Txn)),
      path => [blockchain_poc_path_element_v1:to_json(Elem, ElemOpts) || {Elem, ElemOpts} <- PathElems],
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
                  case blockchain_ledger_v1:find_gateway_location(WitnessGateway, OldLedger) of
                      {error, _} ->
                          false;
                      {ok, _} when Gateway == WitnessGateway ->
                          false;
                      {ok, GWLoc} ->
                          GWLoc /= undefined andalso
                          blockchain_poc_witness_v1:is_valid(Witness, OldLedger) andalso
                          blockchain_poc_witness_v1:packet_hash(Witness) == LayerHash
                  end
          end,
          Witnesses
         )
    of
        true -> ok;
        false -> {error, invalid_witness}
    end.

-spec poc_id(txn_poc_receipts()) -> binary().
poc_id(Txn) ->
    ?BIN_TO_B64(?MODULE:onion_key_hash(Txn)).

vars(Ledger) ->
    blockchain_utils:vars_binary_keys_to_atoms(
      maps:from_list(blockchain_ledger_v1:snapshot_vars(Ledger))).

-spec valid_receipt(PreviousElement :: undefined | blockchain_poc_path_element_v1:poc_element(),
                    Element :: blockchain_poc_path_element_v1:poc_element(),
                    Channel :: non_neg_integer(),
                    Ledger :: blockchain_ledger_v1:ledger()) -> undefined | blockchain_poc_receipt_v1:poc_receipt().
valid_receipt(undefined, _Element, _Channel, _Ledger) ->
    %% first hop in the path, cannot be validated.
    undefined;
valid_receipt(PreviousElement, Element, Channel, Ledger) ->
    case blockchain_poc_path_element_v1:receipt(Element) of
        undefined ->
            %% nothing to validate
            undefined;
        Receipt ->
            DstPubkeyBin = blockchain_poc_path_element_v1:challengee(Element),
            {ok, SourceLoc} = blockchain_ledger_v1:find_gateway_location(
                                blockchain_poc_path_element_v1:challengee(PreviousElement), Ledger),
            {ok, DestinationLoc} = blockchain_ledger_v1:find_gateway_location(DstPubkeyBin, Ledger),
            {ok, ExclusionCells} = blockchain_ledger_v1:config(?poc_v4_exclusion_cells, Ledger),
            {ok, ParentRes} = blockchain_ledger_v1:config(?poc_v4_parent_res, Ledger),
            SourceParentIndex = h3:parent(SourceLoc, ParentRes),
            DestinationParentIndex = h3:parent(DestinationLoc, ParentRes),
            TooFar = case blockchain:config(?poc_distance_limit, Ledger) of
                         {ok, L} ->
                             D = blockchain_utils:distance(SourceLoc, DestinationLoc),
                             D > L;
                         _ ->
                             false
                     end,

            SameRegion = is_same_region(Ledger, SourceLoc, DestinationLoc),

            try h3:grid_distance(SourceParentIndex, DestinationParentIndex) >= ExclusionCells of
                Dist when Dist >= ExclusionCells andalso SameRegion andalso not TooFar ->
                    RSSI = blockchain_poc_receipt_v1:signal(Receipt),
                    SNR = blockchain_poc_receipt_v1:snr(Receipt),
                    Freq = blockchain_poc_receipt_v1:frequency(Receipt),
                    MinRcvSig = min_rcv_sig(Receipt, Ledger, SourceLoc, DstPubkeyBin, DestinationLoc, Freq),
                    case RSSI < MinRcvSig of
                        false ->
                            %% RSSI is impossibly high discard this receipt
                            lager:debug("receipt ~p -> ~p rejected at height ~p for RSSI ~p above FSPL ~p with SNR ~p",
                                          [?TO_ANIMAL_NAME(blockchain_poc_path_element_v1:challengee(PreviousElement)),
                                           ?TO_ANIMAL_NAME(blockchain_poc_path_element_v1:challengee(Element)),
                                           element(2, blockchain_ledger_v1:current_height(Ledger)),
                                           RSSI, MinRcvSig, SNR]),
                            undefined;
                        true ->
                            case blockchain:config(?data_aggregation_version, Ledger) of
                                {ok, 2} ->
                                    case check_rssi_snr(Ledger, RSSI, SNR) of
                                        true ->
                                            case blockchain_poc_receipt_v1:channel(Receipt) == Channel of
                                                true ->
                                                    lager:debug("receipt ok"),
                                                    Receipt;
                                                false ->
                                                    lager:debug("receipt ~p -> ~p rejected at height ~p for channel ~p /= ~p RSSI ~p SNR ~p",
                                                                  [?TO_ANIMAL_NAME(blockchain_poc_path_element_v1:challengee(PreviousElement)),
                                                                   ?TO_ANIMAL_NAME(blockchain_poc_path_element_v1:challengee(Element)),
                                                                   element(2, blockchain_ledger_v1:current_height(Ledger)),
                                                                   blockchain_poc_receipt_v1:channel(Receipt), Channel,
                                                                   RSSI, SNR]),
                                                    undefined
                                            end;
                                        {false, LowerBound} ->
                                            lager:debug("receipt ~p -> ~p rejected at height ~p for RSSI ~p below lower bound ~p with SNR ~p",
                                                          [?TO_ANIMAL_NAME(blockchain_poc_path_element_v1:challengee(PreviousElement)),
                                                           ?TO_ANIMAL_NAME(blockchain_poc_path_element_v1:challengee(Element)),
                                                           element(2, blockchain_ledger_v1:current_height(Ledger)),
                                                           RSSI, LowerBound, SNR]),
                                            undefined
                                    end;
                                _ ->
                                    %% SNR+Freq+Channels not collected, nothing else we can check
                                    Receipt
                            end
                    end;
                _ ->
                    %% too close or too far
                    undefined
            catch
                _:_ ->
                    %% pentagonal distortion
                    undefined
            end
    end.

-spec valid_witnesses(Element :: blockchain_poc_path_element_v1:poc_element(),
                      Channel :: non_neg_integer(),
                      Ledger :: blockchain_ledger_v1:ledger()) -> blockchain_poc_witness_v1:poc_witnesses().
valid_witnesses(Element, Channel, Ledger) ->
    {ok, SourceLoc} = blockchain_ledger_v1:find_gateway_location(
                        blockchain_poc_path_element_v1:challengee(Element), Ledger),

    Witnesses = blockchain_poc_path_element_v1:witnesses(Element),

    lists:filter(fun(Witness) ->
                         WitnessPubkeyBin = blockchain_poc_witness_v1:gateway(Witness),
                         {ok, DestinationLoc} = blockchain_ledger_v1:find_gateway_location(WitnessPubkeyBin, Ledger),
                         {ok, ExclusionCells} = blockchain_ledger_v1:config(?poc_v4_exclusion_cells, Ledger),
                         {ok, ParentRes} = blockchain_ledger_v1:config(?poc_v4_parent_res, Ledger),
                         SourceParentIndex = h3:parent(SourceLoc, ParentRes),
                         DestinationParentIndex = h3:parent(DestinationLoc, ParentRes),
                         TooFar = case blockchain:config(?poc_distance_limit, Ledger) of
                                      {ok, L} ->
                                          D = blockchain_utils:distance(SourceLoc, DestinationLoc),
                                          D > L;
                                      _ ->
                                          false
                                  end,

                         SameRegion = is_same_region(Ledger, SourceLoc, DestinationLoc),

                         try h3:grid_distance(SourceParentIndex, DestinationParentIndex) of
                             Dist when Dist >= ExclusionCells andalso SameRegion andalso not TooFar ->
                                 RSSI = blockchain_poc_witness_v1:signal(Witness),
                                 SNR = blockchain_poc_witness_v1:snr(Witness),
                                 Freq = blockchain_poc_witness_v1:frequency(Witness),
                                 Receipt = blockchain_poc_path_element_v1:receipt(Element),
                                 MinRcvSig = min_rcv_sig(Receipt, Ledger, SourceLoc, WitnessPubkeyBin, DestinationLoc, Freq),

                                 case RSSI < MinRcvSig of
                                     false ->
                                         %% RSSI is impossibly high discard this witness
                                         lager:debug("witness ~p -> ~p rejected at height ~p for RSSI ~p above FSPL ~p with SNR ~p",
                                                       [?TO_ANIMAL_NAME(blockchain_poc_path_element_v1:challengee(Element)),
                                                        ?TO_ANIMAL_NAME(blockchain_poc_witness_v1:gateway(Witness)),
                                                        element(2, blockchain_ledger_v1:current_height(Ledger)),
                                                        RSSI, MinRcvSig, SNR]),
                                         false;
                                     true ->
                                         case blockchain:config(?data_aggregation_version, Ledger) of
                                             {ok, 2} ->
                                                 case check_rssi_snr(Ledger, RSSI, SNR) of
                                                     true ->
                                                         case blockchain_poc_witness_v1:channel(Witness) == Channel of
                                                             true ->
                                                                 lager:debug("witness ok"),
                                                                 true;
                                                             false ->
                                                                 lager:debug("witness ~p -> ~p rejected at height ~p for channel ~p /= ~p RSSI ~p SNR ~p",
                                                                               [?TO_ANIMAL_NAME(blockchain_poc_path_element_v1:challengee(Element)),
                                                                                ?TO_ANIMAL_NAME(blockchain_poc_witness_v1:gateway(Witness)),
                                                                                element(2, blockchain_ledger_v1:current_height(Ledger)),
                                                                                blockchain_poc_witness_v1:channel(Witness), Channel,
                                                                                RSSI, SNR]),
                                                                 false
                                                         end;
                                                     {false, LowerBound} ->
                                                         lager:debug("witness ~p -> ~p rejected at height ~p for RSSI ~p below lower bound ~p with SNR ~p",
                                                                       [?TO_ANIMAL_NAME(blockchain_poc_path_element_v1:challengee(Element)),
                                                                        ?TO_ANIMAL_NAME(blockchain_poc_witness_v1:gateway(Witness)),
                                                                        element(2, blockchain_ledger_v1:current_height(Ledger)),
                                                                        RSSI, LowerBound, SNR]),
                                                         false
                                                 end;
                                             _ ->
                                                 %% SNR+Freq+Channels not collected, nothing else we can check
                                                 true
                                         end
                                 end;
                             _ ->
                                 %% too close or too far
                                 false
                         catch _:_ ->
                                   %% pentagonal distortion
                                   false
                         end
                 end, Witnesses).

-spec is_same_region(
    Ledger :: blockchain_ledger_v1:ledger(),
    SourceLoc :: h3:h3_index(),
    DstLoc :: h3:h3_index()
) -> boolean().
is_same_region(Ledger, SourceLoc, DstLoc) ->
    case blockchain_region_v1:region(SourceLoc, Ledger) of
        {ok, R1} ->
            case blockchain_region_v1:region(DstLoc, Ledger) of
                {ok, R1} ->
                    %% same regions
                    true;
                {ok, _} ->
                    %% different regions
                    false;
                _ ->
                    %% var not set, true
                    true
            end;
        _ ->
            %% var not set, true
            true
    end.

%% This is an ETL specific function, it does NOT filter witnesses. It adds a tag to each witness specifying a reason why
%% a witness was considered invalid
-spec tagged_witnesses(Element :: blockchain_poc_path_element_v1:poc_element(),
                       Channel :: non_neg_integer(),
                       Ledger :: blockchain_ledger_v1:ledger()) -> tagged_witnesses().
tagged_witnesses(Element, Channel, Ledger) ->
    {ok, Source} = blockchain_gateway_cache:get(blockchain_poc_path_element_v1:challengee(Element), Ledger),

    %% foldl will re-reverse
    Witnesses = lists:reverse(blockchain_poc_path_element_v1:witnesses(Element)),

    lists:foldl(fun(Witness, Acc) ->
                         DstPubkeyBin = blockchain_poc_witness_v1:gateway(Witness),
                         {ok, Destination} = blockchain_gateway_cache:get(DstPubkeyBin, Ledger),
                         SourceLoc = blockchain_ledger_gateway_v2:location(Source),
                         DestinationLoc = blockchain_ledger_gateway_v2:location(Destination),
                         {ok, ExclusionCells} = blockchain_ledger_v1:config(?poc_v4_exclusion_cells, Ledger),
                         {ok, ParentRes} = blockchain_ledger_v1:config(?poc_v4_parent_res, Ledger),
                         SourceParentIndex = h3:parent(SourceLoc, ParentRes),
                         DestinationParentIndex = h3:parent(DestinationLoc, ParentRes),
                         TooFar = case blockchain:config(?poc_distance_limit, Ledger) of
                                      {ok, L} ->
                                          D = blockchain_utils:distance(SourceLoc, DestinationLoc),
                                          D > L;
                                      _ ->
                                          false
                                  end,

                         SameRegion = is_same_region(Ledger, SourceLoc, DestinationLoc),

                         try h3:grid_distance(SourceParentIndex, DestinationParentIndex) of
                             Dist when Dist >= ExclusionCells andalso SameRegion andalso not TooFar ->
                                 RSSI = blockchain_poc_witness_v1:signal(Witness),
                                 SNR = blockchain_poc_witness_v1:snr(Witness),
                                 Freq = blockchain_poc_witness_v1:frequency(Witness),
                                 Receipt = blockchain_poc_path_element_v1:receipt(Element),
                                 MinRcvSig = min_rcv_sig(Receipt, Ledger, SourceLoc, DstPubkeyBin, DestinationLoc, Freq),

                                 case RSSI < MinRcvSig of
                                     false ->
                                         %% RSSI is impossibly high discard this witness
                                         lager:debug("witness ~p -> ~p rejected at height ~p for RSSI ~p above FSPL ~p with SNR ~p",
                                                       [?TO_ANIMAL_NAME(blockchain_poc_path_element_v1:challengee(Element)),
                                                        ?TO_ANIMAL_NAME(blockchain_poc_witness_v1:gateway(Witness)),
                                                        element(2, blockchain_ledger_v1:current_height(Ledger)),
                                                        RSSI, MinRcvSig, SNR]),
                                         [{false, <<"witness_rssi_too_high">>, Witness} | Acc];
                                     true ->
                                         case blockchain:config(?data_aggregation_version, Ledger) of
                                             {ok, 2} ->
                                                 case check_rssi_snr(Ledger, RSSI, SNR) of
                                                     true ->
                                                         case blockchain_poc_witness_v1:channel(Witness) == Channel of
                                                             true ->
                                                                 lager:debug("witness ok"),
                                                                 [{true, <<"ok">>, Witness} | Acc];
                                                             false ->
                                                                 lager:debug("witness ~p -> ~p rejected at height ~p for channel ~p /= ~p RSSI ~p SNR ~p",
                                                                               [?TO_ANIMAL_NAME(blockchain_poc_path_element_v1:challengee(Element)),
                                                                                ?TO_ANIMAL_NAME(blockchain_poc_witness_v1:gateway(Witness)),
                                                                                element(2, blockchain_ledger_v1:current_height(Ledger)),
                                                                                blockchain_poc_witness_v1:channel(Witness), Channel,
                                                                                RSSI, SNR]),
                                                                 [{false, <<"witness_on_incorrect_channel">>, Witness} | Acc]
                                                         end;
                                                     {false, LowerBound} ->
                                                         lager:debug("witness ~p -> ~p rejected at height ~p for RSSI ~p below lower bound ~p with SNR ~p",
                                                                       [?TO_ANIMAL_NAME(blockchain_poc_path_element_v1:challengee(Element)),
                                                                        ?TO_ANIMAL_NAME(blockchain_poc_witness_v1:gateway(Witness)),
                                                                        element(2, blockchain_ledger_v1:current_height(Ledger)),
                                                                        RSSI, LowerBound, SNR]),
                                                         [{false, <<"witness_rssi_below_lower_bound">>, Witness} | Acc]
                                                 end;
                                             _ ->
                                                 %% SNR+Freq+Channels not collected, nothing else we can check
                                                 [{true, <<"insufficient_data">>, Witness} | Acc]
                                         end
                                 end;
                             _ ->
                                 %% too close or too far
                                 [{false, <<"witness_too_close">>, Witness} | Acc]
                         catch _:_ ->
                                   %% pentagonal distortion
                                   [{false, <<"pentagonal_distortion">>, Witness} | Acc]
                         end
                 end, [], Witnesses).

scale_unknown_snr(UnknownSNR) ->
    Diffs = lists:map(fun(K) -> {math:sqrt(math:pow(UnknownSNR - K, 2.0)), K} end, maps:keys(?SNR_CURVE)),
    {ScaleFactor, Key} = hd(lists:sort(Diffs)),
    {Low, High} = maps:get(Key, ?SNR_CURVE),
    {Low + (Low * ScaleFactor), High + (High * ScaleFactor)}.

check_rssi_snr(Ledger, RSSI, SNR) ->
    case blockchain:config(?poc_version, Ledger) of
        {ok, POCVersion} when POCVersion >= 11 ->
            %% no checks
            true;
        _ ->
            {LowerBound, _} = calculate_rssi_bounds_from_snr(SNR),
            case RSSI >= LowerBound of
                true ->
                    true;
                false ->
                    {false, LowerBound}
            end
    end.

calculate_rssi_bounds_from_snr(SNR) ->
        %% keef says rounding up hurts the least
        CeilSNR = ceil(SNR),
        case maps:get(CeilSNR, ?SNR_CURVE, undefined) of
            undefined ->
                scale_unknown_snr(CeilSNR);
            V ->
                V
        end.

-spec get_channels(Txn :: txn_poc_receipts(),
                   Chain :: blockchain:blockchain()) -> {ok, [non_neg_integer()]} | {error, any()}.
get_channels(Txn, Chain) ->
    Challenger = ?MODULE:challenger(Txn),
    Path = ?MODULE:path(Txn),
    Secret = ?MODULE:secret(Txn),
    PathLength = length(Path),

    OnionKeyHash = ?MODULE:onion_key_hash(Txn),

    BlockHash = case blockchain:config(?poc_version, blockchain:ledger(Chain)) of
        {ok, POCVer} when POCVer >= 10 ->
            ?MODULE:request_block_hash(Txn);
        _ ->
            %% Retry by walking the chain and attempt to find the last challenge block
            %% Note that this does not scale at all and should not be used
            RequestFilter = fun(T) ->
                                    blockchain_txn:type(T) == blockchain_txn_poc_request_v1 andalso
                                    blockchain_txn_poc_request_v1:onion_key_hash(T) == OnionKeyHash
                            end,
            {ok, CurrentHeight} = blockchain_ledger_v1:current_height(blockchain:ledger(Chain)),
            {ok, Head} = blockchain:get_block(CurrentHeight, Chain),
            blockchain:fold_chain(fun(Block, undefined) ->
                                                      case blockchain_utils:find_txn(Block, RequestFilter) of
                                                          [_T] ->
                                                              blockchain_block:hash_block(Block);
                                                          _ ->
                                                              undefined
                                                      end;
                                                 (_, _Hash) -> return
                                              end, undefined, Head, Chain)
    end,

    case BlockHash of
        <<>> ->
            {error, request_block_hash_not_found};
        undefined ->
            {error, request_block_hash_not_found};
        BH ->
            Entropy1 = <<Secret/binary, BH/binary, Challenger/binary>>,
            [_ | LayerData1] = blockchain_txn_poc_receipts_v1:create_secret_hash(Entropy1, PathLength+1),
            Channels1 = lists:map(fun(Layer) ->
                                          <<IntData:16/integer-unsigned-little>> = Layer,
                                          IntData rem 8
                                  end, LayerData1),
            {ok, Channels1}
    end.

min_rcv_sig(Receipt, Ledger, SourceLoc, DstPubkeyBin, DestinationLoc, Freq) ->
    case blockchain:config(?poc_version, Ledger) of
        {ok, POCVersion} when POCVersion >= 11 ->
            TxPower = maybe_tx_power_from_receipt(Receipt, SourceLoc, Ledger),
            SrcPubkeyBin = blockchain_poc_receipt_v1:gateway(Receipt),
            {ok, DstGW} = blockchain_ledger_v1:find_gateway_info(DstPubkeyBin, Ledger),
            {ok, SrcGW} = blockchain_ledger_v1:find_gateway_info(SrcPubkeyBin, Ledger),
            GT = blockchain_ledger_gateway_v2:gain(SrcGW),
            GR = blockchain_ledger_gateway_v2:gain(DstGW),
            %% {ok, Loss} = blockchain:config(?fspl_loss, Ledger),
            FSPL = blockchain_utils:free_space_path_loss(
                     SourceLoc,
                     DestinationLoc,
                     Freq,
                     GT,
                     GR
                    ),
            blockchain_utils:min_rcv_sig(FSPL, TxPower);
        _ ->
            blockchain_utils:min_rcv_sig(
                blockchain_utils:free_space_path_loss(SourceLoc, DestinationLoc, Freq)
            )
    end.

maybe_tx_power_from_receipt(undefined, SourceLoc, Ledger) ->
    {ok, Region} = blockchain_region_v1:region(SourceLoc, Ledger),
    {ok, RegionParams} = blockchain_region_params_v1:params_for_region(Region, Ledger),
    %% NOTE: all region params have the same max_eirp afaict, just take one
    %% TODO: maybe look at the freq of the source and match max_eirp if they ever differ?
    Param = hd(blockchain_region_params_v1:region_params(RegionParams)),
    blockchain_region_param_v1:max_eirp(Param);
maybe_tx_power_from_receipt(Receipt, _SourceLoc, _Ledger) ->
    blockchain_poc_receipt_v1:tx_power(Receipt).

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
    Challenger = <<"challenger">>,
    Secret = <<"secret">>,
    OnionKeyHash = <<"onion_key_hash">>,
    BlockHash = <<"blockhash">>,

    Receipt = blockchain_poc_receipt_v1:new(<<"r">>, 10, 10, <<"data">>, p2p, 1.2, 915.2, 2, <<"dr">>),

    W1 = blockchain_poc_witness_v1:new(<<"w1">>, 10, 10, <<"ph">>, 1.2, 915.2, 2, <<"dr">>),
    W2 = blockchain_poc_witness_v1:new(<<"w2">>, 10, 10, <<"ph">>, 1.2, 915.2, 2, <<"dr">>),
    Witnesses = [W1, W2],

    P1 = blockchain_poc_path_element_v1:new(<<"c1">>, Receipt, Witnesses),
    P2 = blockchain_poc_path_element_v1:new(<<"c2">>, undefined, []),
    P3 = blockchain_poc_path_element_v1:new(<<"c3">>, undefined, []),
    P4 = blockchain_poc_path_element_v1:new(<<"c4">>, undefined, []),
    P5 = blockchain_poc_path_element_v1:new(<<"c5">>, undefined, []),
    Path1 = [P1, P2, P3, P4, P5],
    Txn1 = new(Challenger, Secret, OnionKeyHash, BlockHash, Path1),

    Deltas1 = deltas(Txn1),
    ?assertEqual(2, length(Deltas1)),
    ?assertEqual({0.9, 0}, proplists:get_value(<<"c1">>, Deltas1)),
    ?assertEqual({0, 1}, proplists:get_value(<<"c2">>, Deltas1)),

    P1Prime = blockchain_poc_path_element_v1:new(<<"c1">>, Receipt, []),
    P2Prime = blockchain_poc_path_element_v1:new(<<"c2">>, undefined, []),
    P3Prime = blockchain_poc_path_element_v1:new(<<"c3">>, undefined, []),
    P4Prime = blockchain_poc_path_element_v1:new(<<"c3">>, undefined, []),
    P5Prime = blockchain_poc_path_element_v1:new(<<"c3">>, undefined, []),
    Path2 = [P1Prime, P2Prime, P3Prime, P4Prime, P5Prime],
    Txn2 = new(Challenger, Secret, OnionKeyHash, BlockHash, Path2),

    Deltas2 = deltas(Txn2),
    ?assertEqual(1, length(Deltas2)),
    ?assertEqual({0, 0}, proplists:get_value(<<"c1">>, Deltas2)),
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
                                   <<"gg">>, <<"blockhash">>},

    Deltas = deltas(Txn),
    ?assertEqual(4, length(Deltas)),
    SecondDeltas = proplists:get_all_values(<<"second">>, Deltas),
    ?assertEqual(2, length(SecondDeltas)),
    {SecondAlphas, _} = lists:unzip(SecondDeltas),
    ?assert(lists:sum(SecondAlphas) > 1),
    ok.

to_json_test() ->
    Challenger = <<"challenger">>,
    Secret = <<"secret">>,
    OnionKeyHash = <<"onion_key_hash">>,
    BlockHash = <<"blockhash">>,

    Receipt = blockchain_poc_receipt_v1:new(<<"r">>, 10, 10, <<"data">>, p2p, 1.2, 915.2, 2, <<"dr">>),

    W1 = blockchain_poc_witness_v1:new(<<"w1">>, 10, 10, <<"ph">>, 1.2, 915.2, 2, <<"dr">>),
    W2 = blockchain_poc_witness_v1:new(<<"w2">>, 10, 10, <<"ph">>, 1.2, 915.2, 2, <<"dr">>),
    Witnesses = [W1, W2],

    P1 = blockchain_poc_path_element_v1:new(<<"c1">>, Receipt, Witnesses),
    P2 = blockchain_poc_path_element_v1:new(<<"c2">>, Receipt, Witnesses),
    P3 = blockchain_poc_path_element_v1:new(<<"c3">>, Receipt, Witnesses),
    Path = [P1, P2, P3],

    Txn = new(Challenger, Secret, OnionKeyHash, BlockHash, Path),
    Json = to_json(Txn, []),

    ?assert(lists:all(fun(K) -> maps:is_key(K, Json) end,
                      [type, hash, secret, onion_key_hash, request_block_hash, path, fee, challenger])).


-endif.

