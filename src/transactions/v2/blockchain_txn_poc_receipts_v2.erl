%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Proof of Coverage Receipts ==
%%%-------------------------------------------------------------------
-module(blockchain_txn_poc_receipts_v2).

-behavior(blockchain_txn).
-behavior(blockchain_json).

-include("blockchain.hrl").
-include("blockchain_json.hrl").
-include("blockchain_vars.hrl").
-include("blockchain_utils.hrl").
-include_lib("helium_proto/include/blockchain_txn_poc_receipts_v2_pb.hrl").
-include_lib("public_key/include/public_key.hrl").

-export([
    new/5,
    hash/1,
    onion_key_hash/1,
    challenger/1,
    secret/1,
    path/1,
    fee/1,
    fee_payer/2,
    block_hash/1,
    signature/1,
    sign/2,
    is_valid/2,
    absorb/2,
    create_secret_hash/2,
    connections/1,
    check_path_continuation/1,
    print/1,
    json_type/0,
    to_json/2,
    poc_id/1,
    good_quality_witnesses/2,
    valid_witnesses/3, valid_witnesses/4,
    tagged_witnesses/3,
    get_channels/2, get_channels/4, get_channels/5,
    get_path/9
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type txn_poc_receipts() :: #blockchain_txn_poc_receipts_v2_pb{}.
-type tagged_witnesses() :: [{IsValid :: boolean(), InvalidReason :: binary(), Witness :: blockchain_poc_witness_v1:witness()}].

-export_type([txn_poc_receipts/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(libp2p_crypto:pubkey_bin(), binary(), binary(),
          blockchain_poc_path_element_v1:path(), binary()) -> txn_poc_receipts().
new(Challenger, Secret, OnionKeyHash, Path, BlockHash) ->
    #blockchain_txn_poc_receipts_v2_pb{
        challenger=Challenger,
        secret=Secret,
        onion_key_hash=OnionKeyHash,
        path=Path,
        fee=0,
        signature = <<>>,
        block_hash = BlockHash
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
-spec path(txn_poc_receipts()) -> blockchain_poc_path_element_v1:path().
path(Txn) ->
    Txn#blockchain_txn_poc_receipts_v2_pb.path.

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

block_hash(Txn) ->
    Txn#blockchain_txn_poc_receipts_v2_pb.block_hash.

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
-spec is_valid(txn_poc_receipts(), blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Challenger = ?MODULE:challenger(Txn),
    Signature = ?MODULE:signature(Txn),
    PubKey = libp2p_crypto:bin_to_pubkey(Challenger),
    BaseTxn = Txn#blockchain_txn_poc_receipts_v2_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_poc_receipts_v2_pb:encode_msg(BaseTxn),
    {ok, POCVersion} = blockchain:config(?poc_version, Ledger),
    case libp2p_crypto:verify(EncodedTxn, Signature, PubKey) of
        false ->
            {error, bad_signature};
        true ->
            %% check the challenger is actually a validator and it exists
            case blockchain_ledger_v1:get_validator(Challenger, Ledger) of
                {error, _Reason}=Error ->
                    Error;
                {ok, _ChallengerInfo} ->
                    case ?MODULE:path(Txn) =:= [] of
                        true ->
                            {error, empty_path};
                        false ->
                            case check_is_valid_poc(POCVersion, Txn, Chain) of
                                {ok, Channels} ->
                                    lager:debug("POCID: ~p, validated ok with reported channels: ~p",
                                                [poc_id(Txn), Channels]),
                                    ok;
                                Error -> Error
                            end
                    end
            end
    end.


-spec check_is_valid_poc(POCVersion :: pos_integer(),
                         Txn :: txn_poc_receipts(),
                         Chain :: blockchain:blockchain()) -> {ok, [non_neg_integer(), ...]} | {error, any()}.
check_is_valid_poc(POCVersion, Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Challenger = ?MODULE:challenger(Txn),
    POCOnionKeyHash = ?MODULE:onion_key_hash(Txn),
    POCID = ?MODULE:poc_id(Txn),
    StartPre = maybe_start_duration(),
    case blockchain_ledger_v1:find_public_poc(POCOnionKeyHash, Ledger) of
        {error, Reason}=Error ->
            lager:warning([{poc_id, POCID}],
                          "poc_receipts error find_public_poc, poc_onion_key_hash: ~p, reason: ~p",
                          [POCOnionKeyHash, Reason]),
            Error;
        {ok, PoC} ->
            Secret = ?MODULE:secret(Txn),
            Keys = libp2p_crypto:keys_from_bin(Secret),
            case verify_poc_details(Txn, PoC, Keys) of
                {error, _Reason} = Error ->
                    lager:debug("invalid poc ~p. Reason ~p", [POCOnionKeyHash, _Reason]),
                    Error;
                ok ->
                    PrePocBlockHeight = blockchain_ledger_poc_v3:start_height(PoC),
                    case blockchain:get_block_info(PrePocBlockHeight, Chain) of
                        {error, Reason}=Error ->
                            lager:warning([{poc_id, POCID}],
                                          "poc_receipts error get_block, last_challenge: ~p, reason: ~p",
                                          [PrePocBlockHeight, Reason]),
                            Error;
                        {ok, #block_info_v2{height = BlockHeight,
                                         time = BlockTime}} ->
                            PrePoCBlockHash = blockchain_ledger_poc_v3:block_hash(PoC),
                            StartLA = maybe_log_duration(prelude, StartPre),
                            {ok, OldLedger} = blockchain:ledger_at(BlockHeight, Chain),
                            StartFT = maybe_log_duration(ledger_at, StartLA),
                            Vars = vars(OldLedger),
                            Entropy = <<POCOnionKeyHash/binary, PrePoCBlockHash/binary>>,
                            {Path, StartP} = ?MODULE:get_path(POCVersion, Challenger, BlockTime, Entropy, Keys, Vars, OldLedger, Ledger, StartFT),
                            N = erlang:length(Path),
                            [<<IV:16/integer-unsigned-little, _/binary>> | LayerData] = blockchain_txn_poc_receipts_v2:create_secret_hash(Entropy, N+1),
                            OnionList = lists:zip([libp2p_crypto:bin_to_pubkey(P) || P <- Path], LayerData),
                            {_Onion, Layers} = case blockchain:config(?poc_typo_fixes, Ledger) of
                                                   {ok, true} ->
                                                       blockchain_poc_packet_v2:build(Keys, IV, OnionList);
                                                   _ ->
                                                       blockchain_poc_packet_v2:build(Keys, IV, OnionList)
                                               end,
                            %% no witness will exist with the first layer hash
                            [_|LayerHashes] = [crypto:hash(sha256, L) || L <- Layers],
                            StartV = maybe_log_duration(packet_construction, StartP),
                            Channels = ?MODULE:get_channels(POCVersion, OldLedger, Path, LayerData, no_prefetch),
                            %% %% run validations
                            Ret = validate(POCVersion, Txn, Path, LayerData, LayerHashes, OldLedger),
                            maybe_log_duration(receipt_validation, StartV),
                            case Ret of
                                ok ->
                                    {ok, Channels};
                                {error, _}=E -> E
                            end
                    end
            end
    end.

get_path(_POCVersion, Challenger, BlockTime, Entropy, Keys, Vars, OldLedger, Ledger, StartT) ->
    %% Targeting phase
    %% Find the original target
    #{public := _OnionCompactKey, secret := {ecc_compact, POCPrivKey}} = Keys,
    #'ECPrivateKey'{privateKey = PrivKeyBin} = POCPrivKey,
    POCPrivKeyHash = crypto:hash(sha256, PrivKeyBin),
    ZoneRandState = blockchain_utils:rand_state(Entropy),
    InitTargetRandState = blockchain_utils:rand_state(POCPrivKeyHash),
    {ok, TargetV} = TargetResp = blockchain:config(?poc_targeting_version, Ledger),
    TargetMod = blockchain_utils:target_v_to_mod(TargetResp),
    %% if v6 targeting or newer in use then use the correct ledger for pathing
    %% this addresses an issue whereby the current ledger was in use when
    %% identifying the target rather than the ledger from the point the
    %% poc was initialized
    PathingLedger =
        case TargetV of
            N when N >= 6 -> OldLedger;
            _ -> Ledger
        end,
    {ok, {Target, TargetRandState}} =  TargetMod:target(Challenger, InitTargetRandState, ZoneRandState, PathingLedger, Vars),
    %% Path building phase
    StartB = maybe_log_duration(target, StartT),
    RetB = blockchain_poc_path_v4:build(Target, TargetRandState, OldLedger, BlockTime, Vars),
    EndT = maybe_log_duration(build, StartB),
    {RetB, EndT}.

%% TODO: I'm not sure that this is actually faster than checking the time, but I suspect that it'll
%% be more lock-friendly?
maybe_start_duration() ->
    case application:get_env(blockchain, log_validation_times, false) of
        true ->
            erlang:monotonic_time(microsecond);
        _ -> 0
    end.

maybe_log_duration(Type, Start) ->
    case application:get_env(blockchain, log_validation_times, false) of
        true ->
            End = erlang:monotonic_time(microsecond),
            lager:info("~p took ~p usec", [Type, End - Start]),
            End;
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


-spec poc_particpants(Txn :: txn_poc_receipts(),
                      Chain :: blockchain:blockchain()) -> [libp2p_crypto:pubkey_bin()].
poc_particpants(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Path = blockchain_txn_poc_receipts_v2:path(Txn),
    Length = length(Path),
    try get_channels(Txn, Chain) of
        {ok, Channels} ->
            Participants =
                lists:foldl(
                    fun
                        ({ElementPos, Element}, Acc) ->
                            Challengee = blockchain_poc_path_element_v1:challengee(Element),
                            {_PreviousElement, _ReceiptChannel, WitnessChannel} =
                                case ElementPos of
                                    1 ->
                                        {undefined, 0, hd(Channels)};
                                    _ ->
                                        {lists:nth(ElementPos - 1, Path), lists:nth(ElementPos - 1, Channels), lists:nth(ElementPos, Channels)}
                                end,
                            Witnesses = case blockchain:config(?poc_receipt_witness_validation, Ledger) of
                                            {ok, false} ->
                                                UnvalidatedWitnesses = lists:reverse(blockchain_poc_path_element_v1:witnesses(Element)),
                                                [W#blockchain_poc_witness_v1_pb.gateway || W <- UnvalidatedWitnesses];
                                            _ ->
                                                valid_witness_addrs(Element, WitnessChannel, Ledger)
                                        end,
                            %% only include the challengee in the poc participants list
                            %% if we have received a receipt
                            ElemParticipants =
                                case blockchain_poc_path_element_v1:receipt(Element) of
                                    undefined -> Witnesses;
                                    _R -> [Challengee | Witnesses]
                                end,
                            [ElemParticipants | Acc];
                        (_, Acc) ->
                            Acc
                    end,
                    [],
                    lists:zip(lists:seq(1, Length), Path)
                ),
            lists:usort(lists:flatten(Participants))
    catch
        _:_ ->
            []
    end.

-spec check_path_continuation(Elements :: [blockchain_poc_path_element_v1:poc_element()]) -> boolean().
check_path_continuation(Elements) ->
    lists:any(fun(E) ->
                      blockchain_poc_path_element_v1:receipt(E) /= undefined orelse
                      blockchain_poc_path_element_v1:witnesses(E) /= []
              end,
              Elements).


-spec good_quality_witnesses(Element :: blockchain_poc_path_element_v1:poc_element(),
                             Ledger :: blockchain_ledger_v1:ledger()) -> [blockchain_poc_witness_v1:poc_witness()].
good_quality_witnesses(Element, _Ledger) ->
    %% NOTE: This function is now just a stub and doesn't really do anything
    %% But it's being used in rewards_v1 and rewards_v2, so keeping it for compatibility.
    Witnesses = blockchain_poc_path_element_v1:witnesses(Element),
    Witnesses.

%% Iterate over all poc_path elements and for each path element calls a given
%% callback function with reason tagged witnesses and valid receipt.
tagged_path_elements_fold(Fun, Acc0, Txn, Ledger, Chain) ->
    try get_channels(Txn, Chain) of
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

                               %% if either crashes, the whole thing is invalid from a rewards perspective
                               {FilteredReceipt, TaggedWitnesses} =
                               try {valid_receipt(PreviousElement, Element, ReceiptChannel, Ledger),
                                    tagged_witnesses(Element, WitnessChannel, Ledger)} of
                                       Res -> Res
                               catch _:_ ->
                                             Witnesses = lists:reverse(blockchain_poc_path_element_v1:witnesses(Element)),
                                             {undefined, [{false, <<"tagged_witnesses_crashed">>, Witness} || Witness <- Witnesses]}
                               end,

                                Fun(Element, {TaggedWitnesses, FilteredReceipt}, Acc)
                        end, Acc0, lists:zip(lists:seq(1, length(Path)), Path));
        {error, request_block_hash_not_found} -> []
    catch
        throw:{error,{region_var_not_set,Region}} ->
            Path = ?MODULE:path(Txn),
            lists:foldl(fun({_ElementPos, Element}, Acc) ->
                                Witnesses = lists:reverse(blockchain_poc_path_element_v1:witnesses(Element)),
                                Fun(Element, {[{false, list_to_binary(io_lib:format("missing_region_parameters_for_~p", [Region])), Witness} || Witness <- Witnesses], undefined}, Acc)
                        end, Acc0, lists:zip(lists:seq(1, length(Path)), Path));
        throw:{error,{unknown_region, UnknownH3}} ->
            Path = ?MODULE:path(Txn),
            lists:foldl(fun({_ElementPos, Element}, Acc) ->
                                Witnesses = lists:reverse(blockchain_poc_path_element_v1:witnesses(Element)),
                                Fun(Element, {lists:map(fun(Witness) -> {false, list_to_binary(io_lib:format("challengee_region_unknown_~p", [UnknownH3])), Witness} end, Witnesses) , undefined}, Acc)
                        end, Acc0, lists:zip(lists:seq(1, length(Path)), Path));
        error:{badmatch, {error, {not_set, Region}}} ->
            Path = ?MODULE:path(Txn),
            lists:foldl(fun({_ElementPos, Element}, Acc) ->
                                Witnesses = lists:reverse(blockchain_poc_path_element_v1:witnesses(Element)),
                                Fun(Element, {[{false, list_to_binary(io_lib:format("missing_region_parameters_for_~p", [Region])), Witness} || Witness <- Witnesses], undefined}, Acc)
                        end, Acc0, lists:zip(lists:seq(1, length(Path)), Path));
        error:{badmatch, {error, regulatory_regions_not_set}} ->
            Path = ?MODULE:path(Txn),
            lists:foldl(fun({_ElementPos, Element}, Acc) ->
                                Witnesses = lists:reverse(blockchain_poc_path_element_v1:witnesses(Element)),
                                Fun(Element, {[{false, <<"regulatory_regions_not_set">>, Witness} || Witness <- Witnesses], undefined}, Acc)
                        end, Acc0, lists:zip(lists:seq(1, length(Path)), Path))
    end.


%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
 -spec absorb(txn_poc_receipts(), blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    {ok, POCVersion} = blockchain:config(?poc_version, Ledger),
    absorb(POCVersion, Txn, Chain).

 -spec absorb(pos_integer(), txn_poc_receipts(), blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
absorb(_POCVersion, Txn, Chain) ->
    OnionKeyHash = ?MODULE:onion_key_hash(Txn),
    Challenger = ?MODULE:challenger(Txn),
    BlockHash = ?MODULE:block_hash(Txn),
    Ledger = blockchain:ledger(Chain),
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
    POCID = ?MODULE:poc_id(Txn),
    try
        %% get these to make sure we're not replaying.
        PoC = case blockchain_ledger_v1:find_public_poc(OnionKeyHash, Ledger) of
                   {ok, Ps} ->
                       Ps;
                   {error, not_found} ->
                       lager:warning("potential replay: ~p not found", [OnionKeyHash]),
                       throw(replay)
               end,
        case blockchain_ledger_poc_v3:verify(PoC, Challenger, BlockHash) of
            false ->
                {error, invalid_poc};
            true ->
                %% maybe update the last activity field for all challengees and GWs
                %% participating in the POC
                case blockchain:config(?poc_activity_filter_enabled, Ledger) of
                    {ok, true} ->
                        Participants = poc_particpants(Txn, Chain),
                        lager:debug("receipt txn poc participants: ~p", [Participants]),
                        [update_participant_gateway(GWAddr, Height, Ledger) || GWAddr <- Participants];
                    _ ->
                        ok
                end,
                ok = blockchain_ledger_v1:delete_public_poc(OnionKeyHash, Ledger)
        end
    catch throw:Reason ->
            {error, Reason};
          What:Why:Stacktrace ->
            lager:error([{poc_id, POCID}], "poc receipt calculation failed: ~p ~p ~p",
                        [What, Why, Stacktrace]),
            {error, state_missing}
    end.

update_participant_gateway(GWAddr, Height, Ledger) ->
    case blockchain_ledger_v1:find_gateway_info(GWAddr, Ledger) of
        {error, _} ->
            {error, no_active_gateway};
        {ok, Gw0} ->
            lager:debug("updating last activity with ~p for gateway ~p", [Height, GWAddr]),
            Gw1 = blockchain_ledger_gateway_v2:last_poc_challenge(Height+1, Gw0),
            ok = blockchain_ledger_v1:update_gateway(Gw0, Gw1, GWAddr, Ledger)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Insert witnesses for gateways in the path into the ledger
%% @end
%%--------------------------------------------------------------------
%%-spec insert_witnesses(Path :: blockchain_poc_path_element_v1:path(),
%%                       LowerTimeBound :: non_neg_integer(),
%%                       UpperTimeBound :: non_neg_integer(),
%%                       Ledger :: blockchain_ledger_v1:ledger()) -> ok.
%%insert_witnesses(Path, LowerTimeBound, UpperTimeBound, Ledger) ->
%%    Length = length(Path),
%%    lists:foreach(fun({N, Element}) ->
%%                          Challengee = blockchain_poc_path_element_v1:challengee(Element),
%%                          Witnesses = blockchain_poc_path_element_v1:witnesses(Element),
%%                          %% TODO check these witnesses have valid RSSI
%%                          WitnessInfo0 = lists:foldl(fun(Witness, Acc) ->
%%                                                             TS = case blockchain_poc_witness_v1:timestamp(Witness) of
%%                                                                      T when T < LowerTimeBound ->
%%                                                                          LowerTimeBound;
%%                                                                      T when T > UpperTimeBound ->
%%                                                                          UpperTimeBound;
%%                                                                      T ->
%%                                                                          T
%%                                                                  end,
%%                                                             [{blockchain_poc_witness_v1:signal(Witness), TS, blockchain_poc_witness_v1:gateway(Witness)} | Acc]
%%                                                     end,
%%                                                     [],
%%                                                     Witnesses),
%%                          NextElements = lists:sublist(Path, N+1, Length),
%%                          WitnessInfo = case check_path_continuation(NextElements) of
%%                                                 true ->
%%                                                     %% the next hop is also a witness for this
%%                                                     NextHopElement = hd(NextElements),
%%                                                     NextHopAddr = blockchain_poc_path_element_v1:challengee(NextHopElement),
%%                                                     case blockchain_poc_path_element_v1:receipt(NextHopElement) of
%%                                                         undefined ->
%%                                                             %% There is no receipt from the next hop
%%                                                             %% We clamp to LowerTimeBound as best-effort
%%                                                             [{undefined, LowerTimeBound, NextHopAddr} | WitnessInfo0];
%%                                                         NextHopReceipt ->
%%                                                             [{blockchain_poc_receipt_v1:signal(NextHopReceipt),
%%                                                               blockchain_poc_receipt_v1:timestamp(NextHopReceipt),
%%                                                               NextHopAddr} | WitnessInfo0]
%%                                                     end;
%%                                                 false ->
%%                                                     WitnessInfo0
%%                                             end,
%%                          blockchain_ledger_v1:add_gateway_witnesses(Challengee, WitnessInfo, Ledger)
%%                  end, lists:zip(lists:seq(1, Length), Path)).

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
-spec validate(pos_integer(), txn_poc_receipts(), list(),
               [binary(), ...], [binary(), ...], blockchain_ledger_v1:ledger()) -> ok | {error, atom()}.
validate(_POCVersion, Txn, Path, LayerData, LayerHashes, OldLedger) ->
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
                    PerHopMaxWitnesses = blockchain_utils:poc_per_hop_max_witnesses(OldLedger),
                    Result = lists:foldl(
                               fun(_, {error, _} = Error) ->
                                       Error;
                                  ({Elem, Gateway, {LayerDatum, LayerHash}}, _Acc) ->
                                       case blockchain_poc_path_element_v1:challengee(Elem) == Gateway of
                                           true ->
                                               IsFirst = Elem == hd(?MODULE:path(Txn)),
                                               Receipt = blockchain_poc_path_element_v1:receipt(Elem),
                                               Witnesses = blockchain_poc_path_element_v1:witnesses(Elem),
                                               ExpectedOrigin = case IsFirst of
                                                                    true -> p2p;
                                                                    false -> radio
                                                                end,
                                               %% check the receipt
                                               RejectTxnEmptyReceipt =
                                                    case blockchain_ledger_v1:config(?poc_reject_empty_receipts, OldLedger) of
                                                        {ok, V} -> V;
                                                        _ -> false
                                                    end,
                                               case
                                                   (Receipt == undefined andalso RejectTxnEmptyReceipt == false) orelse
                                                   (Receipt == undefined andalso RejectTxnEmptyReceipt == true andalso Witnesses /= []) orelse
                                                   (blockchain_poc_receipt_v1:is_valid(Receipt, OldLedger) andalso
                                                    blockchain_poc_receipt_v1:gateway(Receipt) == Gateway andalso
                                                    blockchain_poc_receipt_v1:data(Receipt) == LayerDatum andalso
                                                    blockchain_poc_receipt_v1:origin(Receipt) == ExpectedOrigin)
                                               of
                                                   true ->
                                                       %% ok the receipt looks good, check the witnesses
                                                       Witnesses = blockchain_poc_path_element_v1:witnesses(Elem),
                                                       case erlang:length(Witnesses) > PerHopMaxWitnesses of
                                                           true ->
                                                               {error, too_many_witnesses};
                                                           false ->
                                                               %% check there are no duplicates in witnesses list
                                                               WitnessGateways = [blockchain_poc_witness_v1:gateway(W) || W <- Witnesses],
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
                                                               lager:warning([{poc_id, POCID}],
                                                                             "Receipt undefined, ExpectedOrigin: ~p, LayerDatum: ~p, Gateway: ~p",
                                                                             [ExpectedOrigin, LayerDatum, Gateway]);
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
                                  blockchain_poc_path_element_v1:print(Element)
                          end,
                          Path), "\n\t").

json_type() ->
    <<"poc_receipts_v2">>.

-spec to_json(Txn :: txn_poc_receipts(),
              Opts :: blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Txn, Opts) ->
    PathElems =
    case {lists:keyfind(ledger, 1, Opts), lists:keyfind(chain, 1, Opts)} of
        {{ledger, Ledger}, {chain, Chain}} ->
                    FoldedPath =
                        tagged_path_elements_fold(fun(Elem, {TaggedWitnesses, ValidReceipt}, Acc) ->
                                                     ElemOpts = [{tagged_witnesses, TaggedWitnesses},
                                                                 {valid_receipt, ValidReceipt}],
                                                     [{Elem, ElemOpts} | Acc]
                                             end, [], Txn, Ledger, Chain),
                    lists:reverse(FoldedPath);
        {_, _} ->
            [{Elem, []} || Elem <- path(Txn)]
    end,
    #{
      type => ?MODULE:json_type(),
      hash => ?BIN_TO_B64(hash(Txn)),
      secret => ?BIN_TO_B64(secret(Txn)),
      onion_key_hash => ?BIN_TO_B64(onion_key_hash(Txn)),
      path => [blockchain_poc_path_element_v1:to_json(Elem, ElemOpts) || {Elem, ElemOpts} <- PathElems],
      fee => fee(Txn),
      challenger => ?BIN_TO_B58(challenger(Txn)),
      block_hash => ?BIN_TO_B64(block_hash(Txn))
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
        false ->
            {error, invalid_witness}
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
            Version = poc_version(Ledger),
            DstPubkeyBin = blockchain_poc_path_element_v1:challengee(Element),
            SrcPubkeyBin = blockchain_poc_path_element_v1:challengee(PreviousElement),
            {ok, SourceLoc} = blockchain_ledger_v1:find_gateway_location(SrcPubkeyBin, Ledger),
            SourceRegion = blockchain_region_v1:h3_to_region(SourceLoc, Ledger),
            {ok, DestinationLoc} = blockchain_ledger_v1:find_gateway_location(DstPubkeyBin, Ledger),
            {ok, ExclusionCells} = blockchain_ledger_v1:config(?poc_v4_exclusion_cells, Ledger),
            {ok, ParentRes} = blockchain_ledger_v1:config(?poc_v4_parent_res, Ledger),
            SourceParentIndex = h3:parent(SourceLoc, ParentRes),
            DestinationParentIndex = h3:parent(DestinationLoc, ParentRes),

            case is_same_region(SourceRegion, DestinationLoc, Ledger) of
                false ->
                    DestinationRegion = blockchain_region_v1:h3_to_region(DestinationLoc, Ledger),
                    lager:debug("Not in the same region!~nSrcPubkeyBin: ~p, DstPubkeyBin: ~p, SourceLoc: ~p, DestinationLoc: ~p",
                                [blockchain_utils:addr2name(SrcPubkeyBin),
                                 blockchain_utils:addr2name(DstPubkeyBin),
                                 SourceRegion, DestinationRegion]),
                    undefined;
                true ->
                    Limit = blockchain:config(?poc_distance_limit, Ledger),
                    case is_too_far(Limit, SourceLoc, DestinationLoc) of
                        {true, Distance} ->
                            lager:debug("Src too far from destination!~nSrcPubkeyBin: ~p, DstPubkeyBin: ~p, SourceLoc: ~p, DestinationLoc: ~p, Distance: ~p",
                                        [blockchain_utils:addr2name(SrcPubkeyBin),
                                         blockchain_utils:addr2name(DstPubkeyBin),
                                         SourceLoc, DestinationLoc, Distance]),
                            undefined;
                        false ->
                            try h3:grid_distance(SourceParentIndex, DestinationParentIndex) of
                                Dist when Dist >= ExclusionCells ->
                                    RSSI = blockchain_poc_receipt_v1:signal(Receipt),
                                    SNR = blockchain_poc_receipt_v1:snr(Receipt),
                                    Freq = blockchain_poc_receipt_v1:frequency(Receipt),
                                    MinRcvSig = min_rcv_sig(Receipt, Ledger, SourceLoc, SourceRegion, DstPubkeyBin, DestinationLoc, Freq, Version),
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
                                            case check_valid_frequency(SourceRegion, Freq, Ledger, Version) of
                                                true ->
                                                    case blockchain:config(?data_aggregation_version, Ledger) of
                                                        {ok, DataAggVsn} when DataAggVsn > 1 ->
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
                                                        _ ->
                                                            %% SNR+Freq+Channels not collected, nothing else we can check
                                                            Receipt
                                                    end;
                                                _ ->
                                                    undefined
                                            end
                                    end;
                                _ ->
                                    %% too close
                                    undefined
                            catch
                                _:_ ->
                                    %% pentagonal distortion
                                    undefined
                            end
                    end
            end

    end.

-spec valid_witnesses(Element :: blockchain_poc_path_element_v1:poc_element(),
                      Channel :: non_neg_integer(),
                      Ledger :: blockchain_ledger_v1:ledger()) -> blockchain_poc_witness_v1:poc_witnesses().
valid_witnesses(Element, Channel, Ledger) ->
    {ok, RegionVars} = blockchain_region_v1:get_all_region_bins(Ledger),
    valid_witnesses(Element, Channel, RegionVars, Ledger).

valid_witnesses(Element, Channel, RegionVars, Ledger) ->
    TaggedWitnesses = tagged_witnesses(Element, Channel, RegionVars, Ledger),
    [ W || {true, _, W} <- TaggedWitnesses ].

-spec valid_witness_addrs(Element :: blockchain_poc_path_element_v1:poc_element(),
                      Channel :: non_neg_integer(),
                      Ledger :: blockchain_ledger_v1:ledger()) -> [libp2p_crypto:pubkey_bin()].
valid_witness_addrs(Element, Channel, Ledger) ->
    ValidWitnesses = valid_witnesses(Element, Channel, Ledger),
    [W#blockchain_poc_witness_v1_pb.gateway || W <- ValidWitnesses].

-spec is_too_far(Limit :: any(),
                 SrcLoc :: h3:h3_index(),
                 DstLoc :: h3:h3_index()) -> {true, float()} | false.
is_too_far(Limit, SrcLoc, DstLoc) ->
    case Limit of
        {ok, L} ->
            Distance = blockchain_utils:distance(SrcLoc, DstLoc),
            case Distance > L of
                true ->
                    {true, Distance};
                false ->
                    false
            end;
        _ ->
            %% var not set, it's not too far (don't consider it)
            false
    end.

-spec check_valid_frequency(Region0 :: {error, any()} | {ok, atom()},
                            Frequency :: float(),
                            Ledger :: blockchain_ledger_v1:ledger(),
                            Version :: non_neg_integer()) -> boolean().
check_valid_frequency(Region0, Frequency, Ledger, _Version) ->
    {ok, Region} = Region0,
    {ok, Params} = blockchain_region_params_v1:for_region(Region, Ledger),
    ChannelFreqs = [blockchain_region_param_v1:channel_frequency(I) || I <- Params],
    lists:any(fun(E) -> abs(E - Frequency*?MHzToHzMultiplier) =< 1000 end, ChannelFreqs).

-spec is_same_region(
    SourceRegion :: {error, any()} | {ok, atom()},
    DstLoc :: h3:h3_index(),
    Ledger :: blockchain_ledger_v1:ledger()
) -> boolean().
is_same_region(SourceRegion0, DstLoc, Ledger) ->
    {ok, SourceRegion} = SourceRegion0,
    blockchain_region_v1:h3_in_region(DstLoc, SourceRegion, Ledger).


%% This function adds a tag to each witness specifying a reason why a witness was considered invalid,
%% Further, this same function is used to check witness validity in valid_witnesses fun.
-spec tagged_witnesses(Element :: blockchain_poc_path_element_v1:poc_element(),
                       Channel :: non_neg_integer(),
                       Ledger :: blockchain_ledger_v1:ledger()) -> tagged_witnesses().
tagged_witnesses(Element, Channel, Ledger) ->
    {ok, RegionVars} = blockchain_region_v1:get_all_region_bins(Ledger),
    tagged_witnesses(Element, Channel, RegionVars, Ledger).

%%-spec tagged_witnesses(Element :: blockchain_poc_path_element_v1:poc_element(),
%%                       Channel :: non_neg_integer(),
%%                       RegionVars0 :: no_prefetch | [{atom(), binary() | {error, any()}}] | {ok, [{atom(), binary() | {error, any()}}]},
%%                       Ledger :: blockchain_ledger_v1:ledger()) -> tagged_witnesses().
tagged_witnesses(Element, Channel, RegionVars0, Ledger) ->
    SrcPubkeyBin = blockchain_poc_path_element_v1:challengee(Element),
    {ok, SourceLoc} = blockchain_ledger_v1:find_gateway_location(SrcPubkeyBin, Ledger),
    RegionVars =
        case RegionVars0 of
            {ok, RV} -> RV;
            RV when is_list(RV) -> RV;
            {error, _Reason} -> no_prefetch
        end,
    SourceRegion = blockchain_region_v1:h3_to_region(SourceLoc, Ledger, RegionVars),
    {ok, ParentRes} = blockchain_ledger_v1:config(?poc_v4_parent_res, Ledger),
    SourceParentIndex = h3:parent(SourceLoc, ParentRes),

    %% foldl will re-reverse
    Witnesses = lists:reverse(blockchain_poc_path_element_v1:witnesses(Element)),

    DiscardZeroFreq = blockchain_ledger_v1:config(?discard_zero_freq_witness, Ledger),
    {ok, ExclusionCells} = blockchain_ledger_v1:config(?poc_v4_exclusion_cells, Ledger),
    %% intentionally do not require
    DAV = blockchain:config(?data_aggregation_version, Ledger),
    Limit = blockchain:config(?poc_distance_limit, Ledger),
    Version = poc_version(Ledger),

    TaggedWitnesses = lists:foldl(fun(Witness, Acc) ->
                         DstPubkeyBin = blockchain_poc_witness_v1:gateway(Witness),
                         {ok, DestinationLoc} = blockchain_ledger_v1:find_gateway_location(DstPubkeyBin, Ledger),
%%                            case blockchain_region_v1:h3_to_region(DestinationLoc, Ledger, RegionVars) of
%%                                {error, {unknown_region, _Loc}} when Version >= 11 ->
%%                                    lager:warning("saw unknown region for ~p loc ~p",
%%                                                  [DstPubkeyBin, DestinationLoc]),
%%                                    unknown;
%%                                {error, _} -> unknown;
%%                                {ok, DR} -> {ok, DR}
%%                            end,
                         DestinationParentIndex = h3:parent(DestinationLoc, ParentRes),
                         Freq = blockchain_poc_witness_v1:frequency(Witness),

                         case {DiscardZeroFreq, Freq} of
                             {{ok, true}, 0.0} ->
                                [{false, <<"witness_zero_freq">>, Witness} | Acc];
                             _ ->
                                 case is_same_region(SourceRegion, DestinationLoc, Ledger) of
                                     false ->
                                         lager:debug("Not in the same region!~nSrcPubkeyBin: ~p, DstPubkeyBin: ~p, SourceLoc: ~p, DestinationLoc: ~p",
                                                     [blockchain_utils:addr2name(SrcPubkeyBin),
                                                      blockchain_utils:addr2name(DstPubkeyBin),
                                                      SourceLoc, DestinationLoc]),
                                         [{false, <<"witness_not_same_region">>, Witness} | Acc];
                                     true ->
                                         case is_too_far(Limit, SourceLoc, DestinationLoc) of
                                             {true, Distance} ->
                                                 lager:debug("Src too far from destination!~nSrcPubkeyBin: ~p, DstPubkeyBin: ~p, SourceLoc: ~p, DestinationLoc: ~p, Distance: ~p",
                                                             [blockchain_utils:addr2name(SrcPubkeyBin),
                                                              blockchain_utils:addr2name(DstPubkeyBin),
                                                              SourceLoc, DestinationLoc, Distance]),
                                                 [{false, <<"witness_too_far">>, Witness} | Acc];
                                             false ->
                                                 try h3:grid_distance(SourceParentIndex, DestinationParentIndex) of
                                                     Dist when Dist >= ExclusionCells ->
                                                         RSSI = blockchain_poc_witness_v1:signal(Witness),
                                                         SNR = blockchain_poc_witness_v1:snr(Witness),
                                                         MinRcvSig = min_rcv_sig(blockchain_poc_path_element_v1:receipt(Element),
                                                                                 Ledger,
                                                                                 SourceLoc,
                                                                                 SourceRegion,
                                                                                 DstPubkeyBin,
                                                                                 DestinationLoc,
                                                                                 Freq,
                                                                                 Version),

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
                                                                 case check_valid_frequency(SourceRegion, Freq, Ledger, Version) of
                                                                     true ->
                                                                         case DAV of
                                                                             {ok, DataAggVsn} when DataAggVsn > 1 ->
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
                                                                             _ ->
                                                                                 %% SNR+Freq+Channels not collected, nothing else we can check
                                                                                 [{true, <<"insufficient_data">>, Witness} | Acc]
                                                                         end;
                                                                     _ ->
                                                                         [{false, <<"incorrect_frequency">>, Witness} | Acc]
                                                                 end
                                                         end;
                                                     _ ->
                                                         %% too close
                                                         [{false, <<"witness_too_close">>, Witness} | Acc]
                                                 catch _:_ ->
                                                           %% pentagonal distortion
                                                           [{false, <<"pentagonal_distortion">>, Witness} | Acc]
                                                 end

                                         end
                                 end
                         end
                 end, [], Witnesses),
    WitnessCount = length(Witnesses),
    TaggedWitnessCount = length(TaggedWitnesses),
    lager:debug("filtered ~p of ~p witnesses for receipt ~p", [(WitnessCount - TaggedWitnessCount),
                                                               WitnessCount, blockchain_poc_path_element_v1:receipt(Element)]),
    TaggedWitnesses.

-spec get_channels(Txn :: txn_poc_receipts(),
                   Chain :: blockchain:blockchain()) -> {ok, [non_neg_integer()]} | {error, any()}.
get_channels(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Version = poc_version(Ledger),
    {ok, RegionVars} = blockchain_region_v1:get_all_region_bins(Ledger),
    get_channels(Txn, Version, RegionVars, Chain).

-spec get_channels(Txn :: txn_poc_receipts(),
                   POCVersion :: pos_integer(),
                   RegionVars :: no_prefetch | [{atom(), binary() | {error, any()}}] | {ok, [{atom(), binary() | {error, any()}}]},
                   Chain :: blockchain:blockchain()) -> {ok, [non_neg_integer()]} | {error, any()}.
get_channels(Txn, POCVersion, RegionVars, Chain) ->
    Path0 = ?MODULE:path(Txn),
    PathLength = length(Path0),
    BlockHash = ?MODULE:block_hash(Txn),
    OnionKeyHash = ?MODULE:onion_key_hash(Txn),
    Ledger = blockchain:ledger(Chain),
    case BlockHash of
        <<>> ->
            {error, request_block_hash_not_found};
        undefined ->
            {error, request_block_hash_not_found};
        _BH ->
            Entropy1 = <<OnionKeyHash/binary, BlockHash/binary>>,
            [_ | LayerData] = blockchain_txn_poc_receipts_v2:create_secret_hash(Entropy1, PathLength+1),
            Path = [blockchain_poc_path_element_v1:challengee(Element) || Element <- Path0],
            Channels = get_channels(POCVersion, Ledger, Path, LayerData, RegionVars),
            {ok, Channels}
    end.

-spec get_channels(POCVersion :: pos_integer(),
                    Ledger :: blockchain_ledger_v1:ledger(),
                    Path :: [libp2p_crypto:pubkey_bin()],
                    LayerData :: [binary()],
                    RegionVars :: no_prefetch | [{atom(), binary() | {error, any()}}] | {ok, [{atom(), binary() | {error, any()}}]} | {error, any()}) -> [non_neg_integer()].
get_channels(_POCVersion, Ledger, Path, LayerData, RegionVars0) ->
    Challengee = hd(Path),
    RegionVars =
        case RegionVars0 of
            {ok, RV} -> RV;
            RV when is_list(RV) -> RV;
            no_prefetch -> no_prefetch;
            {error, Reason} -> error({get_channels_region, Reason})
        end,

    ChannelCount =
            %% Get from region vars
            %% Just get the channels using the challengee's region from head of the path
            %% We assert that all path members (which is only 1 member, beacon right now)
            %% will be in the same region
            case blockchain_ledger_v1:find_gateway_location(Challengee, Ledger) of
                {error, _}=E ->
                    throw(E);
                {ok, ChallengeeLoc} ->
                    case blockchain_region_v1:h3_to_region(ChallengeeLoc, Ledger, RegionVars) of
                        {error, _}=E ->
                            throw(E);
                        {ok, Region} ->
                            {ok, Params} = blockchain_region_params_v1:for_region(Region, Ledger),
                            length(Params)
                    end
            end,
    lists:map(fun(Layer) ->
                      <<IntData:16/integer-unsigned-little>> = Layer,
                      IntData rem ChannelCount
              end, LayerData).

-spec min_rcv_sig(Receipt :: undefined | blockchain_poc_receipt_v1:receipt(),
                  Ledger :: blockchain_ledger_v1:ledger(),
                  SourceLoc :: h3:h3_index(),
                  SourceRegion0 :: {ok, atom()} | {error, any()},
                  DstPubkeyBin :: libp2p_crypto:pubkey_bin(),
                  DestinationLoc :: h3:h3_index(),
                  Freq :: float(),
                  POCVersion :: non_neg_integer()) -> float().
min_rcv_sig(undefined, Ledger, SourceLoc, SourceRegion0, DstPubkeyBin, DestinationLoc, Freq, _POCVersion) ->
    %% Receipt can be undefined
    %% Estimate tx power because there is no receipt with attached tx_power
    lager:debug("SourceLoc: ~p, Freq: ~p", [SourceLoc, Freq]),
    {ok, SourceRegion} = SourceRegion0,
    {ok, TxPower} = estimated_tx_power(SourceRegion, Freq, Ledger),
    FSPL = calc_fspl(DstPubkeyBin, SourceLoc, DestinationLoc, Freq, Ledger),
    case blockchain:config(?fspl_loss, Ledger) of
        {ok, Loss} -> blockchain_utils:min_rcv_sig(FSPL, TxPower) * Loss;
        _ -> blockchain_utils:min_rcv_sig(FSPL, TxPower)
    end;
min_rcv_sig(Receipt, Ledger, SourceLoc, SourceRegion0, DstPubkeyBin, DestinationLoc, Freq, POCVersion) ->
    %% We do have a receipt
    %% Get tx_power from attached receipt and use it to calculate min_rcv_sig
    case blockchain_poc_receipt_v1:tx_power(Receipt) of
        %% Missing protobuf fields have default value as 0
        TxPower when TxPower == undefined; TxPower == 0 ->
            min_rcv_sig(undefined, Ledger, SourceLoc, SourceRegion0,
                        DstPubkeyBin, DestinationLoc, Freq, POCVersion);
        TxPower ->
            FSPL = calc_fspl(DstPubkeyBin, SourceLoc, DestinationLoc, Freq, Ledger),
            case blockchain:config(?fspl_loss, Ledger) of
                {ok, Loss} -> blockchain_utils:min_rcv_sig(FSPL, TxPower) * Loss;
                _ -> blockchain_utils:min_rcv_sig(FSPL, TxPower)
            end
    end.

calc_fspl(DstPubkeyBin, SourceLoc, DestinationLoc, Freq, Ledger) ->
    {ok, DstGR} = blockchain_ledger_v1:find_gateway_gain(DstPubkeyBin, Ledger),
    %% NOTE: Transmit gain is set to 0 when calculating free_space_path_loss
    %% This is because the packet forwarder will be configured to subtract the antenna
    %% gain and miner will always transmit at region EIRP.
    GT = 0,
    GR = DstGR / 10,
    blockchain_utils:free_space_path_loss(SourceLoc, DestinationLoc, Freq, GT, GR).

estimated_tx_power(Region, Freq, Ledger) ->
    {ok, Params} = blockchain_region_params_v1:for_region(Region, Ledger),
    FreqEirps = [{blockchain_region_param_v1:channel_frequency(I),
                  blockchain_region_param_v1:max_eirp(I)} || I <- Params],
    %% NOTE: Convert src frequency to Hz before checking freq match for EIRP value
    EIRP = eirp_from_closest_freq(Freq * ?MHzToHzMultiplier, FreqEirps),
    {ok, EIRP / 10}.

eirp_from_closest_freq(Freq, [Head | Tail]) ->
    eirp_from_closest_freq(Freq, Tail, Head).

eirp_from_closest_freq(_Freq, [], {_BestFreq, BestEIRP}) -> BestEIRP;
eirp_from_closest_freq(Freq, [ {NFreq, NEirp} | Rest ], {BestFreq, BestEIRP}) ->
    case abs(Freq - NFreq) =< abs(Freq - BestFreq) of
        true ->
            eirp_from_closest_freq(Freq, Rest, {NFreq, NEirp});
        false ->
            eirp_from_closest_freq(Freq, Rest, {BestFreq, BestEIRP})
    end.

-spec poc_version(blockchain_ledger_v1:ledger()) -> non_neg_integer().
poc_version(Ledger) ->
    case blockchain:config(?poc_version, Ledger) of
        {error, not_found} -> 0;
        {ok, V} -> V
    end.

-spec verify_poc_details(
    Txn :: txn_poc_receipts(),
    PoC :: blockchain_ledger_poc_v3:poc(),
    Keys :: map()
) -> ok | {error, atom()}.
verify_poc_details(Txn, PoC, Keys) ->
    %% verify the secret (pub and priv keys) submitted by the challenger
    %% are a valid key pair
    %% to do this sign a msg with the priv key and verify its sig with
    %% the pub key
    %% we also verify the hash of the pub key matches the onion key hash
    POCOnionKeyHash = ?MODULE:onion_key_hash(Txn),
    BlockHash = ?MODULE:block_hash(Txn),
    Challenger = ?MODULE:challenger(Txn),
    #{public := PubKey, secret := PrivKey} = Keys,
    OnionHash = crypto:hash(sha256, libp2p_crypto:pubkey_to_bin(PubKey)),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    SignedPayload = SigFun(OnionHash),
    case blockchain_ledger_poc_v3:verify(PoC, Challenger, BlockHash) of
        false -> {error, mismatched_poc};
        true ->
            case POCOnionKeyHash == OnionHash of
                false -> {error, mismatched_onion_key_hash};
                true ->
                    case libp2p_crypto:verify(OnionHash, SignedPayload, PubKey) of
                        false -> {error, invalid_secret};
                        true -> ok
                    end

            end
    end.

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
        signature = <<>>,
        block_hash = <<"blockhash">>
    },
    ?assertEqual(Tx, new(<<"challenger">>,  <<"secret">>, <<"onion">>, [], <<"blockhash">>)).

onion_key_hash_test() ->
    Tx = new(<<"challenger">>,  <<"secret">>, <<"onion">>, [], <<"blockhash">>),
    ?assertEqual(<<"onion">>, onion_key_hash(Tx)).

challenger_test() ->
    Tx = new(<<"challenger">>,  <<"secret">>, <<"onion">>, [], <<"blockhash">>),
    ?assertEqual(<<"challenger">>, challenger(Tx)).

blockhash_test() ->
    Tx = new(<<"challenger">>,  <<"secret">>, <<"onion">>, [], <<"blockhash">>),
    ?assertEqual(<<"blockhash">>, block_hash(Tx)).

secret_test() ->
    Tx = new(<<"challenger">>,  <<"secret">>, <<"onion">>, [], <<"blockhash">>),
    ?assertEqual(<<"secret">>, secret(Tx)).

path_test() ->
    Tx = new(<<"challenger">>,  <<"secret">>, <<"onion">>, [], <<"blockhash">>),
    ?assertEqual([], path(Tx)).

fee_test() ->
    Tx = new(<<"challenger">>,  <<"secret">>, <<"onion">>, [], <<"blockhash">>),
    ?assertEqual(0, fee(Tx)).

signature_test() ->
    Tx = new(<<"challenger">>,  <<"secret">>, <<"onion">>, [], <<"blockhash">>),
    ?assertEqual(<<>>, signature(Tx)).

sign_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Challenger = libp2p_crypto:pubkey_to_bin(PubKey),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx0 = new(Challenger,  <<"secret">>, <<"onion">>, [], <<"blockhash">>),
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

    Txn = new(Challenger, Secret, OnionKeyHash, Path, BlockHash),
    Json = to_json(Txn, []),

    ?assert(lists:all(fun(K) -> maps:is_key(K, Json) end,
                      [type, hash, secret, onion_key_hash, block_hash, path, fee, challenger])).


eirp_from_closest_freq_test() ->
    FreqEirps = [{915.8, 10}, {915.3, 20}, {914.9, 30}, {915.2, 15}, {915.7, 12}, {916.9, 100}],
    EIRP = eirp_from_closest_freq(915.1, FreqEirps),
    ?assertEqual(15, EIRP).

-endif.
