%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Proof of Coverage Receipts V2 ==
%%
%% NOTE: This builds on top of v1 poc receipt txns.
%%
%% New additions:
%%
%% * Witness eligbility
%% We now control which witnesses are eligible to be added as witnesses for
%% any particular poc. This is dependent on the distance between the challengee
%% and the witness hotspot (which in turn is controlled via poc_v4_exclusion_cells var).
%%
%% * Limited number of witnesses.
%% This is controller via total_allowed_witnesses chain var.
%%
%% * ...
%%
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
         hex_poc_id/1,
         print/1
        ]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type txn_poc_receipts() :: #blockchain_txn_poc_receipts_v2_pb{}.

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
                %% TODO: Fix this damned function or do something better...
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
                                %% TODO: Update gateway score via the new leveling scheme
                                ok
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
    {ok, TotalAllowedWitnesses} = blockchain:config(?total_allowed_witnesses, Ledger),
    %% Get total eligible witnesses in path
    TotalEligibleWitnesses = blockchain_poc_path_element_v2:total_eligible_witnesses(Path, Ledger),

    case TotalEligibleWitnesses =< TotalAllowedWitnesses of
        true ->
            %% no need to squish, allow adding to ledger
            insert_witnesses_(Path, LowerTimeBound, UpperTimeBound, LastOnionKeyHash, Ledger);
        false ->
            ToSquish = TotalAllowedWitnesses - TotalEligibleWitnesses,
            NewPath = blockchain_poc_path_element_v2:squish(Path, LastOnionKeyHash, Ledger, ToSquish),
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

            Vars =
            blockchain_utils:vars_binary_keys_to_atoms_from_list(blockchain_ledger_v1:snapshot_vars(OldLedger)),
            %% Find the original target
            {ok, {Target, TargetRandState}} = blockchain_poc_target_v4:target(Challenger, Entropy, OldLedger, Vars),
            %% Path building phase
            Time = blockchain_block:time(LastChallengeBlock),
            Path = blockchain_poc_path_v5:build(Target, TargetRandState, OldLedger, Time, Vars),

            N = erlang:length(Path),
            [<<IV:16/integer-unsigned-little, _/binary>> | LayerData] = blockchain_txn_poc_receipts_v2:create_secret_hash(Entropy, N+1),
            OnionList = lists:zip([libp2p_crypto:bin_to_pubkey(P) || P <- Path], LayerData),
            {_Onion, Layers} = blockchain_poc_packet:build(libp2p_crypto:keys_from_bin(Secret), IV, OnionList, PrePoCBlockHash, OldLedger),
            %% no witness will exist with the first layer hash
            [_|LayerHashes] = [crypto:hash(sha256, L) || L <- Layers],
            StartV = erlang:monotonic_time(millisecond),
            Ret = blockchain_poc_path_v5:validate(Txn, Path, LayerData, LayerHashes, OldLedger),
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
