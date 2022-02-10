%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Ledger Gateway ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_ledger_gateway_v2).

-include("blockchain_caps.hrl").

-export([
    new/2, new/3, new/4,
    owner_address/1, owner_address/2,
    location/1, location/2,
    gain/1, gain/2,
    elevation/1, elevation/2,
    mode/1, mode/2,
    last_location_nonce/1, last_location_nonce/2,
    score/4,
    version/1, version/2,
    add_neighbor/2, remove_neighbor/2,
    neighbors/1, neighbors/2,
    last_poc_challenge/1, last_poc_challenge/2,
    last_poc_onion_key_hash/1, last_poc_onion_key_hash/2,
    nonce/1, nonce/2,
    print/3, print/4,
    serialize/1, deserialize/1,
    alpha/1,
    beta/1,
    delta/1,
    set_alpha_beta_delta/4,
    add_witness/1, add_witness/7,
    has_witness/2,
    clear_witnesses/1,
    remove_witness/2,
    witnesses/1, witnesses/3,
    witnesses_plain/1,
    witness_hist/1, witness_recent_time/1, witness_first_time/1,
    oui/1, oui/2,
    mask/2,
    is_valid_capability/3
]).

-import(blockchain_utils, [normalize_float/1]).

-include("blockchain_utils.hrl").
-include("blockchain_vars.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-define(TEST_LOCATION, 631210968840687103).
-endif.

-record(witness, {
          nonce :: non_neg_integer(),
          count :: non_neg_integer(),
          hist = erlang:error(no_histogram) :: [{integer(), integer()}], %% sampled rssi histogram
          first_time :: undefined | non_neg_integer(), %% first time a hotspot witnessed this one
          recent_time :: undefined | non_neg_integer(), %% most recent a hotspots witnessed this one
          time = #{} :: #{integer() => integer()}, %% TODO: add time of flight histogram
          added_location_nonce = 0 :: non_neg_integer()  %% the nonce value of the witness GW at the time the witness was added to the challengee GW
         }).

-record(gateway_v2, {
    owner_address :: libp2p_crypto:pubkey_bin(),
    location :: undefined | pos_integer(),
    alpha = 1.0 :: float(),
    beta = 1.0 :: float(),
    delta :: non_neg_integer(),
    last_poc_challenge :: undefined | non_neg_integer(),
    last_poc_onion_key_hash :: undefined | binary(),
    nonce = 0 :: non_neg_integer(),
    version = 0 :: non_neg_integer(),
    neighbors = [] :: [libp2p_crypto:pubkey_bin()],
    witnesses = [] :: witnesses_int(),
    oui = undefined :: undefined | pos_integer(),
    gain = ?DEFAULT_GAIN :: integer(),
    elevation = ?DEFAULT_ELEVATION :: integer(),
    mode = full :: mode(),
    last_location_nonce = 0 :: non_neg_integer()       %% the value of the GW nonce at the time of the last location assertion
}).

-type gateway() :: #gateway_v2{}.
-type gateway_witness() :: #witness{}.
-type witnesses() :: #{libp2p_crypto:pubkey_bin() => gateway_witness()}.
-type witnesses_int() :: [{libp2p_crypto:pubkey_bin(), gateway_witness()}].
-type histogram() :: #{integer() => integer()}.
-type mode() :: dataonly | light | full.
-export_type([gateway/0, gateway_witness/0, witnesses/0, histogram/0, mode/0]).

-spec new(OwnerAddress :: libp2p_crypto:pubkey_bin(),
          Location :: pos_integer() | undefined) -> gateway().
new(OwnerAddress, Location) ->
    #gateway_v2{
        owner_address=OwnerAddress,
        location=Location,
        delta=1,
        mode=full
    }.

-spec new(OwnerAddress :: libp2p_crypto:pubkey_bin(),
          Location :: pos_integer() | undefined,
          Mode :: mode()) -> gateway().
new(OwnerAddress, Location, Mode) ->
    #gateway_v2{
        owner_address=OwnerAddress,
        location=Location,
        delta=1,
        mode=Mode
    }.

-spec new(OwnerAddress :: libp2p_crypto:pubkey_bin(),
          Location :: pos_integer() | undefined,
          Mode :: mode(),
          Nonce :: non_neg_integer()) -> gateway().
new(OwnerAddress, Location, Mode, Nonce) ->
    #gateway_v2{
        owner_address=OwnerAddress,
        location=Location,
        nonce=Nonce,
        delta=1,
        mode=Mode
    }.

-spec owner_address(Gateway :: gateway()) -> libp2p_crypto:pubkey_bin().
owner_address(Gateway) ->
    Gateway#gateway_v2.owner_address.

-spec owner_address(OwnerAddress :: libp2p_crypto:pubkey_bin(),
                    Gateway :: gateway()) -> gateway().
owner_address(OwnerAddress, Gateway) ->
    Gateway#gateway_v2{owner_address=OwnerAddress}.

-spec location(Gateway :: gateway()) ->  undefined | pos_integer().
location(Gateway) ->
    Gateway#gateway_v2.location.

-spec location(Location :: pos_integer(), Gateway :: gateway()) -> gateway().
location(Location, Gateway) ->
    Gateway#gateway_v2{location=Location}.

-spec gain(Gateway :: gateway()) ->  undefined | integer().
gain(Gateway) ->
    Gateway#gateway_v2.gain.

-spec gain(Gain :: integer(), Gateway :: gateway()) -> gateway().
gain(Gain, Gateway) ->
    Gateway#gateway_v2{gain=Gain}.

-spec elevation(Gateway :: gateway()) ->  undefined | integer().
elevation(Gateway) ->
    Gateway#gateway_v2.elevation.

-spec elevation(Elevation :: integer(), Gateway :: gateway()) -> gateway().
elevation(Elevation, Gateway) ->
    Gateway#gateway_v2{elevation=Elevation}.

-spec mode(Gateway :: gateway()) ->  mode().
mode(Gateway) ->
    Gateway#gateway_v2.mode.

-spec mode(Mode :: mode(), Gateway :: gateway()) -> gateway().
mode(Mode, Gateway) ->
    Gateway#gateway_v2{mode=Mode}.
%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec last_location_nonce(Gateway :: gateway()) ->  non_neg_integer().
last_location_nonce(Gateway) ->
    Gateway#gateway_v2.last_location_nonce.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec last_location_nonce(Nonce :: non_neg_integer(), Gateway :: gateway()) -> gateway().
last_location_nonce(Nonce, Gateway) ->
    Gateway#gateway_v2{last_location_nonce=Nonce}.

version(Gateway) ->
    Gateway#gateway_v2.version.

version(Version, Gateway) ->
    Gateway#gateway_v2{version = Version}.

add_neighbor(Neighbor, Gateway) ->
    N = Gateway#gateway_v2.neighbors,
    Gateway#gateway_v2{neighbors = lists:usort([Neighbor | N])}.

remove_neighbor(Neighbor, Gateway) ->
    N = Gateway#gateway_v2.neighbors,
    Gateway#gateway_v2{neighbors = lists:delete(Neighbor, N)}.

neighbors(Gateway) ->
    Gateway#gateway_v2.neighbors.

neighbors(Neighbors, Gateway) ->
    Gateway#gateway_v2{neighbors = Neighbors}.


%%--------------------------------------------------------------------
%% @doc The score corresponds to the P(claim_of_location).
%% We look at the 1st and 3rd quartile values in the beta distribution
%% which we calculate using Alpha/Beta (shape parameters).
%%
%% The IQR essentially is a measure of the spread of the peak probability distribution
%% function, it boils down to the amount of "confidence" we have in that particular value.
%% The steeper the peak, the lower the IQR and hence the more confidence we have in that hotpot's score.
%%
%% Mean is the expected score without accounting for IQR. Since we _know_ that a lower IQR implies
%% more confidence, we simply do Mean * (1 - IQR) as the eventual score.
%%
%% @end
%%--------------------------------------------------------------------
-spec score(Address :: libp2p_crypto:pubkey_bin(),
            Gateway :: gateway(),
            Height :: pos_integer(),
            Ledger :: blockchain_ledger_v1:ledger()) -> {float(), float(), float()}.
score(Address,
      #gateway_v2{alpha=Alpha, beta=Beta, delta=Delta},
      Height,
      Ledger) ->
    blockchain_score_cache:fetch({Address, Alpha, Beta, Delta, Height},
                                 fun() ->
                                         {ok, AlphaDecay} = blockchain:config(?alpha_decay, Ledger),
                                         {ok, BetaDecay} = blockchain:config(?beta_decay, Ledger),
                                         {ok, MaxStaleness} = blockchain:config(?max_staleness, Ledger),
                                         NewAlpha = normalize_float(scale_shape_param(Alpha - decay(AlphaDecay, Height - Delta, MaxStaleness))),
                                         NewBeta = normalize_float(scale_shape_param(Beta - decay(BetaDecay, Height - Delta, MaxStaleness))),
                                         RV1 = normalize_float(erlang_stats:qbeta(0.25, NewAlpha, NewBeta)),
                                         RV2 = normalize_float(erlang_stats:qbeta(0.75, NewAlpha, NewBeta)),
                                         IQR = normalize_float(RV2 - RV1),
                                         Mean = normalize_float(1 / (1 + NewBeta/NewAlpha)),
                                         {NewAlpha, NewBeta, normalize_float(Mean * (1 - IQR))}
                                 end).

%%--------------------------------------------------------------------
%% @doc
%% K: constant decay factor, calculated empirically (for now)
%% Staleness: current_ledger_height - delta
%% @end
%%--------------------------------------------------------------------
-spec decay(float(), pos_integer(), pos_integer()) -> float().
decay(K, Staleness, MaxStaleness) when Staleness =< MaxStaleness ->
    math:exp(K * Staleness) - 1;
decay(_, _, _) ->
    %% Basically infinite decay at this point
    math:exp(709).

-spec scale_shape_param(float()) -> float().
scale_shape_param(ShapeParam) ->
    case ShapeParam =< 1.0 of
        true -> 1.0;
        false -> ShapeParam
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec alpha(Gateway :: gateway()) -> float().
alpha(Gateway) ->
    Gateway#gateway_v2.alpha.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec beta(Gateway :: gateway()) -> float().
beta(Gateway) ->
    Gateway#gateway_v2.beta.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec delta(Gateway :: gateway()) -> undefined | non_neg_integer().
delta(Gateway) ->
    Gateway#gateway_v2.delta.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec set_alpha_beta_delta(Alpha :: float(), Beta :: float(), Delta :: non_neg_integer(), Gateway :: gateway()) -> gateway().
set_alpha_beta_delta(Alpha, Beta, Delta, Gateway) ->
    Gateway#gateway_v2{alpha=normalize_float(scale_shape_param(Alpha)),
                       beta=normalize_float(scale_shape_param(Beta)),
                       delta=Delta}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec last_poc_challenge(Gateway :: gateway()) ->  undefined | non_neg_integer().
last_poc_challenge(Gateway) ->
    Gateway#gateway_v2.last_poc_challenge.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec last_poc_challenge(LastPocChallenge :: non_neg_integer(), Gateway :: gateway()) -> gateway().
last_poc_challenge(LastPocChallenge, Gateway) ->
    Gateway#gateway_v2{last_poc_challenge=LastPocChallenge}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec last_poc_onion_key_hash(Gateway :: gateway()) ->  undefined | binary().
last_poc_onion_key_hash(Gateway) ->
    Gateway#gateway_v2.last_poc_onion_key_hash.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec last_poc_onion_key_hash(LastPocOnionKeyHash :: binary(), Gateway :: gateway()) -> gateway().
last_poc_onion_key_hash(LastPocOnionKeyHash, Gateway) ->
    Gateway#gateway_v2{last_poc_onion_key_hash=LastPocOnionKeyHash}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec nonce(Gateway :: gateway()) -> non_neg_integer().
nonce(Gateway) ->
    Gateway#gateway_v2.nonce.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec nonce(Nonce :: non_neg_integer(), Gateway :: gateway()) -> gateway().
nonce(Nonce, Gateway) ->
    Gateway#gateway_v2{nonce=Nonce}.

-spec mask(Gateway :: gateway(), Ledger :: blockchain_ledger_v1:ledger())-> non_neg_integer().
mask(#gateway_v2{mode = Mode}, Ledger)->
    mask_for_mode(Mode, Ledger).

-spec is_valid_capability(Mode :: atom(), non_neg_integer(), Ledger :: blockchain_ledger_v1:ledger())-> boolean().
is_valid_capability(Mode, Capability, Ledger)->
    Mask = mask_for_mode(Mode, Ledger),
    (Mask band Capability) == Capability.

-spec print(Address :: libp2p_crypto:pubkey_bin(), Gateway :: gateway(),
            Ledger :: blockchain_ledger_v1:ledger()) -> list().
print(Address, Gateway, Ledger) ->
    print(Address, Gateway, Ledger, false).

-spec print(Address :: libp2p_crypto:pubkey_bin(), Gateway :: gateway(),
            Ledger :: blockchain_ledger_v1:ledger(), boolean()) -> list().
print(Address, Gateway, Ledger, Verbose) ->
    %% TODO: This is annoying but it makes printing happy on the CLI
    UndefinedHandleFunc =
        fun(undefined) -> "undefined";
           (I) -> I
        end,
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
    PocUndef =
        fun(undefined) -> "undefined";
           (I) -> Height - I
        end,
    Scoring =
        case Verbose of
            true ->
                {NewAlpha, NewBeta, Score} = score(Address, Gateway, Height, Ledger),
                [
                  {score, Score},
                  {alpha, alpha(Gateway)},
                  {new_alpha, NewAlpha},
                  {beta, beta(Gateway)},
                  {new_beta, NewBeta},
                  {delta, Height - delta(Gateway)}
                ];
            _ -> []
        end,
    [
     {owner_address, libp2p_crypto:pubkey_bin_to_p2p(owner_address(Gateway))},
     {location, UndefinedHandleFunc(location(Gateway))},
     {last_poc_challenge, PocUndef(last_poc_challenge(Gateway))},
     {nonce, nonce(Gateway)}
    ] ++ Scoring.

add_witness({poc_receipt,
             WitnessAddress,
             WitnessGW = #gateway_v2{nonce=Nonce, last_location_nonce = WitnessLocNonce},
             POCWitness,
             Gateway,
             GatwewayAddress,
             Ledger}) ->
    RSSI = blockchain_poc_receipt_v1:signal(POCWitness),
    TS = blockchain_poc_receipt_v1:timestamp(POCWitness),
    Freq = blockchain_poc_receipt_v1:frequency(POCWitness),
    %% before we do anything purge any stale witnesses
    Gateway1 = #gateway_v2{witnesses=Witnesses} = filter_stale_witnesses(GatwewayAddress, Gateway, Ledger, true),
    case lists:keytake(WitnessAddress, 1, Witnesses) of
        {value, {_, Witness=#witness{nonce=Nonce, count=Count, hist=Hist}}, Witnesses1} ->
            %% nonce is the same, increment the count
            Gateway1#gateway_v2{witnesses=lists:sort([{WitnessAddress,
                                                      Witness#witness{count=Count + 1,
                                                                      added_location_nonce = WitnessLocNonce,
                                                                      hist=update_histogram(RSSI, Hist),
                                                                      recent_time=TS}}
                                                     | Witnesses1])};
        _ ->
            %% nonce mismatch or first witnesses for this peer
            %% replace any old witness record with this new one
            Histogram = create_histogram(WitnessGW, Gateway1, Freq),
            Gateway1#gateway_v2{witnesses=lists:sort([{WitnessAddress,
                                                      #witness{count=1,
                                                               nonce=Nonce,
                                                               added_location_nonce = WitnessLocNonce,
                                                               hist=update_histogram(RSSI, Histogram),
                                                               first_time=TS,
                                                               recent_time=TS}}
                                                     | Witnesses])}
    end;
add_witness({poc_witness,
             WitnessAddress,
             WitnessGW = #gateway_v2{nonce=Nonce, last_location_nonce = WitnessLocNonce},
             POCWitness,
             Gateway,
             GatwewayAddress,
             Ledger}) ->
    RSSI = blockchain_poc_witness_v1:signal(POCWitness),
    TS = blockchain_poc_witness_v1:timestamp(POCWitness),
    Freq = blockchain_poc_witness_v1:frequency(POCWitness),
    %% before we do anything purge any stale witnesses
    Gateway1 = #gateway_v2{witnesses=Witnesses} = filter_stale_witnesses(GatwewayAddress, Gateway, Ledger, true),
    case lists:keytake(WitnessAddress, 1, Witnesses) of
        {value, {_, Witness=#witness{nonce=Nonce, count=Count, hist=Hist}}, Witnesses1} ->
            %% nonce is the same, increment the count
            Gateway1#gateway_v2{witnesses=lists:sort([{WitnessAddress,
                                                      Witness#witness{count=Count + 1,
                                                                      added_location_nonce = WitnessLocNonce,
                                                                      hist=update_histogram(RSSI, Hist),
                                                                      recent_time=TS}}
                                                     | Witnesses1])};
        _ ->
            %% nonce mismatch or first witnesses for this peer
            %% replace any old witness record with this new one
            Histogram = create_histogram(WitnessGW, Gateway1, Freq),
            Gateway1#gateway_v2{witnesses=lists:sort([{WitnessAddress,
                                                      #witness{count=1,
                                                               nonce=Nonce,
                                                               added_location_nonce = WitnessLocNonce,
                                                               hist=update_histogram(RSSI, Histogram),
                                                               first_time=TS,
                                                               recent_time=TS}}
                                                     | Witnesses])}
    end.

add_witness(WitnessAddress,
            WitnessGW = #gateway_v2{nonce=Nonce, last_location_nonce = WitnessLocNonce},
            undefined,
            undefined,
            Gateway,
            GatwewayAddress,
            Ledger) ->
    %% before we do anything purge any stale witnesses
    Gateway1 = #gateway_v2{witnesses=Witnesses} = filter_stale_witnesses(GatwewayAddress, Gateway, Ledger, true),
    %% NOTE: This clause is for next hop receipts (which are also considered witnesses) but have no signal and timestamp
    case lists:keytake(WitnessAddress, 1, Witnesses) of
        {value, {_, Witness=#witness{nonce=Nonce, count=Count}}, Witnesses1} ->
            %% nonce is the same, increment the count
            Gateway1#gateway_v2{witnesses=lists:sort([{WitnessAddress,
                                                      Witness#witness{count=Count + 1, added_location_nonce = WitnessLocNonce}}
                                                     | Witnesses1])};
        _ ->
            %% nonce mismatch or first witnesses for this peer
            %% replace any old witness record with this new one
            Gateway1#gateway_v2{witnesses=lists:sort([{WitnessAddress,
                                                      #witness{count=1,
                                                               nonce=Nonce,
                                                               added_location_nonce = WitnessLocNonce,
                                                               hist=create_histogram(WitnessGW, Gateway1)}}
                                                     | Witnesses])}
    end;
add_witness(WitnessAddress,
            WitnessGW = #gateway_v2{nonce=Nonce, last_location_nonce = WitnessLocNonce},
            RSSI,
            TS,
            Gateway,
            GatwewayAddress,
            Ledger) ->
    %% before we do anything purge any stale witnesses
    Gateway1 = #gateway_v2{witnesses=Witnesses} = filter_stale_witnesses(GatwewayAddress, Gateway, Ledger, true),
    case lists:keytake(WitnessAddress, 1, Witnesses) of
        {value, {_, Witness=#witness{nonce=Nonce, count=Count, hist=Hist}}, Witnesses1} ->
            %% nonce is the same, increment the count
            Gateway1#gateway_v2{witnesses=lists:sort([{WitnessAddress,
                                                      Witness#witness{count=Count + 1,
                                                                      added_location_nonce = WitnessLocNonce,
                                                                      hist=update_histogram(RSSI, Hist),
                                                                      recent_time=TS}}
                                                     | Witnesses1])};
        _ ->
            %% nonce mismatch or first witnesses for this peer
            %% replace any old witness record with this new one
            Histogram = create_histogram(WitnessGW, Gateway1),
            Gateway1#gateway_v2{witnesses=lists:sort([{WitnessAddress,
                                                      #witness{count=1,
                                                               nonce=Nonce,
                                                               added_location_nonce = WitnessLocNonce,
                                                               hist=update_histogram(RSSI, Histogram),
                                                               first_time=TS,
                                                               recent_time=TS}}
                                                     | Witnesses])}
    end.

create_histogram(#gateway_v2{location=WitnessLoc}=_WitnessGW,
                 #gateway_v2{location=GatewayLoc}=_Gateway,
                 Freq) ->
    %% Get the free space path loss
    FreeSpacePathLoss = blockchain_utils:free_space_path_loss(WitnessLoc, GatewayLoc, Freq),
    MinRcvSig = blockchain_utils:min_rcv_sig(FreeSpacePathLoss),
    %% Maximum number of bins in the histogram
    NumBins = 10,
    %% Spacing between histogram keys (x axis)
    StepSize = ((-132 + abs(MinRcvSig))/(NumBins - 1)),
    %% Construct a custom histogram around the expected path loss
    lists:sort([ {28, 0} | [ {trunc(MinRcvSig + (N * StepSize)), 0} || N <- lists:seq(0, (NumBins - 1))]]).

create_histogram(#gateway_v2{location=WitnessLoc}=_WitnessGW,
                 #gateway_v2{location=GatewayLoc}=_Gateway) ->
    %% Get the free space path loss
    FreeSpacePathLoss = blockchain_utils:free_space_path_loss(WitnessLoc, GatewayLoc),
    %% Maximum number of bins in the histogram
    NumBins = 10,
    %% Spacing between histogram keys (x axis)
    StepSize = ((-132 + abs(FreeSpacePathLoss))/(NumBins - 1)),
    %% Construct a custom histogram around the expected path loss
    lists:sort([ {28, 0} | [ {trunc(FreeSpacePathLoss + (N * StepSize)), 0} || N <- lists:seq(0, (NumBins - 1))]]).

update_histogram(Val, Histogram0) ->
    Keys = lists:reverse(lists:sort(element(1, lists:unzip(Histogram0)))),
    Histogram = maps:from_list(Histogram0),
    Histogram1 = update_histogram_(Val, Keys, Histogram),
    lists:sort(maps:to_list(Histogram1)).

update_histogram_(_Val, [LastKey], Histogram) ->
    maps:put(LastKey, maps:get(LastKey, Histogram, 0) + 1, Histogram);
update_histogram_(Val, [Key | [Bound | _]], Histogram) when Val > Bound ->
    maps:put(Key, maps:get(Key, Histogram, 0) + 1, Histogram);
update_histogram_(Val, [_ | Tail], Histogram) ->
    update_histogram_(Val, Tail, Histogram).

-spec clear_witnesses(gateway()) -> gateway().
clear_witnesses(Gateway) ->
    Gateway#gateway_v2{witnesses=[]}.

-spec remove_witness(gateway(), libp2p_crypto:pubkey_bin()) -> gateway().
remove_witness(Gateway, WitnessAddr) ->
    Gateway#gateway_v2{witnesses=lists:keydelete(WitnessAddr, 1, Gateway#gateway_v2.witnesses)}.

-spec has_witness(gateway(), libp2p_crypto:pubkey_bin()) -> boolean().
has_witness(#gateway_v2{witnesses=Witnesses}, WitnessAddr) ->
    case lists:keyfind(WitnessAddr, 1, Witnesses) of
        false -> false;
        _ -> true
    end.

-spec witnesses(gateway()) -> #{libp2p_crypto:pubkey_bin() => gateway_witness()}.
witnesses(Gateway) ->
    maps:from_list(Gateway#gateway_v2.witnesses).

-spec witnesses(libp2p_crypto:pubkey_bin(), gateway(), blockchain:ledger()) -> #{libp2p_crypto:pubkey_bin() => gateway_witness()}.
witnesses(GatewayAddr, Gateway, Ledger) ->
    FilteredGW = filter_stale_witnesses(GatewayAddr, Gateway, Ledger, false),
    maps:from_list(FilteredGW#gateway_v2.witnesses).

-spec witnesses_plain(gateway()) -> [{libp2p_crypto:pubkey_bin(), gateway_witness()}].
witnesses_plain(Gateway) ->
    Gateway#gateway_v2.witnesses.

-spec witness_hist(gateway_witness()) -> erlang:error(no_histogram) | histogram().
witness_hist(Witness) ->
    maps:from_list(Witness#witness.hist).

-spec witness_recent_time(gateway_witness()) -> undefined | non_neg_integer().
witness_recent_time(Witness) ->
    Witness#witness.recent_time.

-spec witness_first_time(gateway_witness()) -> undefined | non_neg_integer().
witness_first_time(Witness) ->
    Witness#witness.first_time.

-spec oui(gateway()) -> undefined | pos_integer().
oui(Gateway) ->
    Gateway#gateway_v2.oui.

-spec oui(pos_integer() | undefined, gateway()) -> gateway().
oui(OUI, Gateway) ->
    Gateway#gateway_v2{oui=OUI}.

%%--------------------------------------------------------------------
%% @doc
%% Version 2
%% @end
%%--------------------------------------------------------------------
-spec serialize(Gateway :: gateway()) -> binary().
serialize(Gw) ->
    Neighbors = neighbors(Gw),
    Gw1 = neighbors(lists:usort(Neighbors), Gw),
    BinGw = erlang:term_to_binary(Gw1, [compressed]),
    <<2, BinGw/binary>>.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-dialyzer([
    {nowarn_function, deserialize/1}
]).

-spec deserialize(binary()) -> gateway().
deserialize(<<1, Bin/binary>>) ->
    V1 = erlang:binary_to_term(Bin),
    convert(V1);
deserialize(<<2, Bin/binary>>) ->
    Gw = erlang:binary_to_term(Bin),
    Gw1 =
        case size(Gw) of
            %% pre-oui upgrade
            12 ->
                L = tuple_to_list(Gw),
                %% add an undefined OUI slot
                %% and add defaults for gain, elevation,  mode and last_location_nonce
                L1 = lists:append(L, [undefined, ?DEFAULT_GAIN, ?DEFAULT_ELEVATION, full, 0]),
                G1 = list_to_tuple(L1),
                neighbors([], G1);
            13 ->
                %% pre gain, elevation, mode update and last_location_nonce
                L = tuple_to_list(Gw),
                %% add defaults for gain, elevation and mode
                L1 = lists:append(L, [?DEFAULT_GAIN, ?DEFAULT_ELEVATION, full, 0]),
                list_to_tuple(L1);
            16 ->
                %% pre last_location_nonce
                L = tuple_to_list(Gw),
                %% add default last_location_nonce
                L1 = lists:append(L, [0]),
                list_to_tuple(L1);
            17 ->
                Gw

        end,
    Neighbors = neighbors(Gw1),
    Gw2 = neighbors(lists:usort(Neighbors), Gw1),
    Witnesses = Gw2#gateway_v2.witnesses,
    WitnessUpgradeFun =
        fun(Witness)->
            case size(Witness) of
                7 ->
                    %% pre added_location_nonce upgrade
                    WL = tuple_to_list(Witness),
                    WL1 = lists:append(WL, [0]),
                    list_to_tuple(WL1);
                8 ->
                    Witness
            end
        end,
    Witnesses1 =
        case is_map(Witnesses) of
            true ->
                lists:sort(
                  maps:to_list(
                    maps:map(
                      fun(_K, W) ->
                          W1 = WitnessUpgradeFun(W),
                          #witness{hist = Hist} = W1,
                          W1#witness{hist = lists:sort(maps:to_list(Hist))}
                      end,
                      Witnesses)));
            false ->
                lists:sort(lists:foldl(
                    fun({K, W}, Acc) ->
                        [{K, WitnessUpgradeFun(W)} | Acc]
                    end, [], Witnesses))
        end,
    Gw2#gateway_v2{witnesses = Witnesses1}.

%% OK to include here, v1 should now be immutable.
-record(gateway_v1, {
    owner_address :: libp2p_crypto:pubkey_bin(),
    location :: undefined | pos_integer(),
    alpha = 1.0 :: float(),
    beta = 1.0 :: float(),
    delta :: non_neg_integer(),
    last_poc_challenge :: undefined | non_neg_integer(),
    last_poc_onion_key_hash :: undefined | binary(),
    nonce = 0 :: non_neg_integer(),
    version = 0 :: non_neg_integer()
}).

convert(#gateway_v1{
          owner_address = Owner,
          location = Location,
          alpha = Alpha,
          beta = Beta,
          delta = Delta,
          last_poc_challenge = LastPoC,
          last_poc_onion_key_hash = LastHash,
          nonce = Nonce,
          version = Version}) ->
    #gateway_v2{
       owner_address = Owner,
       location = Location,
       alpha = Alpha,
       beta = Beta,
       delta = Delta,
       last_poc_challenge = LastPoC,
       last_poc_onion_key_hash = LastHash,
       nonce = Nonce,
       version = Version,
       %% this gets set in the upgrade path
       neighbors = []}.

-spec mask_for_mode(Mode :: atom(), Ledger :: blockchain_ledger_v1:ledger()) -> non_neg_integer().
mask_for_mode(dataonly, Ledger)->
    case blockchain:config(?dataonly_gateway_capabilities_mask, Ledger) of
        {error, not_found} -> ?GW_CAPABILITIES_DATAONLY_GATEWAY_V1;
        {ok, V} -> V
    end;
mask_for_mode(light, Ledger)->
    case blockchain:config(?light_gateway_capabilities_mask, Ledger) of
        {error, not_found} -> ?GW_CAPABILITIES_LIGHT_GATEWAY_V1;
        {ok, V} -> V
    end;
mask_for_mode(full, Ledger)->
    case blockchain:config(?full_gateway_capabilities_mask, Ledger) of
        {error, not_found} -> ?GW_CAPABILITIES_FULL_GATEWAY_V1;
        {ok, V} -> V
    end.

-spec filter_stale_witnesses(libp2p_crypto:pubkey_bin(), gateway(), blockchain_ledger_v2:ledger(), boolean()) -> gateway().
filter_stale_witnesses(GatewayBin, GatewayToPurge = #gateway_v2{witnesses = Witnesses}, Ledger, PurgeGW)->
    PurgedGW =
        lists:foldl(fun
                      ({WitnessPubkeyBin, Witness}, AccGW)->
                          %% get the witness GW
                          %% check when it was last updated location and whether it has done so since this witness report was added
                          %% to the GW being purged
                          {ok, WitnessGW} = blockchain_ledger_v1:find_gateway_info(WitnessPubkeyBin, Ledger),
                          case WitnessGW#gateway_v2.last_location_nonce > Witness#witness.added_location_nonce of
                                  true ->
                                      blockchain_ledger_gateway_v2:remove_witness(AccGW, WitnessPubkeyBin);
                                  false ->
                                      AccGW
                          end
                  end,
            GatewayToPurge, Witnesses),
    case PurgeGW of
        true ->
            ok = blockchain_ledger_v1:update_gateway(GatewayToPurge, PurgedGW, GatewayBin, Ledger);
        _ ->
            noop
    end,
    PurgedGW.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Gw = #gateway_v2{
        owner_address = <<"owner_address">>,
        location = 12,
        last_poc_challenge = undefined,
        last_poc_onion_key_hash = undefined,
        nonce = 0,
        delta=1,
        mode=full
    },
    ?assertEqual(Gw, new(<<"owner_address">>, 12)).

owner_address_test() ->
    Gw = new(<<"owner_address">>, 12, full),
    ?assertEqual(<<"owner_address">>, owner_address(Gw)),
    ?assertEqual(<<"owner_address2">>, owner_address(owner_address(<<"owner_address2">>, Gw))).

location_test() ->
    Gw = new(<<"owner_address">>, 12, full),
    ?assertEqual(12, location(Gw)),
    ?assertEqual(13, location(location(13, Gw))).

mode_test() ->
    Gw = new(<<"owner_address">>, 12),
    ?assertEqual(full, mode(Gw)),
    ?assertEqual(full, mode(mode(full, Gw))).

mode_full_test() ->
    Gw = new(<<"owner_address">>, 12, full),
    ?assertEqual(full, mode(Gw)),
    ?assertEqual(full, mode(mode(full, Gw))).

mode_dataonly_test() ->
    Gw = new(<<"owner_address">>, 12, dataonly),
    ?assertEqual(dataonly, mode(Gw)),
    ?assertEqual(dataonly, mode(mode(dataonly, Gw))).

mode_light_test() ->
    Gw = new(<<"owner_address">>, 12, light),
    ?assertEqual(light, mode(Gw)),
    ?assertEqual(light, mode(mode(light, Gw))).

score_test_() ->
    {
        timeout,
        10,
        fun() ->
            Gw = new(<<"owner_address">>, 12, full),
            fake_config(),
            ?assertEqual({1.0, 1.0, 0.25}, score(<<"score_test_gw">>, Gw, 12, fake_ledger)),
            fake_config_cleanup(),
            blockchain_score_cache:stop()
        end
    }.

score_decay_test_() ->
    {
        timeout,
        10,
        fun() ->
            Gw0 = new(<<"owner_address">>, 1, full),
            Gw1 = set_alpha_beta_delta(1.1, 1.0, 300, Gw0),
            fake_config(),
            {_, _, A} = score(<<"score_decay_test_gw">>, Gw1, 1000, fake_ledger),
            ?assertEqual(normalize_float(A), A),
            ?assertEqual({1.0, 1.0, 0.25}, score(<<"score_decay_test_gw">>, Gw1, 1000, fake_ledger)),
            fake_config_cleanup(),
            blockchain_score_cache:stop()
        end
    }.

score_decay2_test_() ->
    {
        timeout,
        10,
        fun() ->
            Gw0 = new(<<"owner_address">>, 1, full),
            Gw1 = set_alpha_beta_delta(1.1, 10.0, 300, Gw0),
            fake_config(),
            {Alpha, Beta, Score} = score(<<"score_decay2_test">>, Gw1, 1000, fake_ledger),
            ?assertEqual(1.0, Alpha),
            ?assert(Beta < 10.0),
            ?assert(Score < 0.25),
            fake_config_cleanup(),
            blockchain_score_cache:stop()
        end
    }.

last_poc_challenge_test() ->
    Gw = new(<<"owner_address">>, 12, full),
    ?assertEqual(undefined, last_poc_challenge(Gw)),
    ?assertEqual(123, last_poc_challenge(last_poc_challenge(123, Gw))).

last_poc_onion_key_hash_test() ->
    Gw = new(<<"owner_address">>, 12, full),
    ?assertEqual(undefined, last_poc_onion_key_hash(Gw)),
    ?assertEqual(<<"onion_key_hash">>, last_poc_onion_key_hash(last_poc_onion_key_hash(<<"onion_key_hash">>, Gw))).

nonce_test() ->
    Gw = new(<<"owner_address">>, 12, full),
    ?assertEqual(0, nonce(Gw)),
    ?assertEqual(1, nonce(nonce(1, Gw))).


fake_config() ->
    meck:expect(blockchain_event,
                add_handler,
                fun(_) -> ok end),
    meck:expect(blockchain_worker,
                blockchain,
                fun() -> undefined end),
    {ok, Pid} = blockchain_score_cache:start_link(),
    meck:expect(blockchain,
                config,
                fun(alpha_decay, _) ->
                        {ok, 0.007};
                   (beta_decay, _) ->
                        {ok, 0.0005};
                   (max_staleness, _) ->
                        {ok, 100000};
                   (election_version, _) ->
                        {ok, 4}
                end),
    Pid.

fake_config_cleanup() ->
    meck:unload(blockchain_event),
    meck:unload(blockchain_worker),
    meck:unload(blockchain).

-endif.
