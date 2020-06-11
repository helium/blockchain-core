%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Ledger Gateway V3 ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_ledger_gateway_v3).

-export([
    new/2, new/3,
    owner_address/1, owner_address/2,
    location/1, location/2,
    version/1, version/2,
    last_poc_challenge/1, last_poc_challenge/2,
    last_poc_onion_key_hash/1, last_poc_onion_key_hash/2,
    nonce/1, nonce/2,
    height_added_at/1, height_added_at/2,
    print/3, print/4,
    serialize/1, deserialize/1,
    oui/1, oui/2,

    has_witness/2,
    witnesses/1,

    convert/1
]).

-import(blockchain_utils, [normalize_float/1]).

-include("blockchain.hrl").
-include("blockchain_vars.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(gateway_v3, {
    owner_address :: libp2p_crypto:pubkey_bin(),
    location :: undefined | pos_integer(),
    last_poc_challenge :: undefined | non_neg_integer(),
    last_poc_onion_key_hash :: undefined | binary(),
    nonce = 0 :: non_neg_integer(),
    version = 0 :: non_neg_integer(),
    witnesses = #{} ::  witnesses(),
    height_added_at :: undefined | non_neg_integer(),
    oui = undefined :: undefined | pos_integer()
}).

-type gateway() :: #gateway_v3{}.
-type witnesses() :: #{libp2p_crypto:pubkey_bin() => blockchain_ledger_gateway_witness_v2:witness()}.
-export_type([gateway/0]).

-spec new(OwnerAddress :: libp2p_crypto:pubkey_bin(),
          Location :: pos_integer() | undefined) -> gateway().
new(OwnerAddress, Location) ->
    #gateway_v3{
        owner_address=OwnerAddress,
        location=Location
    }.

-spec new(OwnerAddress :: libp2p_crypto:pubkey_bin(),
          Location :: pos_integer() | undefined,
          Nonce :: non_neg_integer()) -> gateway().
new(OwnerAddress, Location, Nonce) ->
    #gateway_v3{
        owner_address=OwnerAddress,
        location=Location,
        nonce=Nonce
    }.

-spec owner_address(Gateway :: gateway()) -> libp2p_crypto:pubkey_bin().
owner_address(Gateway) ->
    Gateway#gateway_v3.owner_address.

-spec owner_address(OwnerAddress :: libp2p_crypto:pubkey_bin(),
                    Gateway :: gateway()) -> gateway().
owner_address(OwnerAddress, Gateway) ->
    Gateway#gateway_v3{owner_address=OwnerAddress}.

-spec location(Gateway :: gateway()) ->  undefined | pos_integer().
location(Gateway) ->
    Gateway#gateway_v3.location.

-spec location(Location :: pos_integer(), Gateway :: gateway()) -> gateway().
location(Location, Gateway) ->
    Gateway#gateway_v3{location=Location}.

-spec version(Gateway :: gateway()) -> pos_integer().
version(Gateway) ->
    Gateway#gateway_v3.version.

-spec version(Version :: pos_integer(), Gateway :: gateway()) -> gateway().
version(Version, Gateway) ->
    Gateway#gateway_v3{version = Version}.

-spec last_poc_challenge(Gateway :: gateway()) ->  undefined | non_neg_integer().
last_poc_challenge(Gateway) ->
    Gateway#gateway_v3.last_poc_challenge.

-spec last_poc_challenge(LastPocChallenge :: non_neg_integer(), Gateway :: gateway()) -> gateway().
last_poc_challenge(LastPocChallenge, Gateway) ->
    Gateway#gateway_v3{last_poc_challenge=LastPocChallenge}.

-spec last_poc_onion_key_hash(Gateway :: gateway()) ->  undefined | binary().
last_poc_onion_key_hash(Gateway) ->
    Gateway#gateway_v3.last_poc_onion_key_hash.

-spec last_poc_onion_key_hash(LastPocOnionKeyHash :: binary(), Gateway :: gateway()) -> gateway().
last_poc_onion_key_hash(LastPocOnionKeyHash, Gateway) ->
    Gateway#gateway_v3{last_poc_onion_key_hash=LastPocOnionKeyHash}.

-spec nonce(Gateway :: gateway()) -> non_neg_integer().
nonce(Gateway) ->
    Gateway#gateway_v3.nonce.

-spec nonce(Nonce :: non_neg_integer(), Gateway :: gateway()) -> gateway().
nonce(Nonce, Gateway) ->
    Gateway#gateway_v3{nonce=Nonce}.

-spec height_added_at(Gateway :: gateway()) -> non_neg_integer().
height_added_at(Gateway) ->
    Gateway#gateway_v3.height_added_at.

-spec height_added_at(HeightAddedAt :: non_neg_integer(), Gateway :: gateway()) -> gateway().
height_added_at(HeightAddedAt, Gateway) ->
    Gateway#gateway_v3{height_added_at=HeightAddedAt}.

-spec oui(OUI :: non_neg_integer(), Gateway :: gateway()) -> gateway().
oui(OUI, Gateway) ->
    Gateway#gateway_v3{oui=OUI}.

-spec oui(Gateway :: gateway()) -> pos_integer().
oui(Gateway) ->
    Gateway#gateway_v3.oui.

-spec print(Address :: libp2p_crypto:pubkey_bin(), Gateway :: gateway(),
            Ledger :: blockchain_ledger_v1:ledger()) -> list().
print(Address, Gateway, Ledger) ->
    print(Address, Gateway, Ledger, false).

-spec print(Address :: libp2p_crypto:pubkey_bin(), Gateway :: gateway(),
            Ledger :: blockchain_ledger_v1:ledger(), boolean()) -> list().
print(_Address, Gateway, Ledger, _Verbose) ->
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
    [
     {owner_address, libp2p_crypto:pubkey_bin_to_p2p(owner_address(Gateway))},
     {location, UndefinedHandleFunc(location(Gateway))},
     {last_poc_challenge, PocUndef(last_poc_challenge(Gateway))},
     {nonce, nonce(Gateway)},
     {height_added_at, height_added_at(Gateway)},
     {witnesses, witnesses(Gateway)},
     {oui, oui(Gateway)}
    ].

-spec has_witness(Gateway :: gateway(), WitnessPubkeyBin :: libp2p_crypto:pubkey_bin()) -> boolean().
has_witness(#gateway_v3{witnesses=Witnesses}, WitnessPubkeyBin) ->
    lists:member(WitnessPubkeyBin, maps:keys(Witnesses)).

-spec witnesses(gateway()) -> witnesses().
witnesses(Gateway) ->
    Gateway#gateway_v3.witnesses.

%%--------------------------------------------------------------------
%% @doc
%% Version 3
%% @end
%%--------------------------------------------------------------------
-spec serialize(Gateway :: gateway()) -> binary().
serialize(Gw) ->
    BinGw = erlang:term_to_binary(Gw),
    <<3, BinGw/binary>>.

-spec deserialize(binary()) -> gateway().
deserialize(<<3, Bin/binary>>) ->
    erlang:binary_to_term(Bin);
deserialize(Bin) ->
    %% gateway_v2 handles deserialization of v2 and v1 gateways
    convert(blockchain_ledger_gateway_v2:deserialize(Bin)).

%% OK to include here, v1 and v2 should now be immutable.
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
    witnesses = #{} :: witnesses(),
    oui = undefined :: undefined | pos_integer()
}).

-spec convert(blockchain_ledger_gateway_v2:gateway() |
              blockchain_ledger_gateway_v1:gateway()) -> ?MODULE:gateway().
convert(GWv1) when is_record(GWv1, gateway_v1, 10) ->
    ?MODULE:convert(blockchain_ledger_gateway_v2:convert(GWv1));
convert(#gateway_v2{
          owner_address = Owner,
          location = Location,
          last_poc_challenge = LastPoC,
          last_poc_onion_key_hash = LastHash,
          nonce = Nonce,
          version = Version,
          witnesses = _Witnesses,
          neighbors = _N,
          oui = OUI}) ->
    #gateway_v3{
       owner_address = Owner,
       location = Location,
       last_poc_challenge = LastPoC,
       last_poc_onion_key_hash = LastHash,
       nonce = Nonce,
       version = Version,
       %% TODO: convert gateway_v2 witnesses to gateway_v3 witnesses
       %% Or maybe it's actually better to just drop them and start fresh
       witnesses = #{},
       oui = OUI
      }.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Gw = #gateway_v3{
        owner_address = <<"owner_address">>,
        location = 12,
        last_poc_challenge = undefined,
        last_poc_onion_key_hash = undefined,
        nonce = 0,
        delta=1
    },
    ?assertEqual(Gw, new(<<"owner_address">>, 12)).

owner_address_test() ->
    Gw = new(<<"owner_address">>, 12),
    ?assertEqual(<<"owner_address">>, owner_address(Gw)),
    ?assertEqual(<<"owner_address2">>, owner_address(owner_address(<<"owner_address2">>, Gw))).

location_test() ->
    Gw = new(<<"owner_address">>, 12),
    ?assertEqual(12, location(Gw)),
    ?assertEqual(13, location(location(13, Gw))).

last_poc_challenge_test() ->
    Gw = new(<<"owner_address">>, 12),
    ?assertEqual(undefined, last_poc_challenge(Gw)),
    ?assertEqual(123, last_poc_challenge(last_poc_challenge(123, Gw))).

last_poc_onion_key_hash_test() ->
    Gw = new(<<"owner_address">>, 12),
    ?assertEqual(undefined, last_poc_onion_key_hash(Gw)),
    ?assertEqual(<<"onion_key_hash">>, last_poc_onion_key_hash(last_poc_onion_key_hash(<<"onion_key_hash">>, Gw))).

nonce_test() ->
    Gw = new(<<"owner_address">>, 12),
    ?assertEqual(0, nonce(Gw)),
    ?assertEqual(1, nonce(nonce(1, Gw))).

height_added_at_test() ->
    Gw0 = new(<<"owner_address">>, 12),
    ?assertEqual(undefined, height_added_at(Gw0)),
    Gw = height_added_at(100, Gw0),
    ?assertEqual(100, height_added_at(Gw)).

convert_v1_to_v3_test() ->
    GwV1 = blockchain_ledger_gateway_v1:new(<<"owner_address">>, 12),
    GwV3 = ?MODULE:convert(GwV1),
    ?assertEqual([], ?MODULE:witnesses(GwV3)),
    ok.

convert_v2_to_v3_test() ->
    GwV2 = blockchain_ledger_gateway_v2:new(<<"owner_address">>, 12),
    GwV3 = ?MODULE:convert(GwV2),
    ?assertEqual([], ?MODULE:witnesses(GwV3)),
    ok.

-endif.
