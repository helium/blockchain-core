%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Ledger Gateway ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_ledger_gateway_v1).

-export([
    new/2, new/4,
    owner_address/1, owner_address/2,
    location/1, location/2,
    score/1, score/2, bayes_score/1,
    last_poc_challenge/1, last_poc_challenge/2,
    last_poc_onion_key_hash/1, last_poc_onion_key_hash/2,
    nonce/1, nonce/2,
    print/1,
    serialize/1, deserialize/1,
    alpha/1, alpha/2,
    beta/1, beta/2,
    last_delta_update/1, last_delta_update/2
]).

-include("blockchain.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(gateway_v1, {
    owner_address :: libp2p_crypto:pubkey_bin(),
    location :: undefined | pos_integer(),
    alpha = 1.0 :: float(),
    beta = 1.0 :: float(),
    score = 0.0 :: float(),
    last_delta_update :: undefined | non_neg_integer(),
    last_poc_challenge :: undefined | non_neg_integer(),
    last_poc_onion_key_hash :: undefined | binary(),
    nonce = 0 :: non_neg_integer()
}).

-type gateway() :: #gateway_v1{}.
-export_type([gateway/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(OwnerAddress :: libp2p_crypto:pubkey_bin(),
          Location :: pos_integer() | undefined) -> gateway().
new(OwnerAddress, Location) ->
    #gateway_v1{
        owner_address=OwnerAddress,
        location=Location
    }.

-spec new(OwnerAddress :: libp2p_crypto:pubkey_bin(),
          Location :: pos_integer() | undefined,
          Nonce :: non_neg_integer(),
          Score :: float()) -> gateway().
new(OwnerAddress, Location, Nonce, Score) ->
    #gateway_v1{
        owner_address=OwnerAddress,
        location=Location,
        score=Score,
        nonce=Nonce
    }.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec owner_address(Gateway :: gateway()) -> libp2p_crypto:pubkey_bin().
owner_address(Gateway) ->
    Gateway#gateway_v1.owner_address.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec owner_address(OwnerAddress :: libp2p_crypto:pubkey_bin(),
                    Gateway :: gateway()) -> gateway().
owner_address(OwnerAddress, Gateway) ->
    Gateway#gateway_v1{owner_address=OwnerAddress}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec location(Gateway :: gateway()) ->  undefined | pos_integer().
location(Gateway) ->
    Gateway#gateway_v1.location.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec location(Location :: pos_integer(), Gateway :: gateway()) -> gateway().
location(Location, Gateway) ->
    Gateway#gateway_v1{location=Location}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec score(Gateway :: gateway()) -> float().
score(Gateway) ->
    Gateway#gateway_v1.score.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec score(Score :: float(), Gateway :: gateway()) -> gateway().
score(Score, Gateway) ->
    Gateway#gateway_v1{score=Score}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec bayes_score(Gateway :: gateway()) -> float().
bayes_score(#gateway_v1{alpha=1.0, beta=1.0}) ->
    0.5;
bayes_score(#gateway_v1{alpha=Alpha, beta=Beta}) ->
    RV1 = erlang_stats:qbeta(0.25, Alpha, Beta),
    RV2 = erlang_stats:qbeta(0.75, Alpha, Beta),
    IQR = RV2 - RV1,
    Mean = 1 / (1 + Beta/Alpha),
    Mean * (1 - IQR).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec alpha(Gateway :: gateway()) -> float().
alpha(Gateway) ->
    Gateway#gateway_v1.alpha.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec alpha(Alpha :: float(), Gateway :: gateway()) -> gateway().
alpha(Alpha, Gateway) ->
    Gateway#gateway_v1{alpha=Alpha}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec beta(Beta :: float(), Gateway :: gateway()) -> gateway().
beta(Beta, Gateway) ->
    Gateway#gateway_v1{beta=Beta}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec beta(Gateway :: gateway()) -> float().
beta(Gateway) ->
    Gateway#gateway_v1.beta.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec last_delta_update(Gateway :: gateway()) -> undefined | non_neg_integer().
last_delta_update(Gateway) ->
    Gateway#gateway_v1.last_delta_update.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec last_delta_update(LastDeltaUpdate :: non_neg_integer(), Gateway :: gateway()) -> gateway().
last_delta_update(LastDeltaUpdate, Gateway) ->
    Gateway#gateway_v1{last_delta_update=LastDeltaUpdate}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec last_poc_challenge(Gateway :: gateway()) ->  undefined | non_neg_integer().
last_poc_challenge(Gateway) ->
    Gateway#gateway_v1.last_poc_challenge.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec last_poc_challenge(LastPocChallenge :: non_neg_integer(), Gateway :: gateway()) -> gateway().
last_poc_challenge(LastPocChallenge, Gateway) ->
    Gateway#gateway_v1{last_poc_challenge=LastPocChallenge}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec last_poc_onion_key_hash(Gateway :: gateway()) ->  undefined | binary().
last_poc_onion_key_hash(Gateway) ->
    Gateway#gateway_v1.last_poc_onion_key_hash.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec last_poc_onion_key_hash(LastPocOnionKeyHash :: binary(), Gateway :: gateway()) -> gateway().
last_poc_onion_key_hash(LastPocOnionKeyHash, Gateway) ->
    Gateway#gateway_v1{last_poc_onion_key_hash=LastPocOnionKeyHash}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec nonce(Gateway :: gateway()) -> non_neg_integer().
nonce(Gateway) ->
    Gateway#gateway_v1.nonce.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec nonce(Nonce :: non_neg_integer(), Gateway :: gateway()) -> gateway().
nonce(Nonce, Gateway) ->
    Gateway#gateway_v1{nonce=Nonce}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec print(Gateway :: gateway()) -> list().
print(Gateway) ->
    %% TODO: This is annoying but it makes printing happy on the CLI
    UndefinedHandleFunc =
        fun(undefined) -> "undefined";
           (I) -> I
        end,
    [
        {owner_address, libp2p_crypto:pubkey_bin_to_p2p(owner_address(Gateway))},
        {location, UndefinedHandleFunc(location(Gateway))},
        {last_poc_challenge, UndefinedHandleFunc(last_poc_challenge(Gateway))},
        {nonce, nonce(Gateway)},
        {score, score(Gateway)}
    ].

%%--------------------------------------------------------------------
%% @doc
%% Version 1
%% @end
%%--------------------------------------------------------------------
-spec serialize(Gateway :: gateway()) -> binary().
serialize(Gw) ->
    BinGw = erlang:term_to_binary(Gw),
    <<1, BinGw/binary>>.

%%--------------------------------------------------------------------
%% @doc
%% Later _ could becomre 1, 2, 3 for different versions.
%% @end
%%--------------------------------------------------------------------
-spec deserialize(binary()) -> gateway().
deserialize(<<_:1/binary, Bin/binary>>) ->
    erlang:binary_to_term(Bin).

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Gw = #gateway_v1{
        owner_address = <<"owner_address">>,
        score = 0.0,
        location = 12,
        last_poc_challenge = undefined,
        last_poc_onion_key_hash = undefined,
        nonce = 0
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

score_test() ->
    Gw = new(<<"owner_address">>, 12),
    ?assertEqual(0.0, score(Gw)),
    ?assertEqual(1.0, score(score(1.0, Gw))).

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

-endif.
