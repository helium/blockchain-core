%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Ledger Gateway ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_ledger_gateway_v1).

-export([
    new/2, new/6,
    owner_address/1, owner_address/2,
    location/1, location/2,
    last_poc_challenge/1, last_poc_challenge/2,
    last_poc_hash/1, last_poc_hash/2,
    nonce/1, nonce/2,
    score/1, score/2,
    print/1,
    serialize/1, deserialize/1
]).

-include("blockchain.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(gateway_v1, {
    owner_address :: libp2p_crypto:pubkey_bin(),
    location :: undefined | pos_integer(),
    last_poc_challenge :: undefined | non_neg_integer(),
    last_poc_hash :: undefined | binary(),
    nonce = 0 :: non_neg_integer(),
    score = 0.0 :: float()
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
          LastPocChallenge :: non_neg_integer() | undefined,
          LastPocHash :: binary() | undefined,
          Nonce :: non_neg_integer(),
          Score :: float()) -> gateway().
new(OwnerAddress, Location, LastPocChallenge, LastPocHash, Nonce, Score) ->
    #gateway_v1{
        owner_address=OwnerAddress,
        location=Location,
        last_poc_challenge=LastPocChallenge,
        last_poc_hash=LastPocHash,
        nonce=Nonce,
        score=Score
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
-spec last_poc_hash(Gateway :: gateway()) ->  undefined | binary().
last_poc_hash(Gateway) ->
    Gateway#gateway_v1.last_poc_hash.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec last_poc_hash(LastPocHash :: binary(), Gateway :: gateway()) -> gateway().
last_poc_hash(LastPocHash, Gateway) ->
    Gateway#gateway_v1{last_poc_hash=LastPocHash}.

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
        {last_poc_hash, UndefinedHandleFunc(last_poc_hash(Gateway))},
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
        location = 12,
        last_poc_challenge = undefined,
        nonce = 0,
        score = 0.0
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

nonce_test() ->
    Gw = new(<<"owner_address">>, 12),
    ?assertEqual(0, nonce(Gw)),
    ?assertEqual(1, nonce(nonce(1, Gw))).

score_test() ->
    Gw = new(<<"owner_address">>, 12),
    ?assertEqual(0.0, score(Gw)),
    ?assertEqual(1.0, score(score(1.0, Gw))).

-endif.
