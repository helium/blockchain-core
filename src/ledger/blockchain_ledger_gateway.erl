%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Ledger Gateway ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_ledger_gateway).

-export([
    new/2
    ,owner_address/1, owner_address/2
    ,location/1, location/2
    ,last_poc_challenge/1, last_poc_challenge/2
    ,nonce/1, nonce/2
    ,score/1, score/2
]).

-include("blockchain.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(gateway, {
    owner_address :: libp2p_crypto:address()
    ,location :: undefined | pos_integer()
    ,last_poc_challenge :: undefined | non_neg_integer()
    ,nonce = 0 :: non_neg_integer()
    ,score = 0.0 :: float()
}).

-type gateway() :: #gateway{}.

-export_type([gateway/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(libp2p_crypto:address(), pos_integer() | undefined) -> gateway().
new(OwnerAddress, Location) ->
    #gateway{
        owner_address=OwnerAddress
        ,location=Location
    }.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec owner_address(gateway()) -> libp2p_crypto:address().
owner_address(Gateway) ->
    Gateway#gateway.owner_address.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec owner_address(libp2p_crypto:address(), gateway()) -> gateway().
owner_address(OwnerAddress, Gateway) ->
    Gateway#gateway{owner_address=OwnerAddress}.


%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec location(gateway()) ->  undefined | pos_integer().
location(Gateway) ->
    Gateway#gateway.location.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec location(pos_integer(), gateway()) -> gateway().
location(Location, Gateway) ->
    Gateway#gateway{location=Location}.


%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec last_poc_challenge(gateway()) ->  undefined | non_neg_integer().
last_poc_challenge(Gateway) ->
    Gateway#gateway.last_poc_challenge.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec last_poc_challenge(non_neg_integer(), gateway()) -> gateway().
last_poc_challenge(LastPocChallenge, Gateway) ->
    Gateway#gateway{last_poc_challenge=LastPocChallenge}.


%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec nonce(gateway()) -> non_neg_integer().
nonce(Gateway) ->
    Gateway#gateway.nonce.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec nonce(non_neg_integer(), gateway()) -> gateway().
nonce(Nonce, Gateway) ->
    Gateway#gateway{nonce=Nonce}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec score(gateway()) -> float().
score(Gateway) ->
    Gateway#gateway.score.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec score(float(), gateway()) -> gateway().
score(Score, Gateway) ->
    Gateway#gateway{score=Score}.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Gw = #gateway{
        owner_address = <<"owner_address">>
        ,location = 12
        ,last_poc_challenge = undefined
        ,nonce = 0
        ,score = 0.0
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
