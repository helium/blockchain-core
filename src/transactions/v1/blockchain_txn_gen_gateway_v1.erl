%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Genesis Gateway ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_gen_gateway_v1).

-behavior(blockchain_txn).

-export([
    new/6,
    hash/1,
    gateway_address/1,
    owner_address/1,
    location/1,
    last_poc_challenge/1,
    last_poc_hash/1,
    nonce/1,
    score/1,
    is/1,
    absorb/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(txn_genesis_gateway_v1, {
    gateway_address :: libp2p_crypto:address(),
    owner_address :: libp2p_crypto:address(),
    location :: undefined | pos_integer(),
    last_poc_challenge :: undefined | non_neg_integer(),
    last_poc_hash :: undefined | binary(),
    nonce = 0 :: non_neg_integer(),
    score = 0.0 :: float()
}).

-type txn_genesis_gateway() :: #txn_genesis_gateway_v1{}.
-export_type([txn_genesis_gateway/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(GatewayAddress :: libp2p_crypto:address(),
          OwnerAddress :: libp2p_crypto:address(),
          Location :: undefined | pos_integer(),
          LastPocChallenge :: undefined | non_neg_integer(),
          Nonce :: non_neg_integer(),
          Score :: float()) -> txn_genesis_gateway().
new(GatewayAddress, OwnerAddress, Location, LastPocChallenge, Nonce, Score) ->
    #txn_genesis_gateway_v1{gateway_address=GatewayAddress,
                            owner_address=OwnerAddress,
                            location=Location,
                            last_poc_challenge=LastPocChallenge,
                            nonce=Nonce,
                            score=Score}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec hash(txn_genesis_gateway()) -> blockchain_txn:hash().
hash(Txn) ->
    crypto:hash(sha256, erlang:term_to_binary(Txn)).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec gateway_address(txn_genesis_gateway()) -> libp2p_crypto:address().
gateway_address(Txn) ->
    Txn#txn_genesis_gateway_v1.gateway_address.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec owner_address(txn_genesis_gateway()) -> libp2p_crypto:address().
owner_address(Txn) ->
    Txn#txn_genesis_gateway_v1.owner_address.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec location(txn_genesis_gateway()) -> undefined | pos_integer().
location(Txn) ->
    Txn#txn_genesis_gateway_v1.location.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec last_poc_challenge(txn_genesis_gateway()) -> undefined | non_neg_integer().
last_poc_challenge(Txn) ->
    Txn#txn_genesis_gateway_v1.last_poc_challenge.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec last_poc_hash(txn_genesis_gateway()) -> undefined | binary().
last_poc_hash(Txn) ->
    Txn#txn_genesis_gateway_v1.last_poc_hash.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec nonce(txn_genesis_gateway()) -> non_neg_integer().
nonce(Txn) ->
    Txn#txn_genesis_gateway_v1.nonce.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec score(txn_genesis_gateway()) -> float().
score(Txn) ->
    Txn#txn_genesis_gateway_v1.score.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is(blockchain_transactions:transaction()) -> boolean().
is(Txn) ->
    erlang:is_record(Txn, txn_genesis_gateway_v1).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb(txn_genesis_gateway(),  blockchain_ledger_v1:ledger()) -> ok | {error, not_in_genesis_block}.
absorb(Txn, Ledger) ->
    %% NOTE: This transaction should only be absorbed when it's in the genesis block
    case blockchain_ledger_v1:current_height(Ledger) of
        %% Ledger height is 0 till the genesis block is absorbed
        {ok, 0} ->
            GatewayAddress = ?MODULE:gateway_address(Txn),
            OwnerAddress = ?MODULE:owner_address(Txn),
            Location = ?MODULE:location(Txn),
            LastPocChallenge = ?MODULE:last_poc_challenge(Txn),
            LastPocHash = ?MODULE:last_poc_hash(Txn),
            Nonce = ?MODULE:nonce(Txn),
            Score = ?MODULE:score(Txn),
            blockchain_ledger_v1:add_gateway(OwnerAddress,
                                             GatewayAddress,
                                             Location,
                                             LastPocChallenge,
                                             LastPocHash,
                                             Nonce,
                                             Score,
                                             Ledger);
        _ ->
            {error, not_in_genesis_block}
    end.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Tx = #txn_genesis_gateway_v1{gateway_address = <<"0">>,
                                 owner_address = <<"1">>,
                                 location=1000,
                                 last_poc_challenge=30,
                                 nonce=10,
                                 score=0.8},
    ?assertEqual(Tx, new(<<"0">>, <<"1">>, 1000, 30, 10, 0.8)).

is_test() ->
    Tx0 = new(<<"0">>, <<"1">>, 1000, 30, 10, 0.8),
    ?assert(is(Tx0)).

gateway_address_test() ->
    Tx = new(<<"0">>, <<"1">>, 1000, 30, 10, 0.8),
    ?assertEqual(<<"0">>, gateway_address(Tx)).

owner_address_test() ->
    Tx = new(<<"0">>, <<"1">>, 1000, 30, 10, 0.8),
    ?assertEqual(<<"1">>, owner_address(Tx)).

location_test() ->
    Tx = new(<<"0">>, <<"1">>, 1000, 30, 10, 0.8),
    ?assertEqual(1000, location(Tx)).

last_poc_challenge_test() ->
    Tx = new(<<"0">>, <<"1">>, 1000, 30, 10, 0.8),
    ?assertEqual(30, last_poc_challenge(Tx)).

nonce_test() ->
    Tx = new(<<"0">>, <<"1">>, 1000, 30, 10, 0.8),
    ?assertEqual(10, nonce(Tx)).

score_test() ->
    Tx = new(<<"0">>, <<"1">>, 1000, 30, 10, 0.8),
    ?assertEqual(0.8, score(Tx)).

-endif.
