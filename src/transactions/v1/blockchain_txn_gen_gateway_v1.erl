%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Genesis Gateway ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_gen_gateway_v1).

-behavior(blockchain_txn).

-include("pb/blockchain_txn_gen_gateway_v1_pb.hrl").

-export([
    new/5,
    hash/1,
    sign/2,
    gateway/1,
    owner/1,
    location/1,
    nonce/1,
    score/1,
    fee/1,
    is_valid/2,
    absorb/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type txn_genesis_gateway() :: #blockchain_txn_gen_gateway_v1_pb{}.
-export_type([txn_genesis_gateway/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(Gateway :: libp2p_crypto:pubkey_bin(),
          Owner :: libp2p_crypto:pubkey_bin(),
          Location :: undefined | h3:h3index(),
          Nonce :: non_neg_integer(),
          Score :: float()) -> txn_genesis_gateway().
new(Gateway, Owner, Location, Nonce, Score) ->
    L = case Location of
            undefined -> undefined;
            _ -> h3:to_string(Location)
        end,
    #blockchain_txn_gen_gateway_v1_pb{gateway=Gateway,
                                      owner=Owner,
                                      location=L,
                                      nonce=Nonce,
                                      score=Score}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec hash(txn_genesis_gateway()) -> blockchain_txn:hash().
hash(Txn) ->
    EncodedTxn = blockchain_txn_gen_gateway_v1_pb:encode_msg(Txn),
    crypto:hash(sha256, EncodedTxn).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec sign(txn_genesis_gateway(), libp2p_crypto:sig_fun()) -> txn_genesis_gateway().
sign(Txn, _SigFun) ->
    Txn.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec gateway(txn_genesis_gateway()) -> libp2p_crypto:pubkey_bin().
gateway(Txn) ->
    Txn#blockchain_txn_gen_gateway_v1_pb.gateway.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec owner(txn_genesis_gateway()) -> libp2p_crypto:pubkey_bin().
owner(Txn) ->
    Txn#blockchain_txn_gen_gateway_v1_pb.owner.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec location(txn_genesis_gateway()) -> h3:h3index().
location(#blockchain_txn_gen_gateway_v1_pb{location=[]}) ->
    undefined;
location(Txn) ->
    h3:from_string(Txn#blockchain_txn_gen_gateway_v1_pb.location).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec nonce(txn_genesis_gateway()) -> non_neg_integer().
nonce(Txn) ->
    Txn#blockchain_txn_gen_gateway_v1_pb.nonce.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec score(txn_genesis_gateway()) -> float().
score(Txn) ->
    Txn#blockchain_txn_gen_gateway_v1_pb.score.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec fee(txn_genesis_gateway()) -> non_neg_integer().
fee(_Txn) ->
    0.

%%--------------------------------------------------------------------
%% @doc
%% This transaction should only be absorbed when it's in the genesis block
%% @end
%%--------------------------------------------------------------------
-spec is_valid(txn_genesis_gateway(), blockchain:blockchain()) -> ok | {error, any()}.
is_valid(_Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    case blockchain_ledger_v1:current_height(Ledger) of
        {ok, 0} ->
            ok;
        _ ->
            {error, not_in_genesis_block}
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb(txn_genesis_gateway(), blockchain:blockchain()) -> ok | {error, not_in_genesis_block}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Gateway = ?MODULE:gateway(Txn),
    Owner = ?MODULE:owner(Txn),
    Location = ?MODULE:location(Txn),
    Nonce = ?MODULE:nonce(Txn),
    Score = ?MODULE:score(Txn),
    blockchain_ledger_v1:add_gateway(Owner,
                                     Gateway,
                                     Location,
                                     Nonce,
                                     Score,
                                     Ledger).

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

-define(TEST_LOCATION, 631210968840687103).

new_test() ->
    Tx = #blockchain_txn_gen_gateway_v1_pb{gateway = <<"0">>,
                                           owner = <<"1">>,
                                           location = h3:to_string(?TEST_LOCATION),
                                           nonce=10,
                                           score=0.8},
    ?assertEqual(Tx, new(<<"0">>, <<"1">>, ?TEST_LOCATION, 10, 0.8)).

gateway_test() ->
    Tx = new(<<"0">>, <<"1">>, ?TEST_LOCATION, 10, 0.8),
    ?assertEqual(<<"0">>, gateway(Tx)).

owner_test() ->
    Tx = new(<<"0">>, <<"1">>, ?TEST_LOCATION, 10, 0.8),
    ?assertEqual(<<"1">>, owner(Tx)).

location_test() ->
    Tx = new(<<"0">>, <<"1">>, ?TEST_LOCATION, 10, 0.8),
    ?assertEqual(?TEST_LOCATION, location(Tx)).

nonce_test() ->
    Tx = new(<<"0">>, <<"1">>, ?TEST_LOCATION, 10, 0.8),
    ?assertEqual(10, nonce(Tx)).

score_test() ->
    Tx = new(<<"0">>, <<"1">>, ?TEST_LOCATION, 10, 0.8),
    ?assertEqual(0.8, score(Tx)).

-endif.
