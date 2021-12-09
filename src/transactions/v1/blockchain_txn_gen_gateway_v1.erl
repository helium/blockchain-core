%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Genesis Gateway ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_gen_gateway_v1).

-behavior(blockchain_txn).
-behavior(blockchain_json).

-include("blockchain_json.hrl").
-include("blockchain_utils.hrl").
-include("blockchain_records_meta.hrl").

-include_lib("helium_proto/include/blockchain_txn_gen_gateway_v1_pb.hrl").

-export([
    new/4,
    hash/1,
    sign/2,
    gateway/1,
    owner/1,
    location/1,
    nonce/1,
    fee/1,
    fee_payer/2,
    is_valid/2,
    is_well_formed/1,
    is_prompt/2,
    absorb/2,
    print/1,
    json_type/0,
    to_json/2
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
          Nonce :: non_neg_integer()) -> txn_genesis_gateway().
new(Gateway, Owner, Location, Nonce) ->
    L = case Location of
            undefined -> undefined;
            _ -> h3:to_string(Location)
        end,
    #blockchain_txn_gen_gateway_v1_pb{gateway=Gateway,
                                      owner=Owner,
                                      location=L,
                                      nonce=Nonce}.


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
-spec fee(txn_genesis_gateway()) -> non_neg_integer().
fee(_Txn) ->
    0.

-spec fee_payer(txn_genesis_gateway(), blockchain_ledger_v1:ledger()) -> libp2p_crypto:pubkey_bin() | undefined.
fee_payer(_Txn, _Ledger) ->
    undefined.

%%--------------------------------------------------------------------
%% @doc
%% This transaction should only be absorbed when it's in the genesis block
%% @end
%%--------------------------------------------------------------------
-spec is_valid(txn_genesis_gateway(), blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
is_valid(_T, _Chain) ->
    ok.

-spec is_well_formed(txn_genesis_gateway()) -> ok | {error, _}.
is_well_formed(#blockchain_txn_gen_gateway_v1_pb{}=T) ->
    blockchain_contract:check(
        record_to_kvl(blockchain_txn_gen_gateway_v1_pb, T),
        {kvl, [
            {gateway , {address, libp2p}},
            {owner   , {address, libp2p}},
            {location, {either, [undefined, {string, {exactly, 0}}, h3_string]}},
            {nonce   , {integer, {min, 1}}}
        ]}
    ).

-spec is_prompt(txn_genesis_gateway(), blockchain:blockchain()) ->
    {ok, blockchain_txn:is_prompt()} | {error, _}.
is_prompt(_T, Chain) ->
    Ledger = blockchain:ledger(Chain),
    case blockchain_ledger_v1:current_height(Ledger) of
        {ok, 0} ->
            {ok, yes};
        {ok, _} ->
            %% Not in genesis block.
            {ok, no};
        {error, _}=Err ->
            Err
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb(txn_genesis_gateway(), blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Gateway = ?MODULE:gateway(Txn),
    Owner = ?MODULE:owner(Txn),
    Location = ?MODULE:location(Txn),
    Nonce = ?MODULE:nonce(Txn),
    blockchain_ledger_v1:add_gateway(Owner,
                                     Gateway,
                                     Location,
                                     Nonce,
                                     full,
                                     Ledger).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec print(txn_genesis_gateway()) -> iodata().
print(undefined) -> <<"type=genesis_gateway, undefined">>;
print(#blockchain_txn_gen_gateway_v1_pb{
         gateway=Gateway, owner=Owner,
         location=L, nonce=Nonce}) ->
    io_lib:format("type=genesis_gateway gateway=~p, owner=~p, location=~p, nonce=~p",
                  [?TO_ANIMAL_NAME(Gateway), ?TO_B58(Owner), L, Nonce]).

json_type() ->
    <<"gen_gateway_v1">>.

-spec to_json(txn_genesis_gateway(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Txn, _Opts) ->
    #{
      type => ?MODULE:json_type(),
      hash => ?BIN_TO_B64(hash(Txn)),
      gateway => ?BIN_TO_B58(gateway(Txn)),
      owner => ?BIN_TO_B58(owner(Txn)),
      location => ?MAYBE_H3(location(Txn)),
      nonce => nonce(Txn)
     }.

-spec record_to_kvl(atom(), tuple()) -> [{atom(), term()}].
?DEFINE_RECORD_TO_KVL(blockchain_txn_gen_gateway_v1_pb).

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

-define(TSET(T, K, V), T#blockchain_txn_gen_gateway_v1_pb{K = V}).
-define(TEST_LOCATION, 631210968840687103).

new_test() ->
    Tx = #blockchain_txn_gen_gateway_v1_pb{gateway = <<"0">>,
                                           owner = <<"1">>,
                                           location = h3:to_string(?TEST_LOCATION),
                                           nonce=10},
    ?assertEqual(Tx, new(<<"0">>, <<"1">>, ?TEST_LOCATION, 10)).

gateway_test() ->
    Tx = new(<<"0">>, <<"1">>, ?TEST_LOCATION, 10),
    ?assertEqual(<<"0">>, gateway(Tx)).

owner_test() ->
    Tx = new(<<"0">>, <<"1">>, ?TEST_LOCATION, 10),
    ?assertEqual(<<"1">>, owner(Tx)).

location_test() ->
    Tx = new(<<"0">>, <<"1">>, ?TEST_LOCATION, 10),
    ?assertEqual(?TEST_LOCATION, location(Tx)).

nonce_test() ->
    Tx = new(<<"0">>, <<"1">>, ?TEST_LOCATION, 10),
    ?assertEqual(10, nonce(Tx)).

json_test() ->
    Tx = new(<<"0">>, <<"1">>, ?TEST_LOCATION, 10),
    Json = to_json(Tx, []),
    ?assert(lists:all(fun(K) -> maps:is_key(K, Json) end,
                      [type, hash, gateway, owner, location, nonce])).

is_well_formed_test_() ->
    Addr =
        begin
            #{public := PK, secret := _} = libp2p_crypto:generate_keys(ecc_compact),
            libp2p_crypto:pubkey_to_bin(PK)
        end,
    T =
        #blockchain_txn_gen_gateway_v1_pb{
            gateway  = Addr,
            owner    = Addr,
            location = h3:to_string(?TEST_LOCATION),
            nonce    = 1
        },
    [
        ?_assertEqual(ok, is_well_formed(T)),
        ?_assertEqual(ok, is_well_formed(?TSET(T, location, undefined))),
        ?_assertEqual(ok, is_well_formed(?TSET(T, location, ""))),
        ?_assertMatch({error, _}, is_well_formed(?TSET(T, location, "foo"))),
        ?_assertMatch({error, _}, is_well_formed(?TSET(T, nonce, -1))),
        ?_assertMatch({error, _}, is_well_formed(?TSET(T, gateway, <<>>))),
        ?_assertMatch({error, _}, is_well_formed(?TSET(T, owner, <<>>)))
    ].

-endif.
