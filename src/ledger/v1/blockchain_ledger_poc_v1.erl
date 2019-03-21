%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Ledger PoC ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_ledger_poc_v1).

-export([
    new/3,
    secret_hash/1, secret_hash/2,
    onion_key_hash/1, onion_key_hash/2,
    gateway/1, gateway/2,
    serialize/1, deserialize/1
]).

-include("blockchain.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(poc_v1, {
    secret_hash :: binary(),
    onion_key_hash :: binary(),
    gateway :: libp2p_crypto:pubkey_bin()
}).

-type poc() :: #poc_v1{}.
-export_type([poc/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(binary(), binary(), libp2p_crypto:pubkey_bin()) -> poc().
new(SecretHash, OnionKeyHash, Gateway) ->
    #poc_v1{
        secret_hash=SecretHash,
        onion_key_hash=OnionKeyHash,
        gateway=Gateway
    }.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec secret_hash(poc()) -> binary().
secret_hash(PoC) ->
    PoC#poc_v1.secret_hash.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec secret_hash(binary(), poc()) -> poc().
secret_hash(SecretHash, PoC) ->
    PoC#poc_v1{secret_hash=SecretHash}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec onion_key_hash(poc()) -> binary().
onion_key_hash(PoC) ->
    PoC#poc_v1.onion_key_hash.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec onion_key_hash(binary(), poc()) -> poc().
onion_key_hash(OnionKeyHash, PoC) ->
    PoC#poc_v1{onion_key_hash=OnionKeyHash}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec gateway(poc()) -> libp2p_crypto:pubkey_bin().
gateway(PoC) ->
    PoC#poc_v1.gateway.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec gateway(libp2p_crypto:pubkey_bin(), poc()) -> poc().
gateway(OnionKeyHash, PoC) ->
    PoC#poc_v1{gateway=OnionKeyHash}.

%%--------------------------------------------------------------------
%% @doc
%% Version 1
%% @end
%%--------------------------------------------------------------------
-spec serialize(poc()) -> binary().
serialize(PoC) ->
    BinPoC = erlang:term_to_binary(PoC),
    <<1, BinPoC/binary>>.

%%--------------------------------------------------------------------
%% @doc
%% Later _ could becomre 1, 2, 3 for different versions.
%% @end
%%--------------------------------------------------------------------
-spec deserialize(binary()) -> poc().
deserialize(<<_:1/binary, Bin/binary>>) ->
    erlang:binary_to_term(Bin).

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    PoC = #poc_v1{
        secret_hash= <<"some sha256">>,
        onion_key_hash= <<"some key bin">>,
        gateway = <<"address">>
    },
    ?assertEqual(PoC, new(<<"some sha256">>, <<"some key bin">>, <<"address">>)).

secret_hash_test() ->
    PoC = new(<<"some sha256">>, <<"some key bin">>, <<"address">>),
    ?assertEqual(<<"some sha256">>, secret_hash(PoC)),
    ?assertEqual(<<"some sha512">>, secret_hash(secret_hash(<<"some sha512">>, PoC))).

onion_key_hash_test() ->
    PoC = new(<<"some sha256">>, <<"some key bin">>, <<"address">>),
    ?assertEqual(<<"some key bin">>, onion_key_hash(PoC)),
    ?assertEqual(<<"some key bin 2">>, onion_key_hash(onion_key_hash(<<"some key bin 2">>, PoC))).

gateway_test() ->
    PoC = new(<<"some sha256">>, <<"some key bin">>, <<"address">>),
    ?assertEqual(<<"address">>, gateway(PoC)),
    ?assertEqual(<<"address 2">>, gateway(gateway(<<"address 2">>, PoC))).

-endif.
