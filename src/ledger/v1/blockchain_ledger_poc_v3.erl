%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Ledger PoC V2 ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_ledger_poc_v3).

-export([
    new/4,
    onion_key_hash/1, onion_key_hash/2,
    challenger/1, challenger/2,
    block_hash/1, block_hash/2,
    start_height/1, start_height/2,
    secret_hash/1,
    orig_version/1,
    serialize/1, deserialize/1,
    rxtx/0, rx/0, tx/0, fail/0
]).

-include("blockchain.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
-record(poc_v3, {
    onion_key_hash :: binary(),
    challenger :: libp2p_crypto:pubkey_bin(),
    block_hash :: binary(),
    start_height :: pos_integer(),
    orig_version :: pos_integer(),
    secret_hash :: binary() | undefined %% TODO, carried over from v2 for backwards compatibility...maybe remove
}).

-define(RXTX, rxtx).
-define(RX, rx).
-define(TX, tx).
-define(FAIL, fail).

-type poc_result_type() :: rxtx | rx | tx | fail.
-type poc_result_types() :: [poc_result_type()].
-type poc() :: #poc_v3{}.
-type pocs() :: [poc()].
-export_type([poc/0, pocs/0, poc_result_type/0, poc_result_types/0]).

-spec new(binary(), libp2p_crypto:pubkey_bin(), binary(), pos_integer()) -> poc().
new(OnionKeyHash, Challenger, BlockHash, StartHeight) ->
    #poc_v3{
        onion_key_hash=OnionKeyHash,
        challenger=Challenger,
        block_hash=BlockHash,
        start_height=StartHeight,
        orig_version = 3
    }.

-spec onion_key_hash(poc()) -> binary().
onion_key_hash(PoC) ->
    PoC#poc_v3.onion_key_hash.

-spec onion_key_hash(binary(), poc()) -> poc().
onion_key_hash(OnionKeyHash, PoC) ->
    PoC#poc_v3{onion_key_hash=OnionKeyHash}.

-spec challenger(poc()) -> libp2p_crypto:pubkey_bin().
challenger(PoC) ->
    PoC#poc_v3.challenger.

-spec challenger(libp2p_crypto:pubkey_bin(), poc()) -> poc().
challenger(Challenger, PoC) ->
    PoC#poc_v3{challenger=Challenger}.

-spec block_hash(poc()) -> binary().
block_hash(PoC) ->
    PoC#poc_v3.block_hash.

-spec block_hash(binary(), poc()) -> poc().
block_hash(Challenger, PoC) ->
    PoC#poc_v3{block_hash=Challenger}.

-spec start_height(poc()) -> binary().
start_height(PoC) ->
    PoC#poc_v3.start_height.

-spec start_height(pos_integer(), poc()) -> poc().
start_height(Height, PoC) ->
    PoC#poc_v3{start_height=Height}.

-spec orig_version(poc()) -> binary().
orig_version(PoC) ->
    PoC#poc_v3.orig_version.

-spec secret_hash(poc()) -> binary().
secret_hash(PoC) ->
    PoC#poc_v3.secret_hash.

-spec serialize(poc()) -> binary().
serialize(PoC) ->
    %% intentionally don't compress here, we compress these in batches
    %% in the ledger code, which should get better compression anyway
    BinPoC = erlang:term_to_binary(PoC),
    <<3, BinPoC/binary>>.

-spec deserialize(binary()) -> poc().
deserialize(<<1, Bin/binary>>) ->
    V1 = erlang:binary_to_term(Bin),
    convert(V1);
deserialize(<<2, Bin/binary>>) ->
    V2 = erlang:binary_to_term(Bin),
    convert(V2);
deserialize(<<3, Bin/binary>>) ->
    erlang:binary_to_term(Bin).


%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec rxtx() -> poc_result_type().
rxtx() ->
    ?RXTX.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec rx() -> poc_result_type().
rx() ->
    ?RX.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec tx() -> poc_result_type().
tx() ->
    ?TX.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec fail() -> poc_result_type().
fail() ->
    ?FAIL.

-record(poc_v1, {
    secret_hash :: binary(),
    onion_key_hash :: binary(),
    challenger :: libp2p_crypto:pubkey_bin()
}).
-record(poc_v2, {
    secret_hash :: binary(),
    onion_key_hash :: binary(),
    challenger :: libp2p_crypto:pubkey_bin(),
    block_hash :: binary()
}).

convert(#poc_v1{secret_hash=SecretHash,
                onion_key_hash=OnionKeyHash,
                challenger=Challenger
               }) ->
    #poc_v3{
        secret_hash=SecretHash,
        onion_key_hash=OnionKeyHash,
        challenger=Challenger,
        block_hash= <<>>,
        start_height=1,
        orig_version=1
    };
convert(#poc_v2{secret_hash=SecretHash,
                onion_key_hash=OnionKeyHash,
                challenger=Challenger,
                block_hash=BlockHash
               }) ->
    #poc_v3{
        secret_hash=SecretHash,
        onion_key_hash=OnionKeyHash,
        challenger=Challenger,
        block_hash= BlockHash,
        start_height=1,
        orig_version=2
    }.
%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    PoC = #poc_v3{
        onion_key_hash= <<"some key bin">>,
        challenger = <<"address">>,
        block_hash = <<"block_hash">>,
        start_height = 120000,
        orig_version = 3,
        secret_hash = undefined
    },
    ?assertEqual(PoC, new(<<"some key bin">>, <<"address">>, <<"block_hash">>, 120000)).

secret_hash_test() ->
    PoC = new(<<"some key bin">>, <<"address">>, <<"block_hash">>, 120000),
    ?assertEqual(undefined, secret_hash(PoC)).

onion_key_hash_test() ->
    PoC = new(<<"some key bin">>, <<"address">>, <<"block_hash">>, 120000),
    ?assertEqual(<<"some key bin">>, onion_key_hash(PoC)),
    ?assertEqual(<<"some key bin 2">>, onion_key_hash(onion_key_hash(<<"some key bin 2">>, PoC))).

challenger_test() ->
    PoC = new(<<"some key bin">>, <<"address">>, <<"block_hash">>, 120000),
    ?assertEqual(<<"address">>, challenger(PoC)),
    ?assertEqual(<<"address 2">>, challenger(challenger(<<"address 2">>, PoC))).

block_hash_test() ->
    PoC = new(<<"some key bin">>, <<"address">>, <<"block_hash">>, 120000),
    ?assertEqual(<<"block_hash">>, block_hash(PoC)),
    ?assertEqual(<<"block_hash 2">>, block_hash(block_hash(<<"block_hash 2">>, PoC))).

start_height_test() ->
    PoC = new(<<"some key bin">>, <<"address">>, <<"block_hash">>, 120000),
    ?assertEqual(120000, start_height(PoC)),
    ?assertEqual(200000, start_height(start_height(200000, PoC))).

orig_version_test() ->
    PoC = new(<<"some key bin">>, <<"address">>, <<"block_hash">>, 120000),
    ?assertEqual(3, orig_version(PoC)).

-endif.
