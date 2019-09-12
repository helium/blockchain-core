%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Ledger PoC ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_ledger_poc_v1).

-export([
    new/4,
    secret_hash/1, secret_hash/2,
    onion_key_hash/1, onion_key_hash/2,
    challenger/1, challenger/2,
    block_hash/1, block_hash/2,
    serialize/1, deserialize/1,
    find_valid/3,
    rxtx/0, rx/0, tx/0, fail/0
]).

-include("blockchain.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(poc_v1, {
    secret_hash :: binary(),
    onion_key_hash :: binary(),
    challenger :: libp2p_crypto:pubkey_bin(),
    block_hash :: binary()
}).

-define(RXTX, rxtx).
-define(RX, rx).
-define(TX, tx).
-define(FAIL, fail).

-type poc_result_type() :: rxtx | rx | tx | fail.
-type poc_result_types() :: [poc_result_type()].
-type poc() :: #poc_v1{}.
-type pocs() :: [poc()].
-export_type([poc/0, pocs/0, poc_result_type/0, poc_result_types/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(binary(), binary(), libp2p_crypto:pubkey_bin(), binary()) -> poc().
new(SecretHash, OnionKeyHash, Challenger, BlockHash) ->
    #poc_v1{
        secret_hash=SecretHash,
        onion_key_hash=OnionKeyHash,
        challenger=Challenger,
        block_hash=BlockHash
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
-spec challenger(poc()) -> libp2p_crypto:pubkey_bin().
challenger(PoC) ->
    PoC#poc_v1.challenger.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec challenger(libp2p_crypto:pubkey_bin(), poc()) -> poc().
challenger(Challenger, PoC) ->
    PoC#poc_v1{challenger=Challenger}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec block_hash(poc()) -> binary().
block_hash(PoC) ->
    PoC#poc_v1.block_hash.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec block_hash(binary(), poc()) -> poc().
block_hash(Challenger, PoC) ->
    PoC#poc_v1{block_hash=Challenger}.

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


%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec find_valid(pocs(), libp2p_crypto:pubkey_bin(), binary()) -> {ok, poc()} | {error, any()}.
find_valid([], _Challenger, _Secret) ->
    {error, not_found};
find_valid([PoC|PoCs], Challenger, Secret) ->
    case
        blockchain_ledger_poc_v1:challenger(PoC) =:= Challenger andalso
        blockchain_ledger_poc_v1:secret_hash(PoC) =:= crypto:hash(sha256, Secret)
    of
        false -> find_valid(PoCs, Challenger, Secret);
        true -> {ok, PoC}
    end.

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


%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    PoC = #poc_v1{
        secret_hash= <<"some sha256">>,
        onion_key_hash= <<"some key bin">>,
        challenger = <<"address">>,
        block_hash = <<"block_hash">>
    },
    ?assertEqual(PoC, new(<<"some sha256">>, <<"some key bin">>, <<"address">>, <<"block_hash">>)).

secret_hash_test() ->
    PoC = new(<<"some sha256">>, <<"some key bin">>, <<"address">>, <<"block_hash">>),
    ?assertEqual(<<"some sha256">>, secret_hash(PoC)),
    ?assertEqual(<<"some sha512">>, secret_hash(secret_hash(<<"some sha512">>, PoC))).

onion_key_hash_test() ->
    PoC = new(<<"some sha256">>, <<"some key bin">>, <<"address">>, <<"block_hash">>),
    ?assertEqual(<<"some key bin">>, onion_key_hash(PoC)),
    ?assertEqual(<<"some key bin 2">>, onion_key_hash(onion_key_hash(<<"some key bin 2">>, PoC))).

challenger_test() ->
    PoC = new(<<"some sha256">>, <<"some key bin">>, <<"address">>, <<"block_hash">>),
    ?assertEqual(<<"address">>, challenger(PoC)),
    ?assertEqual(<<"address 2">>, challenger(challenger(<<"address 2">>, PoC))).

block_hash_test() ->
    PoC = new(<<"some sha256">>, <<"some key bin">>, <<"address">>, <<"block_hash">>),
    ?assertEqual(<<"block_hash">>, block_hash(PoC)),
    ?assertEqual(<<"block_hash 2">>, block_hash(block_hash(<<"block_hash 2">>, PoC))).

-endif.
