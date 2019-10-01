%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Data Credits Utils ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_dcs_utils).

-export([
    get_nonce/2, get_credits/2
]).

-include("blockchain.hrl").
-include("blockchain_dcs.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-spec get_nonce(rocksdb:db_handle(), rocksdb:cf_handle()) -> {ok, non_neg_integer()} | {error, any()}.
get_nonce(DB, CF) ->
    case rocksdb:get(DB, CF, ?NONCE_KEY, [{sync, true}]) of
        {ok, <<Nonce/integer>>} ->
            {ok, Nonce};
        not_found ->
            {error, not_found};
        _Error ->
            _Error
    end.

-spec get_credits(rocksdb:db_handle(), rocksdb:cf_handle()) -> {ok, non_neg_integer()} | {error, any()}.
get_credits(DB, CF) ->
    case rocksdb:get(DB, CF, ?CREDITS_KEY, [{sync, true}]) of
        {ok, <<Credits/integer>>} ->
            {ok, Credits};
        not_found ->
            {error, not_found};
        _Error ->
            _Error
    end.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).
-endif.