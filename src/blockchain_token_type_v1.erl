%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Token Type V1 ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_token_type_v1).

-type token_type() :: hnt | hst | hlt | hgt.
-type token_types() :: [token_type()].

-export_type([token_type/0, token_types/0]).
-export([supported_tokens/0]).

-spec supported_tokens() -> token_types().
supported_tokens() ->
    [hnt, hst, hlt, hgt].
