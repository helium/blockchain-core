%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Token V1 ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_token_v1).

-type type() :: hnt | hst | mobile | iot.
-type types() :: [type()].

-export_type([type/0, types/0]).
-export([supported_tokens/0]).

-spec supported_tokens() -> types().
supported_tokens() ->
    [hnt, hst, mobile, iot].
