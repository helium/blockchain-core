-module(blockchain_json).

-include("blockchain_json.hrl").

-callback to_json(blockchain_block:block()
                 | blockchain_txn:txn(),
                  Opts::[tuple()]
                 ) -> map().


-type json_object() :: map().
-type json_opts() :: [tuple()].

-export_type([json_object/0,
              json_opts/0
             ]).

-export([
         maybe_undefined/1,
         maybe_fn/2,
         maybe_b64/1,
         maybe_b58/1,
         maybe_h3/1,
         maybe_list_to_binary/1
        ]).

%%
%% Utilities
%%

-spec maybe_undefined(any() | undefined | null) -> any() | undefined.
maybe_undefined(undefined) ->
    undefined;
maybe_undefined(null) ->
    undefined;
maybe_undefined(V) ->
    V.

-spec maybe_fn(fun((any()) -> any()), undefined | null | any()) -> undefined | any().
maybe_fn(_Fun, undefined) ->
    undefined;
maybe_fn(_Fun, null) ->
    undefined;
maybe_fn(Fun, V) ->
    Fun(V).

-spec maybe_b64(undefined | binary()) -> undefined | string().
maybe_b64(<<>>) ->
    undefined;
maybe_b64(V) ->
    maybe_fn(fun(Bin) -> ?BIN_TO_B64(Bin) end, V).

-spec maybe_b58(undefined | binary()) -> undefined | binary().
maybe_b58(<<>>) ->
    undefined;
maybe_b58(V) ->
    maybe_fn(fun(Bin) -> ?BIN_TO_B58(Bin) end, V).

-spec maybe_h3(undefined | h3:h3index()) -> undefined | binary().
maybe_h3(V) ->
    maybe_fn(fun(I) -> list_to_binary(h3:to_string(I)) end, V).

-spec maybe_list_to_binary(undefined | list()) -> undefined | binary().
maybe_list_to_binary(V) ->
    maybe_fn(fun ([]) -> undefined;
                 (I) when is_list(I) -> list_to_binary(I);
                 (<<>>) -> undefined;
                 (I) when is_binary(I) -> I
             end, V).
