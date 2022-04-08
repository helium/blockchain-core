-module(data_result).

-export_type([
    t/2,
    void/1
]).

-export([
    is_ok/1
]).

-type t(Ok, Error) :: {ok, Ok} | {error, Error}.
-type void(Error) :: ok | {error, Error}.

-spec is_ok(t(_, _) | void(_)) -> boolean().
is_ok(ok) -> true;
is_ok({ok, _}) -> true;
is_ok({error, _}) -> false.
