-module(result).

-export_type([
    empty/1,
    t/2
]).

-export([
    of_empty/2,
    to_empty/1,
    of_bool/3,
    to_bool/1,
    pipe/2
]).

-type empty(ErrorReason) ::
    ok | {error, ErrorReason}.

-type t(Ok, Error) ::
    {ok, Ok} | {error, Error}. 

-spec of_empty(empty(ErrorReason), OkVal) -> t(OkVal, ErrorReason).
of_empty(ok, OkVal) -> {ok, OkVal};
of_empty({error, _}=Error, _) -> Error.

-spec to_empty(t(_, ErrorReason)) -> empty(ErrorReason).
to_empty({ok, _}) -> ok;
to_empty({error, _}=E) -> E.

-spec of_bool(boolean(), Ok, Error) -> t(Ok, Error).
of_bool(true, Ok, _) -> {ok, Ok};
of_bool(false, _, Error) -> {error, Error}.

-spec pipe([fun((Ok1) -> t(Ok2, Error))], Ok1) -> t(Ok2, Error).
pipe([], X) ->
    {ok, X};
pipe([F | Fs], X) ->
    case F(X) of
        {error, _}=Error ->
            Error;
        {ok, Y} ->
            pipe(Fs, Y)
    end.

-spec to_bool(ok | t(_, _)) -> boolean().
to_bool(ok) -> true;
to_bool({ok, _}) -> true;
to_bool({error, _}) -> false.
