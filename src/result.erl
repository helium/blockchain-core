-module(result).

-export_type([
    t/2
]).

-export([
    of_bool/3,
    to_bool/1,
    pipe/2
]).

-type t(Ok, Error) ::
    {ok, Ok} | {error, Error}. 
    %% TODO Reconsider adding 'ok' case. How should pipe handle it?

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
