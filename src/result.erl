-module(result).

-export_type([
    t/2
]).

-export([
    of_bool/3,
    pipe/2
]).

-type t(Ok, Error) ::
    {ok, Ok} | {error, Error}. 

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
