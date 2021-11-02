-module(blockchain_httpc).

-define(USER_AGENT, "blockchain-worker-3").
-define(DEFAULT_OPTIONS, [{body_format, binary, full_result, false}]).

-export([
         fetch/1,
         fetch/2,
         fetch_to_scratch_file/2,
         json_decode/1
        ]).

fetch(URL) ->
    fetch(URL, ?DEFAULT_OPTIONS).

fetch(URL, Options) ->
    Headers = [
               {"user-agent", ?USER_AGENT}
              ],
    HTTPOptions = [
                   {timeout, 900000}, % milliseconds, 900 sec overall request timeout
                   {connect_timeout, 60000} % milliseconds, 60 second connection timeout
                  ],

    case httpc:request(get, {URL, Headers}, HTTPOptions, Options) of
        {ok, {200, Data}} -> {ok, Data};
        {ok, saved_to_file} -> {ok, saved};
        {ok, {404, _Response}} -> throw({error, {url_not_found, URL}});
        {ok, {Status, Response}} -> throw({error, {Status, Response}});
        Other -> throw(Other)
    end.

fetch_to_scratch_file(URL, Filename) ->
    fetch(URL, ?DEFAULT_OPTIONS++[{stream, Filename}]).

json_decode(Data) ->
    jsx:decode(Data, [{return_maps, true}]).
