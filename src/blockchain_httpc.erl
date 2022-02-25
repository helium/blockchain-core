%% @doc This module is intended to handle http client calls both for snapshot stuff _and_
%% also to the JSONRPC endpoints so that eventually we can retire our clique integrations.
%%
%% This is just for httpc calls - we will have a gen server elsewhere to keep metadata
%% about snapshot downloads and available snap data - but not here.

-module(blockchain_httpc).

-include_lib("kernel/include/fileinfo.hrl").

-export([
         get/2,
         get_with_resume/3]).

get(Url, Options) ->
    do_get(Url, Options).

get_with_resume(Url, File, Options) ->
    Have = case file:read_file_info(File) of
               {error, _Error} -> 0; %% covers enoent too
               {ok, FileInfo} -> FileInfo#file_info.size
           end,
    do_get(Url, Options#{ have => Have }).

do_get(Url, Options) ->
    Headers0 = [{"user-agent", "blockchain-httpc-1"}],
    Headers1 = case maps:get(have, Options, undefined) of
                   undefined -> Headers0;
                   0 -> Headers0;
                   Sz -> [{"range", "bytes=" ++ integer_to_list(Sz) ++ "-"} | Headers0 ]
               end,
    HackneyOptions = [],



    
