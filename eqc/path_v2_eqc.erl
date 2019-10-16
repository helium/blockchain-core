-module(path_v2_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([prop_path_check/0]).

prop_path_check() ->
    ?FORALL({TargetPubkeyBin, Entropy, PathLimit},
            {gen_target(), gen_entropy(), gen_path_limit()},
            begin
                ActiveGateways = active_gateways(),
                Path = blockchain_poc_path_v2:build(TargetPubkeyBin,
                                                    ActiveGateways,
                                                    block_time(),
                                                    Entropy,
                                                    PathLimit),
                PathLength = length(Path),

                B58Path = #{libp2p_crypto:bin_to_b58(TargetPubkeyBin) => [[libp2p_crypto:bin_to_b58(P) || P <- Path]]},

                FileName = "/tmp/paths",
                case file:read_file_info(FileName) of
                    {ok, _FileInfo} ->
                        case length(Path) > 1 of
                            true ->
                                file:write_file(FileName, io_lib:fwrite("~p.\n", [B58Path]), [append]);
                            false ->
                                ok
                        end;
                    {error, enoent} ->
                        % File doesn't exist
                        ok
                end,


                %% Checks:
                %% - honor path limit
                %% - atleast one element in path
                %% - target is always in path
                %% - we never go back to the same h3 index in path
                Check = (PathLength =< PathLimit andalso
                         PathLength >= 1 andalso
                         length(Path) == length(lists:usort(Path)) andalso
                         lists:member(TargetPubkeyBin, Path)),

                ?WHENFAIL(begin
                              io:format("Target: ~p~n", [TargetPubkeyBin]),
                              io:format("PathLimit: ~p~n", [PathLimit]),
                              io:format("Path: ~p~n", [Path])
                          end,
                          conjunction([{verify_path, Check}]))

            end).

gen_entropy() ->
    binary(32).

gen_target() ->
    elements(maps:keys(active_gateways())).

gen_path_limit() ->
    elements([3, 4, 5, 6, 7]).

active_gateways() ->
    {ok, Dir} = file:get_cwd(),
    {ok, [AG]} = file:consult(filename:join([Dir,  "eqc", "active86837.txt"])),
    AG.

block_time() ->
    1571180431 * 1000000000.

%% name(PubkeyBin) ->
%%     {ok, Name} = erl_angry_purple_tiger:animal_name(libp2p_crypto:bin_to_b58(PubkeyBin)),
%%     Name.
