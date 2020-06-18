-module(blockchain_snapshot_SUITE).

-include_lib("common_test/include/ct.hrl").

-include_lib("eunit/include/eunit.hrl").

-include("blockchain_vars.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).

-define(TEST_LOCATION, 631210968840687103).

-export([
    basic_test/1
]).

-import(blockchain_utils, [normalize_float/1]).

%%--------------------------------------------------------------------
%% COMMON TEST CALLBACK FUNCTIONS
%%--------------------------------------------------------------------
%%--------------------------------------------------------------------
%% @public
%% @doc
%%   Running tests for this suite
%% @end
%%--------------------------------------------------------------------
all() ->
    [
        basic_test
    ].

%%--------------------------------------------------------------------
%% TEST CASE SETUP
%%--------------------------------------------------------------------
init_per_testcase(_TestCase, Config) ->
    Config.

%%--------------------------------------------------------------------
%% TEST CASE TEARDOWN
%%--------------------------------------------------------------------
end_per_testcase(_, _Config) ->
    ok.

%%--------------------------------------------------------------------
%% TEST CASES
%%--------------------------------------------------------------------
basic_test(_Config) ->
    Ledger = ledger(),
    {ok, Snapshot} = blockchain_ledger_snapshot_v1:snapshot(Ledger, []),

    %% make a dir for the loaded snapshot
    {ok, Dir} = file:get_cwd(),
    PrivDir = filename:join([Dir, "priv"]),
    NewDir = PrivDir ++ "/ledger2/",
    ok = filelib:ensure_dir(NewDir),

    Size = byte_size(element(2, blockchain_ledger_snapshot_v1:serialize(Snapshot))),
    SHA = blockchain_ledger_snapshot_v1:hash(Snapshot),
    ct:pal("size ~p", [Size]),

    ct:pal("dir: ~p", [os:cmd("pwd")]),

    {ok, BinGen} = file:read_file("../../../../test/genesis"),

    GenesisBlock = blockchain_block:deserialize(BinGen),

    {ok, Chain} = blockchain:new(NewDir, GenesisBlock, blessed_snapshot, undefined),

    {ok, Ledger1} = blockchain_ledger_snapshot_v1:import(Chain, SHA, Snapshot),
    {ok, Snapshot1} = blockchain_ledger_snapshot_v1:snapshot(Ledger1, []),

    ?assertEqual([], blockchain_ledger_snapshot_v1:diff(Snapshot, Snapshot1)),
    ok.

%% utils
ledger() ->
    %% Ledger at height: 194196
    %% ActiveGateway Count: 3023
    % this is deep in the test hierarchy
    {ok, TestDir} = file:get_cwd(),

    Comps = filename:split(TestDir),
    Trimmed = lists:reverse(lists:sublist(lists:reverse(Comps), 5, length(Comps))),
    Dir = filename:join(Trimmed),
    %% Ensure priv dir exists
    PrivDir = filename:join([Dir, "priv"]),
    ok = filelib:ensure_dir(PrivDir ++ "/"),
    %% Path to static ledger tar
    LedgerTar = filename:join([PrivDir, "ledger.tar.gz"]),
    %% Extract ledger tar if required
    ok = extract_ledger_tar(PrivDir, LedgerTar),
    %% Get the ledger
    Ledger = blockchain_ledger_v1:new(PrivDir),
    Ledger1 = blockchain_ledger_v1:new_context(Ledger),
    %% If the hexes aren't on the ledger add them
    blockchain:bootstrap_hexes(Ledger1),
    blockchain_ledger_v1:commit_context(Ledger1),
    Ledger.

extract_ledger_tar(PrivDir, LedgerTar) ->
    case filelib:is_file(LedgerTar) of
        true ->
            %% if we have already unpacked it, no need to do it again
            LedgerDB = filename:join([PrivDir, "ledger.db"]),
            case filelib:is_dir(LedgerDB) of
                true ->
                    ok;
                false ->
                    %% ledger tar file present, extract
                    erl_tar:extract(LedgerTar, [compressed, {cwd, PrivDir}])
            end;
        false ->
            %% ledger tar file not found, download & extract
            ok = ssl:start(),
            {ok, {{_, 200, "OK"}, _, Body}} = httpc:request(
                "https://blockchain-core.s3-us-west-1.amazonaws.com/ledger.tar.gz"
            ),
            ok = file:write_file(filename:join([PrivDir, "ledger.tar.gz"]), Body),
            erl_tar:extract(LedgerTar, [compressed, {cwd, PrivDir}])
    end.
