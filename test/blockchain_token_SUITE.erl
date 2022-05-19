-module(blockchain_token_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("blockchain_vars.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).

-export([
    coinbase_test/1
]).

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
        coinbase_test
    ].

%%--------------------------------------------------------------------
%% TEST CASE SETUP
%%--------------------------------------------------------------------

init_per_testcase(TestCase, Config) ->
    Config0 = blockchain_ct_utils:init_base_dir_config(?MODULE, TestCase, Config),

    HNTBal = 5000,
    HGTBal = 1000,
    HSTBal = 100,
    HLTBal = 10,

    {ok, Sup, {PrivKey, PubKey}, Opts} = test_utils:init(?config(base_dir, Config0)),

    ExtraVars = #{?protocol_version => 2},

    {ok, GenesisMembers, _GenesisBlock, ConsensusMembers, Keys} =
        test_utils:init_chain_with_opts(
            #{
                balance =>
                    HNTBal,
                keys =>
                    {PrivKey, PubKey},
                in_consensus =>
                    false,
                have_init_dc =>
                    true,
                extra_vars =>
                    ExtraVars,
                token_allocations =>
                    #{hnt => HNTBal, hst => HSTBal, hgt => HGTBal, hlt => HLTBal}
            }
        ),

    Chain = blockchain_worker:blockchain(),
    Swarm = blockchain_swarm:tid(),
    N = length(ConsensusMembers),

    [
        {hnt_bal, HNTBal},
        {hst_bal, HSTBal},
        {hgt_bal, HGTBal},
        {hlt_bal, HLTBal},
        {sup, Sup},
        {pubkey, PubKey},
        {privkey, PrivKey},
        {opts, Opts},
        {chain, Chain},
        {swarm, Swarm},
        {n, N},
        {consensus_members, ConsensusMembers},
        {genesis_members, GenesisMembers},
        Keys
        | Config0
    ].

%%--------------------------------------------------------------------
%% TEST CASE TEARDOWN
%%--------------------------------------------------------------------
end_per_testcase(_, Config) ->
    Sup = ?config(sup, Config),
    meck:unload(),
    % Make sure blockchain saved on file = in memory
    case erlang:is_process_alive(Sup) of
        true ->
            true = erlang:exit(Sup, normal),
            ok = test_utils:wait_until(fun() -> false =:= erlang:is_process_alive(Sup) end);
        false ->
            ok
    end,
    test_utils:cleanup_tmp_dir(?config(base_dir, Config)),
    {comment, done}.

%%--------------------------------------------------------------------
%% TEST CASES
%%--------------------------------------------------------------------

coinbase_test(Config) ->
    Chain = ?config(chain, Config),
    HNTBal = ?config(hnt_bal, Config),
    HSTBal = ?config(hst_bal, Config),
    HGTBal = ?config(hgt_bal, Config),
    HLTBal = ?config(hlt_bal, Config),

    % Check ledger to make sure everyone has the right balance
    Ledger = blockchain:ledger(Chain),
    Entries = blockchain_ledger_v1:entries_v2(Ledger),
    _ = lists:foreach(
        fun(Entry) ->
            HNTBal = blockchain_ledger_entry_v2:balance(Entry, hnt),
            0 = blockchain_ledger_entry_v2:nonce(Entry, hnt),
            HSTBal = blockchain_ledger_entry_v2:balance(Entry, hst),
            0 = blockchain_ledger_entry_v2:nonce(Entry, hst),
            HGTBal = blockchain_ledger_entry_v2:balance(Entry, hgt),
            0 = blockchain_ledger_entry_v2:nonce(Entry, hgt),
            HLTBal = blockchain_ledger_entry_v2:balance(Entry, hlt),
            0 = blockchain_ledger_entry_v2:nonce(Entry, hlt)
        end,
        maps:values(Entries)
    ),

    %% TODO: Test a few payment v2 txns
    ok.
