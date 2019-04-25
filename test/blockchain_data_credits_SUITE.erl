-module(blockchain_data_credits_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).

-export([
    basic_test/1
]).

-include("blockchain.hrl").

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

init_per_testcase(TestCase, Config) ->
    BaseDir = "data/test_SUITE/" ++ erlang:atom_to_list(TestCase),
    Balance = 5000,
    {ok, Sup, {PrivKey, PubKey}, Opts} = test_utils:init(BaseDir),
    {ok, ConsensusMembers} = test_utils:init_chain(Balance, {PrivKey, PubKey}),

    Chain = blockchain_worker:blockchain(),
    Swarm = blockchain_swarm:swarm(),
    N = length(ConsensusMembers),

    % Check ledger to make sure everyone has the right balance
    Ledger = blockchain:ledger(Chain),
    Entries = blockchain_ledger_v1:entries(Ledger),
    _ = lists:foreach(fun(Entry) ->
        Balance = blockchain_ledger_entry_v1:balance(Entry),
        0 = blockchain_ledger_entry_v1:nonce(Entry)
    end, maps:values(Entries)),


    [
        {basedir, BaseDir},
        {balance, Balance},
        {sup, Sup},
        {pubkey, PubKey},
        {privkey, PrivKey},
        {opts, Opts},
        {chain, Chain},
        {swarm, Swarm},
        {n, N},
        {consensus_members, ConsensusMembers} | Config
    ].

%%--------------------------------------------------------------------
%% TEST CASE TEARDOWN
%%--------------------------------------------------------------------
end_per_testcase(_, Config) ->
    Sup = proplists:get_value(sup, Config),
    % Make sure blockchain saved on file = in memory
    case erlang:is_process_alive(Sup) of
        true ->
            true = erlang:exit(Sup, normal),
            ok = test_utils:wait_until(fun() -> false =:= erlang:is_process_alive(Sup) end);
        false ->
            ok
    end,
    ok.

%%--------------------------------------------------------------------
%% TEST CASES
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @public
%% @doc
%% @end
%%--------------------------------------------------------------------
basic_test(_Config) ->
    Keys = libp2p_crypto:generate_keys(ecc_compact),

    ok = blockchain_data_credits_server_monitor:channel_server(Keys, 100),

    #{public := PubKey} = Keys,
    PubKeyBin = libp2p_crypto:pubkey_to_bin(PubKey),
    {ok, ChannelServer} = blockchain_data_credits_server_monitor:channel_server(PubKeyBin),

    ?assertEqual({ok, 100}, blockchain_data_credits_channel_server:credits(ChannelServer)),

    % Txn = blockchain_txn_data_credits_v1:new(<<"payer">>, libp2p_crypto:pubkey_to_bin(PubKey), 1000, 1),
    %  Swarm = proplists:get_value(swarm, Config),
    % {ok, TmpSwarm} = libp2p_swarm:start(data_credits_basic_test, [{libp2p_nat, [{enabled, false}]}]),
    % [Addr|_] = libp2p_swarm:listen_addrs(Swarm),

    % case libp2p_swarm:dial_framed_stream(TmpSwarm,
    %                                      Addr,
    %                                      ?DATA_CREDITS_PROTOCOL,
    %                                      blockchain_data_credits_handler,
    %                                      [])
    % of
    %     {ok, Stream} ->
    %         Stream ! {payment_req, 50},
    %         timer:sleep(1000),
    %         ?assertEqual({ok, 50}, blockchain_data_credits_channel_server:credits(ChannelServer));
    %     Error ->
    %         ct:fail(Error)
    % end,
    % libp2p_swarm:stop(TmpSwarm),
    ok.
