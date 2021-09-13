-module(blockchain_hotspot_transfer_v2_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("blockchain_vars.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).

-export([
    basic_validity_test/1,
    bad_owner_signature_test/1,
    gateway_not_owned_by_owner_test/1
]).

all() ->
    [
        basic_validity_test,
        bad_owner_signature_test,
        gateway_not_owned_by_owner_test
    ].

%%--------------------------------------------------------------------
%% TEST CASE SETUP
%%--------------------------------------------------------------------

init_per_testcase(TestCase, Config) ->
    Config0 = blockchain_ct_utils:init_base_dir_config(?MODULE, TestCase, Config),
    Balance = 5000,
    {ok, Sup, {PrivKey, PubKey}, Opts} = test_utils:init(?config(base_dir, Config0)),

    ExtraVars =
        case TestCase of
            %% default is 0
            gateway_stale_test -> #{};
            _ -> #{?transfer_hotspot_stale_poc_blocks => 10}
        end,

    {ok, GenesisMembers, _GenesisBlock, ConsensusMembers, Keys} =
        test_utils:init_chain(Balance, {PrivKey, PubKey}, true, ExtraVars),

    Chain = blockchain_worker:blockchain(),
    Swarm = blockchain_swarm:swarm(),
    N = length(ConsensusMembers),

    % Check ledger to make sure everyone has the right balance
    Ledger = blockchain:ledger(Chain),
    Entries = blockchain_ledger_v1:entries(Ledger),
    _ = lists:foreach(
        fun(Entry) ->
            Balance = blockchain_ledger_entry_v1:balance(Entry),
            0 = blockchain_ledger_entry_v1:nonce(Entry)
        end,
        maps:values(Entries)
    ),

    [
        {balance, Balance},
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
    % Make sure blockchain saved on file = in memory
    case erlang:is_process_alive(Sup) of
        true ->
            true = erlang:exit(Sup, normal),
            ok = test_utils:wait_until(fun() -> false =:= erlang:is_process_alive(Sup) end);
        false ->
            ok
    end,
    ok = test_utils:cleanup_tmp_dir(?config(base_dir, Config)),
    ok.

%%--------------------------------------------------------------------
%% TEST CASES
%%--------------------------------------------------------------------

basic_validity_test(Config) ->
    GenesisMembers = ?config(genesis_members, Config),
    Chain = ?config(chain, Config),
    Ledger = blockchain:ledger(Chain),

    %% In the test, OwnerPubkeyBin = GatewayPubkeyBin

    %% Get some owner and their gateway
    [
        {OwnerPubkeyBin, Gateway},
        {NewOwnerPubkeyBin, _}
        | _
    ] = maps:to_list(blockchain_ledger_v1:active_gateways(Ledger)),

    %% Get owner privkey and sigfun
    {_OwnerPubkey, OwnerPrivKey, _} = proplists:get_value(OwnerPubkeyBin, GenesisMembers),
    OwnerSigFun = libp2p_crypto:mk_sig_fun(OwnerPrivKey),

    ct:pal("Owner: ~p", [OwnerPubkeyBin]),
    ct:pal("Gateway: ~p", [Gateway]),
    ct:pal("NewOwnerPubkeyBin: ~p", [NewOwnerPubkeyBin]),

    Txn = blockchain_txn_transfer_hotspot_v2:new(
        OwnerPubkeyBin,
        OwnerPubkeyBin,
        NewOwnerPubkeyBin
    ),
    OwnerSignedTxn = blockchain_txn_transfer_hotspot_v2:sign(Txn, OwnerSigFun),
    ct:pal("SignedTxn: ~p", [OwnerSignedTxn]),
    ct:pal("IsValidOwner: ~p", [blockchain_txn_transfer_hotspot_v2:is_valid_owner(OwnerSignedTxn)]),

    ok = blockchain_txn:is_valid(OwnerSignedTxn, Chain),

    ok.

bad_owner_signature_test(Config) ->
    GenesisMembers = ?config(genesis_members, Config),
    Chain = ?config(chain, Config),
    Ledger = blockchain:ledger(Chain),

    %% In the test, OwnerPubkeyBin = GatewayPubkeyBin

    %% Get some owner and their gateway
    [
        {OwnerPubkeyBin, Gateway},
        {NewOwnerPubkeyBin, _},
        {OtherPubkeyBin, _}
        | _
    ] = maps:to_list(blockchain_ledger_v1:active_gateways(Ledger)),

    %% Get owner privkey and sigfun
    {_OwnerPubkey, OwnerPrivKey, _} = proplists:get_value(OwnerPubkeyBin, GenesisMembers),
    _OwnerSigFun = libp2p_crypto:mk_sig_fun(OwnerPrivKey),

    %% Get new owner privkey and sigfun
    {_OtherPubkey, OtherPrivKey, _} = proplists:get_value(OtherPubkeyBin, GenesisMembers),
    OtherSigFun = libp2p_crypto:mk_sig_fun(OtherPrivKey),

    ct:pal("Owner: ~p", [OwnerPubkeyBin]),
    ct:pal("Gateway: ~p", [Gateway]),
    ct:pal("NewOwnerPubkeyBin: ~p", [NewOwnerPubkeyBin]),

    Txn = blockchain_txn_transfer_hotspot_v2:new(
        OwnerPubkeyBin,
        OwnerPubkeyBin,
        NewOwnerPubkeyBin
    ),

    %% this is not owner's signature
    OtherSignedTxn = blockchain_txn_transfer_hotspot_v2:sign(Txn, OtherSigFun),

    ct:pal("SignedTxn: ~p", [OtherSignedTxn]),
    ct:pal("IsValidOwner: ~p", [blockchain_txn_transfer_hotspot_v2:is_valid_owner(OtherSignedTxn)]),

    {error, bad_owner_signature} = blockchain_txn:is_valid(OtherSignedTxn, Chain),

    ok.

gateway_not_owned_by_owner_test(Config) ->
    GenesisMembers = ?config(genesis_members, Config),
    Chain = ?config(chain, Config),
    Ledger = blockchain:ledger(Chain),

    %% In the test, OwnerPubkeyBin = GatewayPubkeyBin

    %% Get some owner and their gateway
    [
        {OwnerPubkeyBin, Gateway},
        {NewOwnerPubkeyBin, _}
        | _
    ] = maps:to_list(blockchain_ledger_v1:active_gateways(Ledger)),

    %% Get owner privkey and sigfun
    {_OwnerPubkey, OwnerPrivKey, _} = proplists:get_value(OwnerPubkeyBin, GenesisMembers),
    OwnerSigFun = libp2p_crypto:mk_sig_fun(OwnerPrivKey),

    ct:pal("Owner: ~p", [OwnerPubkeyBin]),
    ct:pal("Gateway: ~p", [Gateway]),
    ct:pal("NewOwnerPubkeyBin: ~p", [NewOwnerPubkeyBin]),

    %% this is not the seller's gw
    Txn = blockchain_txn_transfer_hotspot_v2:new(
        NewOwnerPubkeyBin,
        OwnerPubkeyBin,
        NewOwnerPubkeyBin
    ),
    OwnerSignedTxn = blockchain_txn_transfer_hotspot_v2:sign(Txn, OwnerSigFun),
    ct:pal("SignedTxn: ~p", [OwnerSignedTxn]),
    ct:pal("IsValidOwner: ~p", [blockchain_txn_transfer_hotspot_v2:is_valid_owner(OwnerSignedTxn)]),

    {error, gateway_not_owned_by_owner} = blockchain_txn:is_valid(OwnerSignedTxn, Chain),

    ok.
