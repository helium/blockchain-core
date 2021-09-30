-module(blockchain_hotspot_transfer_v2_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("blockchain_vars.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).

-export([
    basic_validity_test/1,
    bad_owner_signature_test/1,
    gateway_not_owned_by_owner_test/1,
    unknown_gateway_test/1,
    buyback_test/1,
    var_not_set_test/1
]).

all() ->
    [
        basic_validity_test,
        bad_owner_signature_test,
        gateway_not_owned_by_owner_test,
        unknown_gateway_test,
        buyback_test,
        var_not_set_test
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
            var_not_set_test -> #{};
            _ -> #{?transaction_validity_version => 2}
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
        NewOwnerPubkeyBin,
        1
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
        NewOwnerPubkeyBin,
        1
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
        NewOwnerPubkeyBin,
        1
    ),
    OwnerSignedTxn = blockchain_txn_transfer_hotspot_v2:sign(Txn, OwnerSigFun),
    ct:pal("SignedTxn: ~p", [OwnerSignedTxn]),
    ct:pal("IsValidOwner: ~p", [blockchain_txn_transfer_hotspot_v2:is_valid_owner(OwnerSignedTxn)]),

    {error, gateway_not_owned_by_owner} = blockchain_txn:is_valid(OwnerSignedTxn, Chain),

    ok.

buyback_test(Config) ->
    %% Txn1: Seller -> Buyer
    %% Txn2: Buyer -> Seller
    GenesisMembers = ?config(genesis_members, Config),
    ConsensusMembers = ?config(consensus_members, Config),
    Chain = ?config(chain, Config),
    Ledger = blockchain:ledger(Chain),
    {ok, Ht} = blockchain:height(Chain),

    %% In the test, SellerPubkeyBin = GatewayPubkeyBin

    %% Get some owner and their gateway
    [
        {SellerPubkeyBin, Gateway},
        {BuyerPubkeyBin, _}
        | _
    ] = maps:to_list(blockchain_ledger_v1:active_gateways(Ledger)),

    GatewayPubkeyBin = SellerPubkeyBin,

    %% Get seller privkey and sigfun
    {_SellerPubkey, SellerPrivKey, _} = proplists:get_value(SellerPubkeyBin, GenesisMembers),
    SellerSigFun = libp2p_crypto:mk_sig_fun(SellerPrivKey),

    %% Get buyer privkey and sigfun, for buyback transaction later
    {_BuyerPubkey, BuyerPrivKey, _} = proplists:get_value(BuyerPubkeyBin, GenesisMembers),
    BuyerSigFun = libp2p_crypto:mk_sig_fun(BuyerPrivKey),

    ct:pal("Owner: ~p", [SellerPubkeyBin]),
    ct:pal("Gateway: ~p", [Gateway]),
    ct:pal("BuyerPubkeyBin: ~p", [BuyerPubkeyBin]),

    Txn1 = blockchain_txn_transfer_hotspot_v2:new(
        GatewayPubkeyBin,
        SellerPubkeyBin,
        BuyerPubkeyBin,
        1
    ),
    STxn1 = blockchain_txn_transfer_hotspot_v2:sign(Txn1, SellerSigFun),
    ct:pal("SignedTxn: ~p", [STxn1]),
    ct:pal("IsValidOwner: ~p", [blockchain_txn_transfer_hotspot_v2:is_valid_owner(STxn1)]),

    ok = blockchain_txn:is_valid(STxn1, Chain),

    {ok, B1} = test_utils:create_block(ConsensusMembers, [STxn1]),
    _ = blockchain_gossip_handler:add_block(B1, Chain, self(), blockchain_swarm:swarm()),

    ok = test_utils:wait_until(fun() -> {ok, Ht + 1} =:= blockchain:height(Chain) end),

    Txn2 = blockchain_txn_transfer_hotspot_v2:new(
        GatewayPubkeyBin,
        BuyerPubkeyBin,
        SellerPubkeyBin,
        2
    ),
    STxn2 = blockchain_txn_transfer_hotspot_v2:sign(Txn2, BuyerSigFun),
    ct:pal("SignedTxn: ~p", [STxn2]),
    ct:pal("IsValidOwner: ~p", [blockchain_txn_transfer_hotspot_v2:is_valid_owner(STxn2)]),

    ok = blockchain_txn:is_valid(STxn2, Chain),

    {ok, B2} = test_utils:create_block(ConsensusMembers, [STxn2]),
    _ = blockchain_gossip_handler:add_block(B2, Chain, self(), blockchain_swarm:swarm()),

    ok = test_utils:wait_until(fun() -> {ok, Ht + 1 + 1} =:= blockchain:height(Chain) end),

    %% Check: Txn1 should be invalid if resubmitted after Txn2
    {error, {invalid_nonce, _}} = blockchain_txn:is_valid(STxn1, Chain),

    ok.

var_not_set_test(Config) ->
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
        NewOwnerPubkeyBin,
        1
    ),
    OwnerSignedTxn = blockchain_txn_transfer_hotspot_v2:sign(Txn, OwnerSigFun),
    ct:pal("SignedTxn: ~p", [OwnerSignedTxn]),
    ct:pal("IsValidOwner: ~p", [blockchain_txn_transfer_hotspot_v2:is_valid_owner(OwnerSignedTxn)]),

    {error, transfer_hotspot_txn_version_not_set} = blockchain_txn:is_valid(OwnerSignedTxn, Chain),

    ok.

unknown_gateway_test(Config) ->
    GenesisMembers = ?config(genesis_members, Config),
    Chain = ?config(chain, Config),
    Ledger = blockchain:ledger(Chain),

    %% Get some owner and their gateway
    [
        {OwnerPubkeyBin, _},
        {NewOwnerPubkeyBin, _}
        | _
    ] = maps:to_list(blockchain_ledger_v1:active_gateways(Ledger)),

    %% Get owner privkey and sigfun
    {_OwnerPubkey, OwnerPrivKey, _} = proplists:get_value(OwnerPubkeyBin, GenesisMembers),
    OwnerSigFun = libp2p_crypto:mk_sig_fun(OwnerPrivKey),

    ct:pal("Owner: ~p", [OwnerPubkeyBin]),
    ct:pal("NewOwnerPubkeyBin: ~p", [NewOwnerPubkeyBin]),

    %% Generate some random key to use as gateway pubkey bin

    [{UnknownGwPubkeyBin, {_, _, _}}] = test_utils:generate_keys(1),
    ct:pal("Gateway: ~p", [UnknownGwPubkeyBin]),

    Txn = blockchain_txn_transfer_hotspot_v2:new(
        UnknownGwPubkeyBin,
        OwnerPubkeyBin,
        NewOwnerPubkeyBin,
        1
    ),
    OwnerSignedTxn = blockchain_txn_transfer_hotspot_v2:sign(Txn, OwnerSigFun),
    ct:pal("SignedTxn: ~p", [OwnerSignedTxn]),
    ct:pal("IsValidOwner: ~p", [blockchain_txn_transfer_hotspot_v2:is_valid_owner(OwnerSignedTxn)]),

    {error, unknown_gateway} = blockchain_txn:is_valid(OwnerSignedTxn, Chain),

    ok.
