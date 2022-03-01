-module(blockchain_hotspot_transfer_v2_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("blockchain_vars.hrl").
-include("blockchain.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).

-export([
    basic_validity_test/1,
    bad_owner_signature_test/1,
    gateway_not_owned_by_owner_test/1,
    unknown_gateway_test/1,
    buyback_test/1,
    var_not_set_test/1,
    owner_can_afford_test/1,
    owner_cannot_afford_test/1,
    groups/0,
    init_per_group/2,
    end_per_group/2
]).

groups() ->
    [
        {validity_ver_2, [], validity_ver_2()},
        {validity_ver_3, [], validity_ver_3()}
    ].

validity_ver_3() ->
    test_cases() ++ [owner_can_afford_test, owner_cannot_afford_test].

validity_ver_2() ->
    test_cases().

test_cases() ->
    [
        basic_validity_test,
        bad_owner_signature_test,
        gateway_not_owned_by_owner_test,
        unknown_gateway_test,
        buyback_test,
        var_not_set_test
    ].

all() ->
    [
        {group, validity_ver_2},
        {group, validity_ver_3}
    ].

%%--------------------------------------------------------------------
%% Test group setup
%%--------------------------------------------------------------------
init_per_group(validity_ver_3, Config) ->
    [{transaction_validity_version, 3} | Config];
init_per_group(validity_ver_2, Config) ->
    [{transaction_validity_version, 2} | Config].

%%--------------------------------------------------------------------
%% group teardown
%%--------------------------------------------------------------------
end_per_group(_, _Config) ->
    ok.

%%--------------------------------------------------------------------
%% TEST CASE SETUP
%%--------------------------------------------------------------------

init_per_testcase(TestCase, Config) ->
    Config0 = blockchain_ct_utils:init_base_dir_config(?MODULE, TestCase, Config),
    Balance = ?bones(300),
    {ok, Sup, {PrivKey, PubKey}, Opts} = test_utils:init(?config(base_dir, Config0)),

    ExtraVars =
        case TestCase of
            var_not_set_test -> #{};
            owner_cannot_afford_test ->
                #{?transaction_validity_version => ?config(transaction_validity_version, Config0),
                  ?txn_fees => true,
                  ?max_payments => 10
                 };
            _ ->
                #{?transaction_validity_version => ?config(transaction_validity_version, Config0)}
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
    _ = blockchain_gossip_handler:add_block(B1, Chain, self(), blockchain_swarm:tid()),

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
    _ = blockchain_gossip_handler:add_block(B2, Chain, self(), blockchain_swarm:tid()),

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

    {error, transaction_validity_version_not_set} = blockchain_txn:is_valid(OwnerSignedTxn, Chain),

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

owner_can_afford_test(Config) ->
    %% TODO: Don't meck, instead construct an original price_oracle txn...
    OraclePrice = 30,
    meck:new(blockchain_ledger_v1, [passthrough]),
    meck:expect(blockchain_ledger_v1, current_oracle_price, fun(_) -> {ok, OraclePrice} end),
    meck:expect(blockchain_ledger_v1, current_oracle_price_list, fun(_) -> {ok, [OraclePrice]} end),
    meck:expect(blockchain_ledger_v1, hnt_to_dc, fun(HNT, _) -> {ok, HNT*OraclePrice} end),

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

    Txn0 = blockchain_txn_transfer_hotspot_v2:new(
        OwnerPubkeyBin,
        OwnerPubkeyBin,
        NewOwnerPubkeyBin,
        1
    ),
    Fee0 = blockchain_txn_transfer_hotspot_v2:calculate_fee(Txn0, Chain),
    Txn = blockchain_txn_transfer_hotspot_v2:fee(Txn0, Fee0),
    OwnerSignedTxn = blockchain_txn_transfer_hotspot_v2:sign(Txn, OwnerSigFun),
    ct:pal("SignedTxn: ~p", [OwnerSignedTxn]),

    ct:pal("oracle price: ~p", [blockchain_ledger_v1:current_oracle_price(Ledger)]),

    ok = blockchain_txn:is_valid(OwnerSignedTxn, Chain),

    meck:unload(blockchain_ledger_v1),

    ok.

owner_cannot_afford_test(Config) ->
    %% Gateway owner transfers part of their balance to some other member thus failing the txn
    %% as they won't be able to afford the fee

    %% TODO: Don't meck, instead construct an original price_oracle txn...
    OraclePrice = 30,
    meck:new(blockchain_ledger_v1, [passthrough]),
    meck:expect(blockchain_ledger_v1, current_oracle_price, fun(_) -> {ok, OraclePrice} end),
    meck:expect(blockchain_ledger_v1, current_oracle_price_list, fun(_) -> {ok, [OraclePrice]} end),
    meck:expect(blockchain_ledger_v1, hnt_to_dc, fun(HNT, _) -> {ok, HNT*OraclePrice} end),

    ConsensusMembers = ?config(consensus_members, Config),
    GenesisMembers = ?config(genesis_members, Config),
    Chain = ?config(chain, Config),
    Ledger = blockchain:ledger(Chain),
    {ok, Ht} = blockchain:height(Chain),

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
    OwnerSigFun = libp2p_crypto:mk_sig_fun(OwnerPrivKey),

    ct:pal("Owner: ~p", [OwnerPubkeyBin]),
    ct:pal("OtherPubkeyBin: ~p", [OtherPubkeyBin]),

    Amt = 300,
    Payment = blockchain_payment_v2:new(OtherPubkeyBin, Amt),
    PaymentTxn0 = blockchain_txn_payment_v2:new(OwnerPubkeyBin, [Payment], 1),
    Fee = blockchain_txn_payment_v2:calculate_fee(PaymentTxn0, Chain),
    PaymentTxn = blockchain_txn_payment_v2:fee(PaymentTxn0, Fee),
    SignedPaymentTxn = blockchain_txn_payment_v2:sign(PaymentTxn, OwnerSigFun),

    ct:pal("SignedPaymentTxn: ~p", [SignedPaymentTxn]),
    ok = blockchain_txn:is_valid(SignedPaymentTxn, Chain),

    {ok, B1} = test_utils:create_block(ConsensusMembers, [SignedPaymentTxn]),
    _ = blockchain_gossip_handler:add_block(B1, Chain, self(), blockchain_swarm:tid()),

    ct:pal("New Balance: ~p", [blockchain_ledger_v1:find_entry(OwnerPubkeyBin, Ledger)]),

    ok = test_utils:wait_until(fun() -> {ok, Ht + 1} =:= blockchain:height(Chain) end),

    ct:pal("Owner: ~p", [OwnerPubkeyBin]),
    ct:pal("Gateway: ~p", [Gateway]),
    ct:pal("NewOwnerPubkeyBin: ~p", [NewOwnerPubkeyBin]),

    ct:pal("Owner Entry: ~p", [blockchain_ledger_v1:find_entry(OwnerPubkeyBin, Ledger)]),
    ct:pal("Owner DCEntry: ~p", [blockchain_ledger_v1:find_dc_entry(OwnerPubkeyBin, Ledger)]),

    Txn0 = blockchain_txn_transfer_hotspot_v2:new(
        OwnerPubkeyBin,
        OwnerPubkeyBin,
        NewOwnerPubkeyBin,
        1
    ),
    Fee0 = blockchain_txn_transfer_hotspot_v2:calculate_fee(Txn0, Chain),
    Txn = blockchain_txn_transfer_hotspot_v2:fee(Txn0, Fee0),
    OwnerSignedTxn = blockchain_txn_transfer_hotspot_v2:sign(Txn, OwnerSigFun),
    ct:pal("SignedTxn: ~p", [OwnerSignedTxn]),

    ct:pal("oracle price: ~p", [blockchain_ledger_v1:current_oracle_price(Ledger)]),

    {error, gateway_owner_cannot_pay_fee} = blockchain_txn:is_valid(OwnerSignedTxn, Chain),

    meck:unload(blockchain_ledger_v1),

    ok.
