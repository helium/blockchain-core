-module(blockchain_absorbed_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("blockchain_vars.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).

-define(TEST_LOCATION, 631210968840687103).

-export([
    absorbed_txn_add_gateway_and_location_v1_test/1,
    absorbed_txn_payment_v1_test/1,
    absorbed_txn_htlc_v1_test/1,
    absorbed_txn_oui_v1_test/1,
    absorbed_txn_poc_receipts_v1/1,
    absorbed_txn_routing_v1_test/1,
    absorbed_txn_security_exchange_v1_test/1,
    absorbed_txn_token_burn_v1_test/1,
    absorbed_txn_vars_v1_test/1
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
        absorbed_txn_add_gateway_and_location_v1_test,
        absorbed_txn_payment_v1_test,
        absorbed_txn_htlc_v1_test,
        absorbed_txn_oui_v1_test,
        absorbed_txn_poc_receipts_v1,
        absorbed_txn_routing_v1_test,
        absorbed_txn_security_exchange_v1_test,
        absorbed_txn_token_burn_v1_test,
        absorbed_txn_vars_v1_test

    ].

%%--------------------------------------------------------------------
%% TEST CASE SETUP
%%--------------------------------------------------------------------

init_per_testcase(TestCase, Config) ->
    BaseDir = "data/test_SUITE/" ++ erlang:atom_to_list(TestCase),
    Balance = 5000,
    {ok, Sup, {PrivKey, PubKey}, Opts} = test_utils:init(BaseDir),
    {ok, GenesisMembers, ConsensusMembers, Keys} = test_utils:init_chain(Balance, {PrivKey, PubKey}),

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
        {consensus_members, ConsensusMembers},
        {genesis_members, GenesisMembers},
        Keys
        | Config
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
absorbed_txn_add_gateway_and_location_v1_test(Config) ->
    ConsensusMembers = proplists:get_value(consensus_members, Config),
    PubKey = proplists:get_value(pubkey, Config),
    PrivKey = proplists:get_value(privkey, Config),
    _Owner = libp2p_crypto:pubkey_to_bin(PubKey),
    Chain = proplists:get_value(chain, Config),
    Swarm = proplists:get_value(swarm, Config),
    _Balance = proplists:get_value(balance, Config),
    OwnerSigFun = libp2p_crypto:mk_sig_fun(PrivKey),

    #{public := GatewayPubKey, secret := GatewayPrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Gateway = libp2p_crypto:pubkey_to_bin(GatewayPubKey),
    GatewaySigFun = libp2p_crypto:mk_sig_fun(GatewayPrivKey),
    OwnerSigFun = libp2p_crypto:mk_sig_fun(PrivKey),

    Rate = 1000000,
    {Priv, _} = proplists:get_value(master_key, Config),
    Vars = #{token_burn_exchange_rate => Rate},
    VarTxn = blockchain_txn_vars_v1:new(Vars, 3),
    Proof = blockchain_txn_vars_v1:create_proof(Priv, VarTxn),
    VarTxn1 = blockchain_txn_vars_v1:proof(VarTxn, Proof),
    Block1 = test_utils:create_block(ConsensusMembers, [VarTxn1]),
    _ = blockchain_gossip_handler:add_block(Swarm, Block1, Chain, self()),

    %% We need a gateway added for a couple of tests, so pushed out to a reusable function
    p_create_and_test_gateway(Config, Gateway, GatewaySigFun, OwnerSigFun, Rate).


%%--------------------------------------------------------------------
%% @public
%% @doc
%% @end
%%--------------------------------------------------------------------
absorbed_txn_htlc_v1_test(Config) ->
    BaseDir = proplists:get_value(basedir, Config),
    ConsensusMembers = proplists:get_value(consensus_members, Config),
    BaseDir = proplists:get_value(basedir, Config),
    Chain = proplists:get_value(chain, Config),
    Swarm = proplists:get_value(swarm, Config),

    %% create the htlc txn
    {Payer, {_, _, PayerSigFun}} = lists:last(ConsensusMembers),
    % Generate a random address
    HTLCAddress = crypto:strong_rand_bytes(32),
    % Create a Hashlock
    Hashlock = crypto:hash(sha256, <<"sharkfed">>),

    Recipient = blockchain_swarm:pubkey_bin(),
    Txn1 = blockchain_txn_create_htlc_v1:new(Payer, Recipient, HTLCAddress, Hashlock, 3, 2500, 0, 1),
    SignedTxn1 = blockchain_txn_create_htlc_v1:sign(Txn1, PayerSigFun),

    %% before we add the block verify it validates
    ?assertEqual(ok, blockchain_txn_create_htlc_v1:is_valid(SignedTxn1, Chain)),

    %% confirm the txn has not yet been absorbed
    ?assertEqual(false, blockchain_txn_create_htlc_v1:absorbed(SignedTxn1, Chain)),

    Block1 = test_utils:create_block(ConsensusMembers, [SignedTxn1]),
    _ = blockchain_gossip_handler:add_block(Swarm, Block1, Chain, self()),

    ok = blockchain_ct_utils:wait_until(fun() -> {ok, 2} =:= blockchain:height(Chain) end),

    %% then check absorbed again on same block, it should return true
    ?assertEqual(true, blockchain_txn_create_htlc_v1:absorbed(Txn1, Chain)),
    %% but it should continue to pass validation
    ?assertEqual(ok, blockchain_txn_create_htlc_v1:is_valid(SignedTxn1, Chain)).


%%--------------------------------------------------------------------
%% @public
%% @doc
%% @end
%%--------------------------------------------------------------------
absorbed_txn_oui_v1_test(Config) ->
    BaseDir = proplists:get_value(basedir, Config),
    ConsensusMembers = proplists:get_value(consensus_members, Config),
    BaseDir = proplists:get_value(basedir, Config),
    Chain = proplists:get_value(chain, Config),
    Swarm = proplists:get_value(swarm, Config),
    _Ledger = blockchain:ledger(Chain),

    %% create the txn
    [_, {Payer, {_, PayerPrivKey, _}}|_] = ConsensusMembers,
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),

    meck:new(blockchain_txn_oui_v1, [no_link, passthrough]),
    meck:expect(blockchain_txn_oui_v1, is_valid, fun(_, _) -> ok end),

    OUI1 = 1,
    Addresses = [erlang:list_to_binary(libp2p_swarm:p2p_address(Swarm))],
    Txn1 = blockchain_txn_oui_v1:new(Payer, Addresses, OUI1, 0, 0),
    SignedTxn1 = blockchain_txn_oui_v1:sign(Txn1, SigFun),

    %% before we add the block verify it validates
    ?assertEqual(ok, blockchain_txn_oui_v1:is_valid(Txn1, Chain)),
    %% confirm the txn has not previously been absorbed
    ?assertEqual(false, blockchain_txn_oui_v1:absorbed(Txn1, Chain)),

    %% create and add the block
    Block1 = test_utils:create_block(ConsensusMembers, [SignedTxn1]),
    _ = blockchain_gossip_handler:add_block(Swarm, Block1, Chain, self()),

    ok = blockchain_ct_utils:wait_until(fun() -> {ok, 2} =:= blockchain:height(Chain) end),

    %% then check absorbed again on same block, it should return true
    ?assertEqual(true, blockchain_txn_oui_v1:absorbed(Txn1, Chain)),
    %% but it should continue to pass validation
    ?assertEqual(ok, blockchain_txn_oui_v1:is_valid(Txn1, Chain)),

    meck:unload(blockchain_txn_oui_v1),
    ok.


%%--------------------------------------------------------------------
%% @public
%% @doc
%% @end
%%--------------------------------------------------------------------
absorbed_txn_payment_v1_test(Config) ->
    BaseDir = proplists:get_value(basedir, Config),
    ConsensusMembers = proplists:get_value(consensus_members, Config),
    BaseDir = proplists:get_value(basedir, Config),
    Chain = proplists:get_value(chain, Config),
    Swarm = proplists:get_value(swarm, Config),

    %% create the payment txn
    {Payer, {_, _, PayerSigFun}} = lists:last(ConsensusMembers),
    Recipient = blockchain_swarm:pubkey_bin(),
    Txn1 = blockchain_txn_payment_v1:new(Payer, Recipient, 2500, 0, 1),
    SignedTxn1 = blockchain_txn_payment_v1:sign(Txn1, PayerSigFun),

    %% before we add the block verify it validates
    ?assertEqual(ok, blockchain_txn_payment_v1:is_valid(SignedTxn1, Chain)),
    %% confirm the txn has not yet been absorbed
    ?assertEqual(false, blockchain_txn_payment_v1:absorbed(Txn1, Chain)),

    Block1 = test_utils:create_block(ConsensusMembers, [SignedTxn1]),
    _ = blockchain_gossip_handler:add_block(Swarm, Block1, Chain, self()),

    ok = blockchain_ct_utils:wait_until(fun() -> {ok, 2} =:= blockchain:height(Chain) end),

    %% then check absorbed again on same block, it should return true
    ?assertEqual(true, blockchain_txn_payment_v1:absorbed(Txn1, Chain)),
    %% but it should continue to pass validation
    ?assertEqual(ok, blockchain_txn_payment_v1:is_valid(SignedTxn1, Chain)).



%%--------------------------------------------------------------------
%% @public
%% @doc
%% @end
%%--------------------------------------------------------------------
absorbed_txn_poc_receipts_v1(Config) ->
    ConsensusMembers = proplists:get_value(consensus_members, Config),
    PubKey = proplists:get_value(pubkey, Config),
    PrivKey = proplists:get_value(privkey, Config),
    _Owner = libp2p_crypto:pubkey_to_bin(PubKey),
    Chain = proplists:get_value(chain, Config),
    Swarm = proplists:get_value(swarm, Config),
    _Balance = proplists:get_value(balance, Config),
    OwnerSigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Ledger = blockchain:ledger(Chain),

    #{public := GatewayPubKey, secret := GatewayPrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Gateway = libp2p_crypto:pubkey_to_bin(GatewayPubKey),
    GatewaySigFun = libp2p_crypto:mk_sig_fun(GatewayPrivKey),
    OwnerSigFun = libp2p_crypto:mk_sig_fun(PrivKey),

    Rate = 1000000,
    {Priv, _} = proplists:get_value(master_key, Config),
    Vars = #{token_burn_exchange_rate => Rate},
    Txn1 = blockchain_txn_vars_v1:new(Vars, 3),
    Proof = blockchain_txn_vars_v1:create_proof(Priv, Txn1),
    Txn2 = blockchain_txn_vars_v1:proof(Txn1, Proof),
    Block1 = test_utils:create_block(ConsensusMembers, [Txn2]),
    _ = blockchain_gossip_handler:add_block(Swarm, Block1, Chain, self()),

    %% add the gateway ( NOTE: this will generate 25 blocks )
    p_create_and_test_gateway(Config, Gateway, GatewaySigFun, OwnerSigFun, Rate),

    % Create the PoC challenge request txn
    Keys0 = libp2p_crypto:generate_keys(ecc_compact),
    Secret0 = libp2p_crypto:keys_to_bin(Keys0),
    #{public := OnionCompactKey0} = Keys0,
    SecretHash0 = crypto:hash(sha256, Secret0),
    OnionKeyHash0 = crypto:hash(sha256, libp2p_crypto:pubkey_to_bin(OnionCompactKey0)),
    Txn3 = blockchain_txn_poc_request_v1:new(Gateway, SecretHash0, OnionKeyHash0, blockchain_block:hash_block(Block1), 1),
    SignedTxn3 = blockchain_txn_poc_request_v1:sign(Txn3, GatewaySigFun),
    Block26 = test_utils:create_block(ConsensusMembers, [SignedTxn3]),
    _ = blockchain_gossip_handler:add_block(Swarm, Block26, Chain, self()),
    ok = blockchain_ct_utils:wait_until(fun() -> {ok, 26} =:= blockchain:height(Chain) end),

    Ledger = blockchain:ledger(Chain),
    {ok, HeadHash3} = blockchain:head_hash(Chain),
    ?assertEqual(blockchain_block:hash_block(Block26), HeadHash3),
    ?assertEqual({ok, Block26}, blockchain:get_block(HeadHash3, Chain)),
    % Check that the last_poc_challenge block height got recorded in GwInfo
    {ok, GwInfo2} = blockchain_ledger_v1:find_gateway_info(Gateway, Ledger),
    ?assertEqual(26, blockchain_ledger_gateway_v2:last_poc_challenge(GwInfo2)),
    ?assertEqual(OnionKeyHash0, blockchain_ledger_gateway_v2:last_poc_onion_key_hash(GwInfo2)),

    % Check that the PoC info
    {ok, [PoC]} = blockchain_ledger_v1:find_poc(OnionKeyHash0, Ledger),
    ?assertEqual(SecretHash0, blockchain_ledger_poc_v2:secret_hash(PoC)),
    ?assertEqual(OnionKeyHash0, blockchain_ledger_poc_v2:onion_key_hash(PoC)),
    ?assertEqual(Gateway, blockchain_ledger_poc_v2:challenger(PoC)),

    meck:new(blockchain_txn_poc_receipts_v1, [passthrough]),
    meck:expect(blockchain_txn_poc_receipts_v1, is_valid, fun(_Txn, _Chain) -> ok end),

    Txn4 = blockchain_txn_poc_receipts_v1:new(Gateway, Secret0, OnionKeyHash0, []),
    SignedTxn4 = blockchain_txn_poc_receipts_v1:sign(Txn4, GatewaySigFun),
    Block27 = test_utils:create_block(ConsensusMembers, [SignedTxn4]),

    %% before we add the block verify it validates
    ?assertEqual(ok, blockchain_txn_poc_receipts_v1:is_valid(Txn4, Chain)),
    %% confirm the txn has not yet been absorbed
    ?assertEqual(false, blockchain_txn_poc_receipts_v1:absorbed(Txn4, Chain)),

    _ = blockchain_gossip_handler:add_block(Swarm, Block27, Chain, self()),
    ok = blockchain_ct_utils:wait_until(fun() -> {ok, 27} =:= blockchain:height(Chain) end),

    %% then check absorbed again on same block, it should return true
    ?assertEqual(true, blockchain_txn_poc_receipts_v1:absorbed(Txn4, Chain)),
    %% but it should continue to pass validation
    ?assertEqual(ok, blockchain_txn_poc_receipts_v1:is_valid(Txn4, Chain)),

    meck:unload(blockchain_txn_poc_receipts_v1),
    ok.


%%--------------------------------------------------------------------
%% @public
%% @doc
%% @end
%%--------------------------------------------------------------------
absorbed_txn_routing_v1_test(Config) ->
    BaseDir = proplists:get_value(basedir, Config),
    ConsensusMembers = proplists:get_value(consensus_members, Config),
    BaseDir = proplists:get_value(basedir, Config),
    Chain = proplists:get_value(chain, Config),
    Swarm = proplists:get_value(swarm, Config),
    Ledger = blockchain:ledger(Chain),

    [_, {Payer, {_, PayerPrivKey, _}}|_] = ConsensusMembers,
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),

    meck:new(blockchain_txn_oui_v1, [no_link, passthrough]),
    meck:expect(blockchain_txn_oui_v1, is_valid, fun(_, _) -> ok end),

    OUI1 = 1,
    Addresses0 = [erlang:list_to_binary(libp2p_swarm:p2p_address(Swarm))],
    Txn1 = blockchain_txn_oui_v1:new(Payer, Addresses0, OUI1, 0, 0),
    SignedTxn1 = blockchain_txn_oui_v1:sign(Txn1, SigFun),

    ?assertEqual({error, not_found}, blockchain_ledger_v1:find_routing(OUI1, Ledger)),

    Block0 = test_utils:create_block(ConsensusMembers, [SignedTxn1]),
    _ = blockchain_gossip_handler:add_block(Swarm, Block0, Chain, self()),

    ok = test_utils:wait_until(fun() -> {ok, 2} == blockchain:height(Chain) end),

    Routing0 = blockchain_ledger_routing_v1:new(OUI1, Payer, Addresses0, 0),
    ?assertEqual({ok, Routing0}, blockchain_ledger_v1:find_routing(OUI1, Ledger)),

    %% create the routing txn
    Addresses1 = [<<"/p2p/random">>],
    Txn2 = blockchain_txn_routing_v1:new(OUI1, Payer, Addresses1, 0, 1),
    SignedTxn2 = blockchain_txn_routing_v1:sign(Txn2, SigFun),

    %% before we add the block verify it validates
    ?assertEqual(ok, blockchain_txn_routing_v1:is_valid(SignedTxn2, Chain)),
    %% confirm the txn has not yet been absorbed
    ?assertEqual(false, blockchain_txn_routing_v1:absorbed(Txn2, Chain)),

    Block1 = test_utils:create_block(ConsensusMembers, [SignedTxn2]),
    _ = blockchain_gossip_handler:add_block(Swarm, Block1, Chain, self()),

    ok = test_utils:wait_until(fun() -> {ok, 3} == blockchain:height(Chain) end),

    %% then check absorbed again on same block, it should return true
    ?assertEqual(true, blockchain_txn_routing_v1:absorbed(Txn2, Chain)),
    %% but it should continue to pass validation
    ?assertEqual(ok, blockchain_txn_routing_v1:is_valid(SignedTxn2, Chain)),

    meck:unload(blockchain_txn_oui_v1),
    ok.


%%--------------------------------------------------------------------
%% @public
%% @doc
%% @end
%%--------------------------------------------------------------------
absorbed_txn_security_exchange_v1_test(Config) ->
    BaseDir = proplists:get_value(basedir, Config),
    ConsensusMembers = proplists:get_value(consensus_members, Config),
    BaseDir = proplists:get_value(basedir, Config),
    Chain = proplists:get_value(chain, Config),
    Swarm = proplists:get_value(swarm, Config),

    %% create the security exchange txn
    [_, {Payer, {_, PayerPrivKey, _}}|_] = ConsensusMembers,
    Recipient = blockchain_swarm:pubkey_bin(),
    Txn1 = blockchain_txn_security_exchange_v1:new(Payer, Recipient, 2500, 0, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    SignedTxn1 = blockchain_txn_security_exchange_v1:sign(Txn1, SigFun),

    %% before we add the block verify it validates
    ?assertEqual(ok, blockchain_txn_security_exchange_v1:is_valid(SignedTxn1, Chain)),
    %% confirm the txn has not yet been absorbed
    ?assertEqual(false, blockchain_txn_security_exchange_v1:absorbed(Txn1, Chain)),

    Block = test_utils:create_block(ConsensusMembers, [SignedTxn1]),
    _ = blockchain_gossip_handler:add_block(Swarm, Block, Chain, self()),

    ok = blockchain_ct_utils:wait_until(fun() -> {ok, 2} =:= blockchain:height(Chain) end),

    %% then check absorbed again on same block, it should return true
    ?assertEqual(true, blockchain_txn_security_exchange_v1:absorbed(Txn1, Chain)),
    %% but it should continue to pass validation
    ?assertEqual(ok, blockchain_txn_security_exchange_v1:is_valid(SignedTxn1, Chain)).


%%--------------------------------------------------------------------
%% @public
%% @doc
%% @end
%%--------------------------------------------------------------------
absorbed_txn_token_burn_v1_test(Config) ->
    BaseDir = proplists:get_value(basedir, Config),
    ConsensusMembers = proplists:get_value(consensus_members, Config),
    BaseDir = proplists:get_value(basedir, Config),
    Chain = proplists:get_value(chain, Config),
    Swarm = proplists:get_value(swarm, Config),

    [_, {Payer, {_, PayerPrivKey, _}} |_] = ConsensusMembers,
    PayerSigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),

    % need to first add exchange rate to ledger
    Rate = 1000000,
    {Priv, _} = proplists:get_value(master_key, Config),
    Vars = #{token_burn_exchange_rate => Rate},

    Txn1 = blockchain_txn_vars_v1:new(Vars, 3),
    Proof = blockchain_txn_vars_v1:create_proof(Priv, Txn1),
    ProofedTxn1 = blockchain_txn_vars_v1:proof(Txn1, Proof),
    Block2 = test_utils:create_block(ConsensusMembers, [ProofedTxn1]),
    _ = blockchain_gossip_handler:add_block(Swarm, Block2, Chain, self()),

    ok = blockchain_ct_utils:wait_until(fun() -> {ok, 2} =:= blockchain:height(Chain) end),

    _Blocks = lists:map(
               fun(_) ->
                       Block = test_utils:create_block(ConsensusMembers, []),
                       _ = blockchain_gossip_handler:add_block(Swarm, Block, Chain, self()),
                       Block
               end,
               lists:seq(1, 20)),

    ok = blockchain_ct_utils:wait_until(fun() -> {ok, 22} =:= blockchain:height(Chain) end),

    %% create the token burn txn
    Txn2 = blockchain_txn_token_burn_v1:new(Payer, 10, 1),
    SignedTxn2 = blockchain_txn_token_burn_v1:sign(Txn2, PayerSigFun),

    %% before we add the block verify it validates
    ?assertEqual(ok, blockchain_txn_token_burn_v1:is_valid(SignedTxn2, Chain)),
    %% confirm the txn has not yet been absorbed
    ?assertEqual(false, blockchain_txn_token_burn_v1:absorbed(Txn2, Chain)),

    Block23 = test_utils:create_block(ConsensusMembers, [SignedTxn2]),
    _ = blockchain_gossip_handler:add_block(Swarm, Block23, Chain, self()),

    ok = blockchain_ct_utils:wait_until(fun() -> {ok, 23} =:= blockchain:height(Chain) end),

    %% then check absorbed again on same block, it should return true
    ?assertEqual(true, blockchain_txn_token_burn_v1:absorbed(Txn2, Chain)),
    %% but it should continue to pass validation
    ?assertEqual(ok, blockchain_txn_token_burn_v1:is_valid(SignedTxn2, Chain)).


%%--------------------------------------------------------------------
%% @public
%% @doc
%% @end
%%--------------------------------------------------------------------
absorbed_txn_vars_v1_test(Config) ->
    BaseDir = proplists:get_value(basedir, Config),
    ConsensusMembers = proplists:get_value(consensus_members, Config),
    BaseDir = proplists:get_value(basedir, Config),
    Chain = proplists:get_value(chain, Config),
    Swarm = proplists:get_value(swarm, Config),
    {Priv, _} = proplists:get_value(master_key, Config),

    %% create the vars exchange txn
    Vars = #{poc_version => 2},
    Txn1 = blockchain_txn_vars_v1:new(Vars, 3),
    Proof = blockchain_txn_vars_v1:create_proof(Priv, Txn1),
    ProofedTxn1 = blockchain_txn_vars_v1:proof(Txn1, Proof),

    %% before we add the block verify it validates
    ?assertEqual(ok, blockchain_txn_vars_v1:is_valid(ProofedTxn1, Chain)),
    %% confirm the txn has not yet been absorbed
    ?assertEqual(false, blockchain_txn_vars_v1:absorbed(Txn1, Chain)),

    InitBlock = test_utils:create_block(ConsensusMembers, [ProofedTxn1]),
    _ = blockchain_gossip_handler:add_block(Swarm, InitBlock, Chain, self()),

    ok = blockchain_ct_utils:wait_until(fun() -> {ok, 2} =:= blockchain:height(Chain) end),

    %% then check absorbed again on same block, it should return true
    ?assertEqual(true, blockchain_txn_vars_v1:absorbed(Txn1, Chain)),
    %% but it should continue to pass validation
    ?assertEqual(ok, blockchain_txn_vars_v1:is_valid(ProofedTxn1, Chain)).



%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

p_create_and_test_gateway(Config, Gateway, GatewaySigFun, OwnerSigFun, Rate)->
    BaseDir = proplists:get_value(basedir, Config),
    ConsensusMembers = proplists:get_value(consensus_members, Config),
    GenesisMembers = proplists:get_value(genesis_members, Config),
    BaseDir = proplists:get_value(basedir, Config),
    Chain = proplists:get_value(chain, Config),
    Swarm = proplists:get_value(swarm, Config),

    PubKey = proplists:get_value(pubkey, Config),
    _PrivKey = proplists:get_value(privkey, Config),
    Owner = libp2p_crypto:pubkey_to_bin(PubKey),
    Balance = proplists:get_value(balance, Config),

    Ledger = blockchain:ledger(Chain),

    lists:foreach(
        fun(_) ->
                Block = test_utils:create_block(ConsensusMembers, []),
                _ = blockchain_gossip_handler:add_block(Swarm, Block, Chain, self())
        end,
        lists:seq(1, 20)
    ),
    ?assertEqual({ok, Rate}, blockchain_ledger_v1:config(?token_burn_exchange_rate, Ledger)),


    BurnTx0 = blockchain_txn_token_burn_v1:new(Owner, 10, 1),
    SignedBurnTx0 = blockchain_txn_token_burn_v1:sign(BurnTx0, OwnerSigFun),

    %% before we add the block verify it validates
    ?assertEqual(ok, blockchain_txn_token_burn_v1:is_valid(SignedBurnTx0, Chain)),

    %% confirm the txn has not yet been absorbed
    ?assertEqual(false, blockchain_txn_token_burn_v1:absorbed(BurnTx0, Chain)),

    Block23 = test_utils:create_block(ConsensusMembers, [SignedBurnTx0]),
    _ = blockchain_gossip_handler:add_block(Swarm, Block23, Chain, self()),

    ok = blockchain_ct_utils:wait_until(fun() -> {ok, 23} =:= blockchain:height(Chain) end),

    {ok, NewEntry0} = blockchain_ledger_v1:find_entry(Owner, Ledger),
    ?assertEqual(Balance - 10, blockchain_ledger_entry_v1:balance(NewEntry0)),

    {ok, DCEntry0} = blockchain_ledger_v1:find_dc_entry(Owner, Ledger),
    ?assertEqual(10*Rate, blockchain_ledger_data_credits_entry_v1:balance(DCEntry0)),

    %% Create the gateway

    %% a GW will have been generated as part of the test init
    %% so to test absorbed/2 for the gen_gateway txn, we will just create a new txn
    %% with a used nonce and confirm it returns true to indicate already absorbed
    {Addr, _} = hd(GenesisMembers),
    InitialGatewayTxn = blockchain_txn_gen_gateway_v1:new(Addr, Addr, ?TEST_LOCATION, 0),
    %% confirm the txn HAS been absorbed
    ?assertEqual(true, blockchain_txn_gen_gateway_v1:absorbed(InitialGatewayTxn, Chain)),
    %% and as we are not in genesis block validation will fail
    ?assertEqual({error,not_in_genesis_block}, blockchain_txn_gen_gateway_v1:is_valid(InitialGatewayTxn, Chain)),


    Tx24 = blockchain_txn_add_gateway_v1:new(Owner, Gateway, 1, 1),
    SignedTx24 = blockchain_txn_add_gateway_v1:sign(Tx24, OwnerSigFun),
    SignedTx24Request = blockchain_txn_add_gateway_v1:sign_request(SignedTx24, GatewaySigFun),

    %% before we add the block verify it validates
    ?assertEqual(ok, blockchain_txn_add_gateway_v1:is_valid(SignedTx24Request, Chain)),
    %% confirm the txn has not yet been absorbed
    ?assertEqual(false, blockchain_txn_add_gateway_v1:absorbed(Tx24, Chain)),

    %% creat the block with the txn
    Block24 = test_utils:create_block(ConsensusMembers, [SignedTx24Request]),
    _ = blockchain_gossip_handler:add_block(Swarm, Block24, Chain, self()),

    %% confirm the head block and the heights
    ok = blockchain_ct_utils:wait_until(fun() -> {ok, 24} =:= blockchain:height(Chain) end),

    %% then check absorbed again on same block, it should return true
    ?assertEqual(true, blockchain_txn_add_gateway_v1:absorbed(Tx24, Chain)),
    %% but it should continue to pass validation
    ?assertEqual(ok, blockchain_txn_add_gateway_v1:is_valid(SignedTx24Request, Chain)),

    % Check that the Gateway is there
    {ok, GwInfo} = blockchain_ledger_v1:find_gateway_info(Gateway, blockchain:ledger(Chain)),
    ?assertEqual(Owner, blockchain_ledger_gateway_v2:owner_address(GwInfo)),

    % Assert the Gateways location
    Tx25 = blockchain_txn_assert_location_v1:new(Gateway, Owner, ?TEST_LOCATION, 1, 1, 1),
    Tx25SignedRequest = blockchain_txn_assert_location_v1:sign_request(Tx25, GatewaySigFun),
    Tx25Signed = blockchain_txn_assert_location_v1:sign(Tx25SignedRequest, OwnerSigFun),

    %% before we add the block verify it validates
    ?assertEqual(ok, blockchain_txn_assert_location_v1:is_valid(Tx25Signed, Chain)),
    %% confirm the txn has not yet been absorbed
    ?assertEqual(false, blockchain_txn_assert_location_v1:absorbed(Tx25, Chain)),

    Block25 = test_utils:create_block(ConsensusMembers, [Tx25Signed]),
    ok = blockchain_gossip_handler:add_block(Swarm, Block25, Chain, self()),
    timer:sleep(500),

    ok = blockchain_ct_utils:wait_until(fun() -> {ok, 25} =:= blockchain:height(Chain) end),

    %% then check absorbed again on same block, it should return true
    ?assertEqual(true, blockchain_txn_assert_location_v1:absorbed(Tx25, Chain)),
    %% but it should continue to pass validation
    ?assertEqual(ok, blockchain_txn_assert_location_v1:is_valid(Tx25Signed, Chain)).