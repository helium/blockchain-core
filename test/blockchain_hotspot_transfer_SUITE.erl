-module(blockchain_hotspot_transfer_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("blockchain_vars.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).

-export([
         basic_validity_test/1,
         bad_seller_signature_test/1,
         bad_buyer_signature_test/1,
         buyer_has_enough_hnt_test/1,
         gateway_not_owned_by_seller_test/1,
         gateway_stale_test/1,
         replay_test/1
        ]).

all() ->
    [
     basic_validity_test,
     bad_seller_signature_test,
     bad_buyer_signature_test,
     buyer_has_enough_hnt_test,
     gateway_not_owned_by_seller_test,
     gateway_stale_test,
     replay_test
    ].

%%--------------------------------------------------------------------
%% TEST CASE SETUP
%%--------------------------------------------------------------------

init_per_testcase(TestCase, Config) ->
    Config0 = blockchain_ct_utils:init_base_dir_config(?MODULE, TestCase, Config),
    Balance = 5000,
    {ok, Sup, {PrivKey, PubKey}, Opts} = test_utils:init(?config(base_dir, Config0)),

    ExtraVars = case TestCase of
                    gateway_stale_test -> #{ }; %% default is 0
                    _ -> #{ ?transfer_hotspot_stale_poc_blocks => 10 }
                end,

    {ok, GenesisMembers, _GenesisBlock, ConsensusMembers, Keys} =
        test_utils:init_chain(Balance, {PrivKey, PubKey}, true, ExtraVars),

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

    %% In the test, SellerPubkeyBin = GatewayPubkeyBin

    %% Get some owner and their gateway
    [{SellerPubkeyBin, Gateway},
     {BuyerPubkeyBin, _} | _] = maps:to_list(blockchain_ledger_v1:active_gateways(Ledger)),

    %% Get seller privkey and sigfun
    {_SellerPubkey, SellerPrivKey, _} = proplists:get_value(SellerPubkeyBin, GenesisMembers),
    SellerSigFun = libp2p_crypto:mk_sig_fun(SellerPrivKey),

    %% Get buyer privkey and sigfun
    {_BuyerPubkey, BuyerPrivKey, _} = proplists:get_value(BuyerPubkeyBin, GenesisMembers),
    BuyerSigFun = libp2p_crypto:mk_sig_fun(BuyerPrivKey),

    ct:pal("Seller: ~p", [SellerPubkeyBin]),
    ct:pal("Gateway: ~p", [Gateway]),
    ct:pal("BuyerPubkeyBin: ~p", [BuyerPubkeyBin]),

    Txn = blockchain_txn_transfer_hotspot_v1:new(SellerPubkeyBin, SellerPubkeyBin, BuyerPubkeyBin, 1),
    SellerSignedTxn = blockchain_txn_transfer_hotspot_v1:sign_seller(Txn, SellerSigFun),
    BuyerSignedTxn = blockchain_txn_transfer_hotspot_v1:sign_buyer(SellerSignedTxn, BuyerSigFun),
    ct:pal("SignedTxn: ~p", [BuyerSignedTxn]),
    ct:pal("IsValidSeller: ~p", [blockchain_txn_transfer_hotspot_v1:is_valid_seller(BuyerSignedTxn)]),

    ok = blockchain_txn:is_valid(BuyerSignedTxn, Chain),

    ok.

bad_seller_signature_test(Config) ->
    GenesisMembers = ?config(genesis_members, Config),
    Chain = ?config(chain, Config),
    Ledger = blockchain:ledger(Chain),

    %% In the test, SellerPubkeyBin = GatewayPubkeyBin

    %% Get some owner and their gateway
    [{SellerPubkeyBin, Gateway},
     {BuyerPubkeyBin, _} | _] = maps:to_list(blockchain_ledger_v1:active_gateways(Ledger)),

    %% Get seller privkey and sigfun
    {_SellerPubkey, SellerPrivKey, _} = proplists:get_value(SellerPubkeyBin, GenesisMembers),
    _SellerSigFun = libp2p_crypto:mk_sig_fun(SellerPrivKey),

    %% Get buyer privkey and sigfun
    {_BuyerPubkey, BuyerPrivKey, _} = proplists:get_value(BuyerPubkeyBin, GenesisMembers),
    BuyerSigFun = libp2p_crypto:mk_sig_fun(BuyerPrivKey),

    ct:pal("Seller: ~p", [SellerPubkeyBin]),
    ct:pal("Gateway: ~p", [Gateway]),
    ct:pal("BuyerPubkeyBin: ~p", [BuyerPubkeyBin]),

    Txn = blockchain_txn_transfer_hotspot_v1:new(SellerPubkeyBin, SellerPubkeyBin, BuyerPubkeyBin, 1),

    %% this is not seller's signature
    SellerSignedTxn = blockchain_txn_transfer_hotspot_v1:sign_seller(Txn, BuyerSigFun),

    BuyerSignedTxn = blockchain_txn_transfer_hotspot_v1:sign_buyer(SellerSignedTxn, BuyerSigFun),
    ct:pal("SignedTxn: ~p", [BuyerSignedTxn]),
    ct:pal("IsValidSeller: ~p", [blockchain_txn_transfer_hotspot_v1:is_valid_seller(BuyerSignedTxn)]),

    {error, bad_seller_signature} = blockchain_txn:is_valid(BuyerSignedTxn, Chain),

    ok.

bad_buyer_signature_test(Config) ->
    GenesisMembers = ?config(genesis_members, Config),
    Chain = ?config(chain, Config),
    Ledger = blockchain:ledger(Chain),

    %% In the test, SellerPubkeyBin = GatewayPubkeyBin

    %% Get some owner and their gateway
    [{SellerPubkeyBin, Gateway},
     {BuyerPubkeyBin, _} | _] = maps:to_list(blockchain_ledger_v1:active_gateways(Ledger)),

    %% Get seller privkey and sigfun
    {_SellerPubkey, SellerPrivKey, _} = proplists:get_value(SellerPubkeyBin, GenesisMembers),
    SellerSigFun = libp2p_crypto:mk_sig_fun(SellerPrivKey),

    %% Get buyer privkey and sigfun
    {_BuyerPubkey, BuyerPrivKey, _} = proplists:get_value(BuyerPubkeyBin, GenesisMembers),
    _BuyerSigFun = libp2p_crypto:mk_sig_fun(BuyerPrivKey),

    ct:pal("Seller: ~p", [SellerPubkeyBin]),
    ct:pal("Gateway: ~p", [Gateway]),
    ct:pal("BuyerPubkeyBin: ~p", [BuyerPubkeyBin]),

    Txn = blockchain_txn_transfer_hotspot_v1:new(SellerPubkeyBin, SellerPubkeyBin, BuyerPubkeyBin, 1),
    SellerSignedTxn = blockchain_txn_transfer_hotspot_v1:sign_seller(Txn, SellerSigFun),

    %% this is not buyer's signature
    BuyerSignedTxn = blockchain_txn_transfer_hotspot_v1:sign_buyer(SellerSignedTxn, SellerSigFun),

    ct:pal("SignedTxn: ~p", [BuyerSignedTxn]),
    ct:pal("IsValidSeller: ~p", [blockchain_txn_transfer_hotspot_v1:is_valid_seller(BuyerSignedTxn)]),

    {error, bad_buyer_signature} = blockchain_txn:is_valid(BuyerSignedTxn, Chain),

    ok.

buyer_has_enough_hnt_test(Config) ->
    GenesisMembers = ?config(genesis_members, Config),
    Chain = ?config(chain, Config),
    Ledger = blockchain:ledger(Chain),

    %% In the test, SellerPubkeyBin = GatewayPubkeyBin

    %% Get some owner and their gateway
    [{SellerPubkeyBin, Gateway},
     {BuyerPubkeyBin, _} | _] = maps:to_list(blockchain_ledger_v1:active_gateways(Ledger)),

    %% Get seller privkey and sigfun
    {_SellerPubkey, SellerPrivKey, _} = proplists:get_value(SellerPubkeyBin, GenesisMembers),
    SellerSigFun = libp2p_crypto:mk_sig_fun(SellerPrivKey),

    %% Get buyer privkey and sigfun
    {_BuyerPubkey, BuyerPrivKey, _} = proplists:get_value(BuyerPubkeyBin, GenesisMembers),
    BuyerSigFun = libp2p_crypto:mk_sig_fun(BuyerPrivKey),

    ct:pal("Seller: ~p", [SellerPubkeyBin]),
    ct:pal("Gateway: ~p", [Gateway]),
    ct:pal("BuyerPubkeyBin: ~p", [BuyerPubkeyBin]),

    Txn = blockchain_txn_transfer_hotspot_v1:new(SellerPubkeyBin,
                                                 SellerPubkeyBin,
                                                 BuyerPubkeyBin,
                                                 1,
                                                 10000),    %% buyer only has 5K hnt to boot
    SellerSignedTxn = blockchain_txn_transfer_hotspot_v1:sign_seller(Txn, SellerSigFun),
    BuyerSignedTxn = blockchain_txn_transfer_hotspot_v1:sign_buyer(SellerSignedTxn, BuyerSigFun),
    ct:pal("SignedTxn: ~p", [BuyerSignedTxn]),
    ct:pal("IsValidSeller: ~p", [blockchain_txn_transfer_hotspot_v1:is_valid_seller(BuyerSignedTxn)]),

    {error, buyer_insufficient_hnt_balance} = blockchain_txn:is_valid(BuyerSignedTxn, Chain),

    ok.

gateway_not_owned_by_seller_test(Config) ->
    GenesisMembers = ?config(genesis_members, Config),
    Chain = ?config(chain, Config),
    Ledger = blockchain:ledger(Chain),

    %% In the test, SellerPubkeyBin = GatewayPubkeyBin

    %% Get some owner and their gateway
    [{SellerPubkeyBin, Gateway},
     {BuyerPubkeyBin, _} | _] = maps:to_list(blockchain_ledger_v1:active_gateways(Ledger)),

    %% Get seller privkey and sigfun
    {_SellerPubkey, SellerPrivKey, _} = proplists:get_value(SellerPubkeyBin, GenesisMembers),
    SellerSigFun = libp2p_crypto:mk_sig_fun(SellerPrivKey),

    %% Get buyer privkey and sigfun
    {_BuyerPubkey, BuyerPrivKey, _} = proplists:get_value(BuyerPubkeyBin, GenesisMembers),
    BuyerSigFun = libp2p_crypto:mk_sig_fun(BuyerPrivKey),

    ct:pal("Seller: ~p", [SellerPubkeyBin]),
    ct:pal("Gateway: ~p", [Gateway]),
    ct:pal("BuyerPubkeyBin: ~p", [BuyerPubkeyBin]),

    Txn = blockchain_txn_transfer_hotspot_v1:new(BuyerPubkeyBin,    %% this is not the seller's gw
                                                 SellerPubkeyBin,
                                                 BuyerPubkeyBin,
                                                 1),
    SellerSignedTxn = blockchain_txn_transfer_hotspot_v1:sign_seller(Txn, SellerSigFun),
    BuyerSignedTxn = blockchain_txn_transfer_hotspot_v1:sign_buyer(SellerSignedTxn, BuyerSigFun),
    ct:pal("SignedTxn: ~p", [BuyerSignedTxn]),
    ct:pal("IsValidSeller: ~p", [blockchain_txn_transfer_hotspot_v1:is_valid_seller(BuyerSignedTxn)]),

    {error, gateway_not_owned_by_seller} = blockchain_txn:is_valid(BuyerSignedTxn, Chain),

    ok.

gateway_stale_test(Config) ->
    GenesisMembers = ?config(genesis_members, Config),
    Chain = ?config(chain, Config),
    Ledger = blockchain:ledger(Chain),

    %% Get some owner and their gateway
    [{SellerPubkeyBin, Gateway},
     {BuyerPubkeyBin, _} | _] = maps:to_list(blockchain_ledger_v1:active_gateways(Ledger)),

    %% Get seller privkey and sigfun
    {_SellerPubkey, SellerPrivKey, _} = proplists:get_value(SellerPubkeyBin, GenesisMembers),
    SellerSigFun = libp2p_crypto:mk_sig_fun(SellerPrivKey),

    %% Get buyer privkey and sigfun
    {_BuyerPubkey, BuyerPrivKey, _} = proplists:get_value(BuyerPubkeyBin, GenesisMembers),
    BuyerSigFun = libp2p_crypto:mk_sig_fun(BuyerPrivKey),

    ct:pal("Seller: ~p", [SellerPubkeyBin]),
    ct:pal("Gateway: ~p", [Gateway]),
    ct:pal("BuyerPubkeyBin: ~p", [BuyerPubkeyBin]),

    Txn = blockchain_txn_transfer_hotspot_v1:new(SellerPubkeyBin,
                                                 SellerPubkeyBin,
                                                 BuyerPubkeyBin,
                                                 1),
    SellerSignedTxn = blockchain_txn_transfer_hotspot_v1:sign_seller(Txn, SellerSigFun),
    BuyerSignedTxn = blockchain_txn_transfer_hotspot_v1:sign_buyer(SellerSignedTxn, BuyerSigFun),
    ct:pal("SignedTxn: ~p", [BuyerSignedTxn]),

    {error, gateway_too_stale} = blockchain_txn:is_valid(BuyerSignedTxn, Chain),

    ok.

replay_test(Config) ->
    GenesisMembers = ?config(genesis_members, Config),
    ConsensusMembers = ?config(consensus_members, Config),
    Chain = ?config(chain, Config),
    Ledger = blockchain:ledger(Chain),

    %% In the test, SellerPubkeyBin = GatewayPubkeyBin

    %% Get some owner and their gateway
    [{SellerPubkeyBin, Gateway},
     {BuyerPubkeyBin, _} | _] = maps:to_list(blockchain_ledger_v1:active_gateways(Ledger)),

    %% Get seller privkey and sigfun
    {_SellerPubkey, SellerPrivKey, _} = proplists:get_value(SellerPubkeyBin, GenesisMembers),
    SellerSigFun = libp2p_crypto:mk_sig_fun(SellerPrivKey),

    %% Get buyer privkey and sigfun
    {_BuyerPubkey, BuyerPrivKey, _} = proplists:get_value(BuyerPubkeyBin, GenesisMembers),
    BuyerSigFun = libp2p_crypto:mk_sig_fun(BuyerPrivKey),

    ct:pal("Seller: ~p", [SellerPubkeyBin]),
    ct:pal("Gateway: ~p", [Gateway]),
    ct:pal("BuyerPubkeyBin: ~p", [BuyerPubkeyBin]),

    %% This is valid, nonce=1
    Txn = blockchain_txn_transfer_hotspot_v1:new(SellerPubkeyBin,
                                                 SellerPubkeyBin,
                                                 BuyerPubkeyBin,
                                                 1),
    SellerSignedTxn = blockchain_txn_transfer_hotspot_v1:sign_seller(Txn, SellerSigFun),
    BuyerSignedTxn = blockchain_txn_transfer_hotspot_v1:sign_buyer(SellerSignedTxn, BuyerSigFun),
    ct:pal("SignedTxn: ~p", [BuyerSignedTxn]),

    ok = blockchain_txn:is_valid(BuyerSignedTxn, Chain),

    %% Put the valid txn in a block and gossip
    {ok, Block2} = test_utils:create_block(ConsensusMembers, [BuyerSignedTxn]),
    _ = blockchain_gossip_handler:add_block(Block2, Chain, self(), blockchain_swarm:tid()),
    ?assertEqual({ok, blockchain_block:hash_block(Block2)}, blockchain:head_hash(Chain)),
    ?assertEqual({ok, Block2}, blockchain:head_block(Chain)),
    ?assertEqual({ok, 2}, blockchain:height(Chain)),

    %% Now the same txn should not work
    {error, {invalid_txns, _}} = test_utils:create_block(ConsensusMembers, [BuyerSignedTxn]),

    %% Back to seller
    BackToSellerTxn = blockchain_txn_transfer_hotspot_v1:new(SellerPubkeyBin,
                                                             BuyerPubkeyBin,
                                                             SellerPubkeyBin,
                                                             1),
    SellerSignedBackToSellerTxn = blockchain_txn_transfer_hotspot_v1:sign_seller(BackToSellerTxn, BuyerSigFun),
    BuyerSignedBackToSellerTxn = blockchain_txn_transfer_hotspot_v1:sign_buyer(SellerSignedBackToSellerTxn, SellerSigFun),
    ct:pal("SignedTxn2: ~p", [BuyerSignedBackToSellerTxn]),

    ok = blockchain_txn:is_valid(BuyerSignedBackToSellerTxn, Chain),

    %% Put the buyback txn in a block
    {ok, Block3} = test_utils:create_block(ConsensusMembers, [BuyerSignedBackToSellerTxn]),
    _ = blockchain_gossip_handler:add_block(Block3, Chain, self(), blockchain_swarm:tid()),
    ?assertEqual({ok, blockchain_block:hash_block(Block3)}, blockchain:head_hash(Chain)),
    ?assertEqual({ok, Block3}, blockchain:head_block(Chain)),
    ?assertEqual({ok, 3}, blockchain:height(Chain)),

    %% The first txn should be now invalid
    {error, _} = blockchain_txn:is_valid(Txn, Chain),
    %% Also check it does not appear in a block
    {error, {invalid_txns, _}} = test_utils:create_block(ConsensusMembers, [BuyerSignedTxn]),
    ok.

