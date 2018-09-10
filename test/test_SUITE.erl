-module(test_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
    all/0
]).

-export([
    basic/1
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
    [basic].

%%--------------------------------------------------------------------
%% TEST CASES
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @public
%% @doc
%% @end
%%--------------------------------------------------------------------
basic(_Config) ->
    {PrivKey, PubKey} = libp2p_crypto:generate_keys(),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Opts = [
        {key, {PubKey, SigFun}}
        ,{seed_nodes, []}
        ,{port, 0}
    ],
    Balance = 5000,

    {ok, _Sup} = blockchain_sup:start_link(Opts),
    ?assert(erlang:is_pid(blockchain_swarm:swarm())),

    RandomKeys = generate_keys(3),
    Address = blockchain_swarm:address(),
    ConsensusMembers = [
        {Address, {PubKey, PrivKey, SigFun}}
    ] ++ RandomKeys,

    GenPaymentTxs = [blockchain_transaction:new_coinbase_txn(libp2p_crypto:address_to_b58(Addr), Balance)
                         || {Addr, _} <- ConsensusMembers],
    GenConsensusGroupTx = blockchain_transaction:new_genesis_consensus_group([Addr || {Addr, _} <- ConsensusMembers]),
    Txs = GenPaymentTxs ++ [GenConsensusGroupTx],
    GenesisBlock = blockchain_block:new_genesis_block(Txs),
    ok = blockchain_worker:integrate_genesis_block(GenesisBlock),

    Ledger = blockchain_worker:ledger(),
    Entries = [blockchain_ledger:find_entry(Addr, Ledger) || {Addr, _} <- ConsensusMembers],
    _ = [{?assertEqual(Balance, blockchain_ledger:balance(Entry))
          ,?assertEqual(0, blockchain_ledger:nonce(Entry))}
         || Entry <- Entries],

    ok.

% NOTE: We should be able to mock another blockchain node just with libp2p
% frame stream stuff so that I can play with blocks

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

generate_keys(N) ->
    lists:foldl(
        fun(_, Acc) ->
            {PrivKey, PubKey} = libp2p_crypto:generate_keys(),
            SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
            [{libp2p_crypto:pubkey_to_address(PubKey), {PubKey, PrivKey, SigFun}}|Acc]
        end
        ,[]
        ,lists:seq(1, N)
    ).
