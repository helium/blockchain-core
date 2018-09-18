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
    BaseDir = "data",
    Opts = [
        {key, {PubKey, SigFun}}
        ,{seed_nodes, []}
        ,{port, 0}
        ,{num_consensus_members, 7}
        ,{base_dir, BaseDir}
    ],
    Balance = 5000,

    {ok, Sup} = blockchain_sup:start_link(Opts),
    ?assert(erlang:is_pid(blockchain_swarm:swarm())),

    % Generate fake blockchains (just the keys)
    RandomKeys = generate_keys(10),
    Address = blockchain_swarm:address(),
    ConsensusMembers = [
        {Address, {PubKey, PrivKey, SigFun}}
    ] ++ RandomKeys,

    % Create genesis block
    GenPaymentTxs = [blockchain_transaction:new_coinbase_txn(Addr, Balance)
                     || {Addr, _} <- ConsensusMembers],
    GenConsensusGroupTx = blockchain_transaction:new_genesis_consensus_group([Addr || {Addr, _} <- ConsensusMembers]),
    Txs = GenPaymentTxs ++ [GenConsensusGroupTx],
    GenesisBlock = blockchain_block:new_genesis_block(Txs),
    ok = blockchain_worker:integrate_genesis_block(GenesisBlock),

    ?assertEqual(blockchain_block:hash_block(GenesisBlock), blockchain_worker:head_hash()),
    ?assertEqual(GenesisBlock, blockchain_worker:head_block()),
    ?assertEqual(blockchain_block:hash_block(GenesisBlock), blockchain_worker:genesis_hash()),
    ?assertEqual(GenesisBlock, blockchain_worker:genesis_block()),
    ?assertEqual(1, blockchain_worker:height()),

    % Check ledger to make sure everyone has the right balance
    Ledger = blockchain_worker:ledger(),
    Entries = [blockchain_ledger:find_entry(Addr, Ledger) || {Addr, _} <- ConsensusMembers],
    _ = [{?assertEqual(Balance, blockchain_ledger:balance(Entry))
          ,?assertEqual(0, blockchain_ledger:payment_nonce(Entry))}
         || Entry <- Entries],

    % Test a payment transaction, add a block and check balances
    [{Payer, {_, PayerPrivKey, _}}|_] = RandomKeys,
    Recipient = Address,
    Tx = blockchain_transaction:new_payment_txn(Payer, Recipient, 2500, 1),
    SignedTx = blockchain_transaction:sign_payment_txn(Tx, PayerPrivKey),
    Block = add_block(ConsensusMembers, [SignedTx]),

    ?assertEqual(blockchain_block:hash_block(Block), blockchain_worker:head_hash()),
    ?assertEqual(Block, blockchain_worker:head_block()),
    ?assertEqual(2, blockchain_worker:height()),

    NewEntry0 = blockchain_ledger:find_entry(Recipient, blockchain_worker:ledger()),
    ?assertEqual(Balance + 2500, blockchain_ledger:balance(NewEntry0)),

    NewEntry1 = blockchain_ledger:find_entry(Payer, blockchain_worker:ledger()),
    ?assertEqual(Balance - 2500, blockchain_ledger:balance(NewEntry1)),

    % Make sure blockchain saved on file =  in memory
    Chain = blockchain_worker:blockchain(),
    ok = compare_chains(Chain, blockchain:load(BaseDir)),

    % Restart blockchain and make sure nothing has changed
    true = erlang:exit(Sup, normal),
    ok = wait_until(fun() -> false =:= erlang:is_process_alive(Sup) end),

    {ok, _Sup1} = blockchain_sup:start_link(Opts),
    ?assert(erlang:is_pid(blockchain_swarm:swarm())),

    ok = compare_chains(Chain, blockchain_worker:blockchain()),
    ok.

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

add_block(ConsensusMembers, Txs) ->
    PrevHash = blockchain_worker:head_hash(),
    Height = blockchain_worker:height() + 1,
    Block0 = blockchain_block:new(PrevHash, Height, Txs, <<>>, #{}),
    BinBlock = erlang:term_to_binary(blockchain_block:remove_signature(Block0)),
    Signatures = signatures(ConsensusMembers, BinBlock),
    Block1 = blockchain_block:sign_block(Block0, erlang:term_to_binary(Signatures)),
    ok = blockchain_worker:add_block(Block1, self()),
    Block1.

signatures(ConsensusMembers, BinBlock) ->
    lists:foldl(
        fun({A, {_, _, F}}, Acc) ->
            Sig = F(BinBlock),
            [{A, Sig}|Acc]
        end
        ,[]
        ,ConsensusMembers
    ).

wait_until(Fun) ->
    wait_until(Fun, 40, 100).

wait_until(Fun, Retry, Delay) when Retry > 0 ->
    Res = Fun(),
    case Res of
        true ->
            ok;
        _ when Retry == 1 ->
            {fail, Res};
        _ ->
            timer:sleep(Delay),
            wait_until(Fun, Retry-1, Delay)
    end.

compare_chains(Expected, Got) ->
    ?assertEqual(blockchain:dir(Expected), blockchain:dir(Got)),
    ?assertEqual(blockchain:head_hash(Expected), blockchain:head_hash(Got)),
    ?assertEqual(blockchain:head_block(Expected), blockchain:head_block(Got)),
    ?assertEqual(blockchain:genesis_hash(Expected), blockchain:genesis_hash(Got)),
    ?assertEqual(blockchain:genesis_block(Expected), blockchain:genesis_block(Got)),
    ?assertEqual(blockchain:ledger(Expected), blockchain:ledger(Got)),
    ok.
