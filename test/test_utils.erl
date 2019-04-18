-module(test_utils).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
    init/1, init_chain/2,
    generate_keys/1, generate_keys/2,
    wait_until/1, wait_until/3,
    create_block/2,
    tmp_dir/0, tmp_dir/1,
    nonl/1,
    create_payment_transaction/6,
    atomic_save/2
]).

init(BaseDir) ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Opts = [
        {key, {PubKey, SigFun}},
        {seed_nodes, []},
        {port, 0},
        {num_consensus_members, 7},
        {base_dir, BaseDir}
    ],
    {ok, Sup} = blockchain_sup:start_link(Opts),
    ?assert(erlang:is_pid(blockchain_swarm:swarm())),
    {ok, Sup, {PrivKey, PubKey}, Opts}.

init_chain(Balance, {PrivKey, PubKey}) ->
    % Generate fake blockchains (just the keys)
    RandomKeys = test_utils:generate_keys(10),
    Address = blockchain_swarm:pubkey_bin(),
    ConsensusMembers = [
        {Address, {PubKey, PrivKey, libp2p_crypto:mk_sig_fun(PrivKey)}}
    ] ++ RandomKeys,

    % Create genesis block
    GenPaymentTxs = [blockchain_txn_coinbase_v1:new(Addr, Balance)
                     || {Addr, _} <- ConsensusMembers],
    GenSecPaymentTxs = [blockchain_txn_security_coinbase_v1:new(Addr, Balance)
                     || {Addr, _} <- ConsensusMembers],

    GenConsensusGroupTx = blockchain_txn_consensus_group_v1:new([Addr || {Addr, _} <- ConsensusMembers]),
    Txs = GenPaymentTxs ++ GenSecPaymentTxs ++ [GenConsensusGroupTx],
    GenesisBlock = blockchain_block:new_genesis_block(Txs),
    ok = blockchain_worker:integrate_genesis_block(GenesisBlock),

    Chain = blockchain_worker:blockchain(),
    {ok, HeadBlock} = blockchain:head_block(Chain),
    ?assertEqual(blockchain_block:hash_block(GenesisBlock), blockchain_block:hash_block(HeadBlock)),
    ?assertEqual({ok, GenesisBlock}, blockchain:head_block(Chain)),
    ?assertEqual({ok, blockchain_block:hash_block(GenesisBlock)}, blockchain:genesis_hash(Chain)),
    ?assertEqual({ok, GenesisBlock}, blockchain:genesis_block(Chain)),
    ?assertEqual({ok, 1}, blockchain:height(Chain)),
    {ok, ConsensusMembers}.

generate_keys(N) ->
    generate_keys(N, ecc_compact).

generate_keys(N, Type) ->
    lists:foldl(
        fun(_, Acc) ->
            #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(Type),
            SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
            [{libp2p_crypto:pubkey_to_bin(PubKey), {PubKey, PrivKey, SigFun}}|Acc]
        end
        ,[]
        ,lists:seq(1, N)
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

create_block(ConsensusMembers, Txs) ->
    Blockchain = blockchain_worker:blockchain(),
    {ok, PrevHash} = blockchain:head_hash(Blockchain),
    {ok, HeadBlock} = blockchain:head_block(Blockchain),
    Height = blockchain_block:height(HeadBlock) + 1,
    Time = blockchain_block:time(HeadBlock) + 1,
    Block0 = blockchain_block_v1:new(#{prev_hash => PrevHash,
                                     height => Height,
                                     transactions => Txs,
                                     signatures => [],
                                     time => Time,
                                     hbbft_round => 0
                                     }),
    BinBlock = blockchain_block:serialize(Block0),
    Signatures = signatures(ConsensusMembers, BinBlock),
    Block1 = blockchain_block:set_signatures(Block0, Signatures),
    Block1.

signatures(ConsensusMembers, BinBlock) ->
    lists:foldl(
      fun({A, {_, _, F}}, Acc) ->
              Sig = F(BinBlock),
              [{A, Sig}|Acc];
         %% NOTE: This clause matches the consensus members generated for the dist suite
         ({A, _, F}, Acc) ->
              Sig = F(BinBlock),
              [{A, Sig}|Acc]
      end
      ,[]
      ,ConsensusMembers
     ).

tmp_dir() ->
    ?MODULE:nonl(os:cmd("mktemp -d")).

tmp_dir(Dir) ->
    filename:join(tmp_dir(), Dir).

nonl([$\n|T]) -> nonl(T);
nonl([H|T]) -> [H|nonl(T)];
nonl([]) -> [].

create_payment_transaction(Payer, PayerPrivKey, Amount, Fee, Nonce, Recipient) ->
    Tx = blockchain_txn_payment_v1:new(Payer, Recipient, Amount, Fee, Nonce),
    SigFun = libp2p_crypto:mk_sig_fun(PayerPrivKey),
    blockchain_txn_payment_v1:sign(Tx, SigFun).


%%--------------------------------------------------------------------
%% @doc
%% @end
%%-------------------------------------------------------------------
-spec atomic_save(file:filename_all(), binary() | string()) -> ok | {error, any()}.
atomic_save(File, Bin) ->
    ok = filelib:ensure_dir(File),
    TmpFile = File ++ "-tmp",
    ok = file:write_file(TmpFile, Bin),
    file:rename(TmpFile, File).
