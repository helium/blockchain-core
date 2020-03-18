-module(test_utils).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("blockchain_vars.hrl").

-export([
    init/1,
    init_chain/2, init_chain/3, init_chain/4,
    generate_keys/1, generate_keys/2,
    wait_until/1, wait_until/3,
    create_block/2,
    tmp_dir/0, tmp_dir/1,
    cleanup_tmp_dir/1,
    nonl/1,
    create_payment_transaction/6,
    atomic_save/2
]).

-define(BASE_TMP_DIR, "./_build/test/tmp").
-define(BASE_TMP_DIR_TEMPLATE, "XXXXXXXXXX").

init(BaseDir) ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    ECDHFun = libp2p_crypto:mk_ecdh_fun(PrivKey),
    Opts = [
        {key, {PubKey, SigFun, ECDHFun}},
        {seed_nodes, []},
        {port, 0},
        {num_consensus_members, 7},
        {base_dir, BaseDir}
    ],
    {ok, Sup} = blockchain_sup:start_link(Opts),
    ?assert(erlang:is_pid(blockchain_swarm:swarm())),
    {ok, Sup, {PrivKey, PubKey}, Opts}.

init_chain(Balance, Keys) ->
    init_chain(Balance, Keys, true, #{}).

init_chain(Balance, Keys, InConsensus) ->
    init_chain(Balance, Keys, InConsensus, #{}).

init_chain(Balance, {PrivKey, PubKey}, InConsensus, ExtraVars) ->
    % Generate fake blockchains (just the keys)
    GenesisMembers = case InConsensus of
                         true ->
                             RandomKeys = test_utils:generate_keys(10),
                             Address = blockchain_swarm:pubkey_bin(),
                             [
                              {Address, {PubKey, PrivKey, libp2p_crypto:mk_sig_fun(PrivKey)}}
                             ] ++ RandomKeys;
                         false ->
                             test_utils:generate_keys(11)
                     end,

    % Create genesis block
    {InitialVars, Keys} = blockchain_ct_utils:create_vars(ExtraVars),

    GenPaymentTxs = [blockchain_txn_coinbase_v1:new(Addr, Balance)
                     || {Addr, _} <- GenesisMembers],

    GenSecPaymentTxs = [blockchain_txn_security_coinbase_v1:new(Addr, Balance)
                     || {Addr, _} <- GenesisMembers],

    GenDCsTxns = [blockchain_txn_dc_coinbase_v1:new(Addr, Balance) || {Addr, _} <- GenesisMembers],

    Addresses = [Addr || {Addr, _} <- GenesisMembers],

    Locations = lists:foldl(
        fun(I, Acc) ->
            [h3:from_geo({37.780586, -122.469470 + I/100}, 12)|Acc]
        end,
        [],
        lists:seq(1, length(Addresses))
    ),
    InitialGatewayTxn = [blockchain_txn_gen_gateway_v1:new(Addr, Addr, Loc, 0)
                         || {Addr, Loc} <- lists:zip(Addresses, Locations)],

    ConsensusMembers = lists:sublist(GenesisMembers, 7),
    GenConsensusGroupTx = blockchain_txn_consensus_group_v1:new(
                            [Addr || {Addr, _} <- ConsensusMembers], <<"proof">>, 1, 0),
    Txs = InitialVars ++
        GenPaymentTxs ++
        GenSecPaymentTxs ++
        InitialGatewayTxn ++
        GenDCsTxns ++
        [GenConsensusGroupTx],
    lager:info("initial transactions: ~p", [Txs]),

    GenesisBlock = blockchain_block:new_genesis_block(Txs),
    ok = blockchain_worker:integrate_genesis_block(GenesisBlock),

    Chain = blockchain_worker:blockchain(),
    {ok, HeadBlock} = blockchain:head_block(Chain),
    ?assertEqual(blockchain_block:hash_block(GenesisBlock), blockchain_block:hash_block(HeadBlock)),
    ?assertEqual({ok, GenesisBlock}, blockchain:head_block(Chain)),
    ?assertEqual({ok, blockchain_block:hash_block(GenesisBlock)}, blockchain:genesis_hash(Chain)),
    ?assertEqual({ok, GenesisBlock}, blockchain:genesis_block(Chain)),
    ?assertEqual({ok, 1}, blockchain:height(Chain)),
    {ok, GenesisMembers, ConsensusMembers, Keys}.

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
                                     transactions => lists:sort(fun blockchain_txn:sort/2, Txs),
                                     signatures => [],
                                     time => Time,
                                     hbbft_round => 0,
                                     election_epoch => 1,
                                     epoch_start => 0
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
%%--------------------------------------------------------------------
%% @doc
%% generate a tmp directory to be used as a scratch by eunit tests
%% @end
%%-------------------------------------------------------------------
tmp_dir() ->
    os:cmd("mkdir -p " ++ ?BASE_TMP_DIR),
    create_tmp_dir(?BASE_TMP_DIR_TEMPLATE).
tmp_dir(SubDir) ->
    Path = filename:join(?BASE_TMP_DIR, SubDir),
    os:cmd("mkdir -p " ++ Path),
    create_tmp_dir(Path ++ "/" ++ ?BASE_TMP_DIR_TEMPLATE).

%%--------------------------------------------------------------------
%% @doc
%% Deletes the specified directory
%% @end
%%-------------------------------------------------------------------
-spec cleanup_tmp_dir(list()) -> ok.
cleanup_tmp_dir(Dir)->
    os:cmd("rm -rf " ++ Dir),
    ok.


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

-spec create_tmp_dir(list()) -> list().
create_tmp_dir(Path)->
    ?MODULE:nonl(os:cmd("mktemp -d " ++  Path)).

