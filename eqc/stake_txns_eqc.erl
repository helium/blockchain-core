-module(stake_txns_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").

-include_lib("stdlib/include/assert.hrl").

-include_lib("blockchain/include/blockchain_vars.hrl").
-include_lib("blockchain/include/blockchain.hrl").

-compile([export_all, nowarn_export_all]).

-behaviour(eqc_statem).

%% -- State and state functions ----------------------------------------------

%-record

-record(s,
        {
         init = false,
         chain,
         pending_validators = [],
         validators = [],
         unstaked_validators = [],
         group,
         accounts,

         height = 1,
         pending_txns = #{},
         prepending_unstake = #{},
         pending_unstake = #{},
         txn_ctr = 1
        }).

-define(call(Fun, ArgList),
        {call, ?M, Fun, ArgList}).

-record(validator,
        {
         owner,
         addr,
         status,
         stake
        }).

-record(account,
        {
         id,
         address,
         balance,
         sig_fun,
         pub,
         priv
        }).

-define(M, ?MODULE).

%% @doc Returns the state in which each test case starts. (Unless a different
%%      initial state is supplied explicitly to, e.g. commands/2.)
-spec initial_state() -> eqc_statem:symbolic_state().
initial_state() ->
    #s{
       accounts = maps:from_list(
                    lists:zip(lists:seq(1, 5),
                              lists:duplicate(5, ?bones(10000))))
      }.

init_chain_env() ->
    %% these should be idempotent
    _ = application:ensure_all_started(lager),
    _ = blockchain_lock:start_link(),
    application:set_env(blockchain, test_mode, true),

    %% create a chain
    BaseDir = make_base_dir(),

    %%% local is just a follower of the fake chain
    %% LocalKeys = libp2p_crypto:generate_keys(ecc_compact),

    %% SigFun = libp2p_crypto:mk_sig_fun(maps:get(secret, LocalKeys)),
    %% ECDHFun = libp2p_crypto:mk_ecdh_fun(maps:get(secret, LocalKeys)),

    %% Opts = [
    %%     {key, {maps:get(public, LocalKeys), SigFun, ECDHFun}},
    %%     {seed_nodes, []},
    %%     {port, 0},
    %%     {num_consensus_members, 4},
    %%     {base_dir, BaseDir}
    %% ],

    {no_genesis, Chain} = blockchain:new(BaseDir, "", undefined, undefined),

    {InitialVars, _MasterKeys} = blockchain_ct_utils:create_vars(val_vars()),

    GenesisMembers = test_utils:generate_keys(4),

    Balance = ?bones(10000),
    Accounts = [#account{id = ID,
                         address = Addr,
                         %% give all accounts 1 val stake
                         balance = Balance,
                         sig_fun = SigFun,
                         pub = Pub,
                         priv = Priv}
                || {ID, {Addr, {Pub, Priv, SigFun}}} <- lists:zip(lists:seq(1, 5),
                                                                  test_utils:generate_keys(5))],

    GenOwner = hd(Accounts),

    GenPaymentTxs = [blockchain_txn_coinbase_v1:new(Addr, Balance)
                     || #account{address = Addr} <- Accounts],

    %% GenSecPaymentTxs = [blockchain_txn_security_coinbase_v1:new(Addr, Balance)
    %%                  || {Addr, _} <- GenesisMembers],

    Addresses = [Addr || {Addr, _} <- GenesisMembers],

    InitialConsensusTxn =
        [blockchain_txn_gen_validator_v1:new(Addr, GenOwner#account.address, ?bones(10000))
         || Addr <- Addresses],

    GenConsensusGroupTx = blockchain_txn_consensus_group_v1:new(
                            [Addr || {Addr, _} <- GenesisMembers], <<"proof">>, 1, 0),
    Txs = InitialVars ++
        GenPaymentTxs ++
        %% GenSecPaymentTxs ++
        InitialConsensusTxn ++
        [GenConsensusGroupTx],
    %% lager:info("initial transactions: ~p", [Txs]),

    GenesisBlock = blockchain_block:new_genesis_block(Txs),
    ok = blockchain:integrate_genesis(GenesisBlock, Chain),

    ?assertEqual({ok, 1}, blockchain:height(Chain)),

    [{chain, Chain},
%%       keys = LocalKeys,
     {base_dir, BaseDir},
%%     master_keys = MasterKeys,
     {accounts, maps:from_list([{ID, Acct} || #account{id = ID} = Acct <- Accounts])},
     {validators, GenesisMembers},
     {group, GenesisMembers}].

make_base_dir() ->
    "stake-txns-dir-"++integer_to_list(rand:uniform(8999999) + 1000000).

val_vars() ->
    #{
      ?election_version => 5,
      ?validator_version => 2,
      ?validator_minimum_stake => ?bones(10000),
      ?validator_liveness_grace_period => 10,
      ?validator_liveness_interval => 5,
      ?stake_withdrawl_cooldown => 5,
      ?stake_withdrawal_max => 500,
      ?dkg_penalty => 1.0,
      ?penalty_history_limit => 100,
      ?election_interval => 5
     }.

%% -- Generators -------------------------------------------------------------

weight(_S, init) ->
    1;
weight(_S, stake) ->
    4;
weight(_S, unstake) ->
    4;
weight(_S, block) ->
    4.

command_precondition_common(S, Cmd) ->
    S#s.init /= false orelse Cmd == init.

invariant(#s{chain = undefined}) ->
    true;
invariant(#s{chain = Chain,
             validators = Vals,
             pending_unstake = Pends
            }) ->
    Ledger = blockchain:ledger(Chain),
    Circ = blockchain_ledger_v1:query_circulating_hnt(Ledger),
    Cool = blockchain_ledger_v1:query_cooldown_hnt(Ledger),
    Staked = blockchain_ledger_v1:query_staked_hnt(Ledger),

    Stake = ?bones(10000),

    NumPends = length(lists:flatten(maps:values(Pends))),
    NumVals = length(Vals),
    lager:debug("circ ~p cool ~p staked ~p vals ~p pend ~p",
                [Circ, Cool, Staked, NumVals, NumPends]),

    try
        ExpCool = NumPends * Stake,
        case ExpCool == Cool of
            false -> throw({cool, ExpCool, Cool});
            _ -> ok
        end,
        ExpStaked = (4 + NumVals) * Stake,
        case ExpStaked == Staked of
            false -> throw({staked, ExpStaked, Cool});
            _ -> ok
        end,
        ExpCirc = (5 - NumVals - NumPends) * Stake,
        case ExpCirc == Circ of
            false -> throw({circ, ExpCirc, Circ});
            _ -> ok
        end,
        true
    catch throw:E ->
            E
    end.

add_block(Chain, {ok, _Valid, _Invalid, Block}) ->
    %% doing this for the side-effects, not sure if it's right :/
    ok = blockchain:add_block(Block, Chain),
    Chain.

%% generalize?
add_pending({ok, _Addr, Txn}, ID, Pending, Reason) ->
    Pending#{ID => {Reason, Txn}}.

update_pending({ok, Valid, Invalid0, _Block}, Pending) ->
    {Invalid, _Reasons} = lists:unzip(Invalid0),
    ToRemove = Valid ++ Invalid,
    maps:filter(
      fun(_ID, {_tag, Txn}) ->
              not lists:member(Txn, ToRemove)
      end, Pending).

update_accounts(stake, SymAccts, valid,
                {ok, #validator{owner = Owner,
                                stake = Stake}, _}) ->
    OAcctBal = maps:get(Owner, SymAccts),
    SymAccts#{Owner => OAcctBal - Stake};
update_accounts(stake, SymAccts, _, _) ->
    SymAccts.

%% -- Commands ---------------------------------------------------------------
init_pre(S, _) ->
    S#s.init == false.

init_args(_S) ->
    [{var, chain}].

init(Chain) ->
    Chain.

init_next(S, R, _) ->
    S#s{init = true,
        chain = R}.

%% stake command
stake_dynamicpre(#s{unstaked_validators = Dead0}, [_, _, _, bad_validator]) ->
    Dead = lists:flatten(Dead0),
    Dead /= [];
stake_dynamicpre(_S, [Accounts, _Dead, _DynAccts, balance]) ->
    maps:size(
      maps:filter(
        fun(_, Bal) ->
                Bal < ?bones(10000)
        end,
        Accounts)) =/= 0;
%% we need at least one possible staker for these others to be reasonable
stake_dynamicpre(_S, [Accounts, _Dead, _DynAccts, _]) ->
    maps:size(
      maps:filter(
        fun(_, Bal) ->
                Bal >= ?bones(10000)
        end,
        Accounts)) =/= 0.

stake_args(S) ->
    oneof([[S#s.accounts, S#s.unstaked_validators, {var, accounts}, valid],
           [S#s.accounts, S#s.unstaked_validators, {var, accounts}, balance],
           [S#s.accounts, S#s.unstaked_validators, {var, accounts}, bad_sig],
           [S#s.accounts, S#s.unstaked_validators, {var, accounts}, bad_validator],
           [S#s.accounts, S#s.unstaked_validators, {var, accounts}, bad_owner]]).

stake(SymAccts, Dead, Accounts, Reason) ->
    %% todo rich accounts vs poor accounts
    Filter =
        case Reason of
            balance ->
                fun(_, Bal) ->
                        Bal < ?bones(10000)
                end;
            _ ->
                fun(_, Bal) ->
                        Bal >= ?bones(10000)
                end
        end,
    {Val, Addr, Account} =
        case Reason of
            bad_validator ->
                V = select(Dead),
                {V, V#validator.addr, V#validator.owner};
              _ ->
                Acct = select(maps:keys(maps:filter(Filter, SymAccts))),
                [{Address, _}] = test_utils:generate_keys(1),
                {#validator{owner = Acct, addr = Address, stake = ?bones(10000)},
                 Address,
                 Acct}
        end,
    lager:info("val ~p acct ~p reason ~p", [Val, Account, Reason]),
    Txn = stake_txn(maps:get(Account, Accounts), Addr, Reason),
    {ok, Val, Txn}.

stake_txn(#account{address = Account0,
                   sig_fun = SigFun}, Val, Reason) ->
    Account =
        case Reason of
            bad_owner ->
                [{Acct, _}] = test_utils:generate_keys(1),
                Acct;
            _ -> Account0
        end,
    Txn = blockchain_txn_stake_validator_v1:new(
            Val, Account,
            ?bones(10000),
            35000
           ),
    STxn = blockchain_txn_stake_validator_v1:sign(Txn, SigFun),
    case Reason of
        bad_sig ->
            blockchain_txn_stake_validator_v1:owner_signature(<<0:512>>, Txn);
        _ ->
            STxn
    end.
%% todo: try with mainnet/testnet keys
stake_next(#s{} = S,
           V,
           [SymAccounts, _Dead, _Accounts, Reason]) ->
    S#s{accounts = ?call(update_accounts, [stake, SymAccounts, Reason, V]),
        pending_txns = ?call(add_pending, [V, S#s.txn_ctr, S#s.pending_txns, Reason]),
        pending_validators = ?call(update_validators, [S#s.pending_validators, Reason, V]),
        txn_ctr = S#s.txn_ctr + 1}.

update_validators(Validators, Reason, {ok, Val, _Txn}) ->
    case Reason of
        valid -> Validators ++ [Val];
        _ -> Validators
    end.

%% unstake command
unstake_dynamicpre(#s{unstaked_validators = Dead0}, [_, _, _, _, _, bad_validator]) ->
    Dead = lists:flatten(Dead0),
    Dead /= [];
unstake_dynamicpre(#s{prepending_unstake = Unstaked,
                      validators = Validators}, _Args) ->
    %% lager:info("dynamic pre ~p ~p", [Validators, maps:values(Unstaked)]),
    (Validators -- lists:flatten(maps:values(Unstaked))) /= [].

unstake_args(S) ->
    oneof([
           [S#s.height, {var, accounts}, S#s.prepending_unstake, S#s.validators, S#s.unstaked_validators, valid],
           [S#s.height, {var, accounts}, S#s.prepending_unstake, S#s.validators, S#s.unstaked_validators, bad_account],
           [S#s.height, {var, accounts}, S#s.prepending_unstake, S#s.validators, S#s.unstaked_validators, bad_sig],
           [S#s.height, {var, accounts}, S#s.prepending_unstake, S#s.validators, S#s.unstaked_validators, wrong_account],
           [S#s.height, {var, accounts}, S#s.prepending_unstake, S#s.validators, S#s.unstaked_validators, bad_validator]
          ]).

unstake(Height, Accounts, Unstaked, SymVals, Dead, Reason) ->
    Val = case Reason of
              bad_validator -> select(Dead);
              _ -> select(SymVals -- lists:flatten(maps:values(Unstaked)))
          end,
    lager:info("unstake ~p reas ~p ded ~p ok ~p",
               [Val, Reason, Dead,
                SymVals -- lists:flatten(maps:values(Unstaked))]),
    Txn = unstake_txn(Val, Accounts, Height, Reason),
    {ok, Val, Txn}.

unstake_next(#s{} = S,
             V,
             [_Height, _Accounts, _Vals, _, _, Reason]) ->
    S#s{prepending_unstake = ?call(update_preunstake, [S#s.prepending_unstake, Reason, V]),
        pending_txns = ?call(add_pending, [V, S#s.txn_ctr, S#s.pending_txns, Reason]),
        txn_ctr = S#s.txn_ctr + 1}.

update_preunstake(Pending, valid, {ok, Val, Txn}) ->
    UnstakeHeight = blockchain_txn_unstake_validator_v1:stake_release_height(Txn),
    maps:update_with(UnstakeHeight, fun(X) -> [Val | X] end, [Val], Pending);
update_preunstake(Pending, _Reason, _Res) ->
    Pending.

unstake_txn(#validator{owner = Owner, addr = Addr}, Accounts, Height, Reason) ->
    Account =
        case Reason of
            %% make up a non-existent account
            bad_account ->
                [{Acct, {_, _, Sig}}] = test_utils:generate_keys(1),
                #account{address = Acct, sig_fun = Sig};
            %% use existing but non-owner account
            wrong_account ->
                element(2, hd(maps:to_list(maps:remove(Owner, Accounts))));
            _ ->
                maps:get(Owner, Accounts)
        end,

    Txn = blockchain_txn_unstake_validator_v1:new(
            Addr, Account#account.address,
            ?bones(10000),
            Height + 5 + 1,
            35000
           ),
    STxn = blockchain_txn_unstake_validator_v1:sign(Txn, Account#account.sig_fun),
    case Reason of
        bad_sig ->
            blockchain_txn_unstake_validator_v1:owner_signature(<<0:512>>, Txn);
        _ ->
            STxn
    end.

%% block commands
block_args(S) ->
    [{var, chain}, {var, group}, S#s.pending_txns].

block(Chain, Group, Txns) ->
    STxns = lists:sort(fun blockchain_txn:sort/2, element(2, lists:unzip(maps:values(Txns)))),
    {Valid, Invalid} = blockchain_txn:validate(STxns, Chain),
    {ok, HeadBlock} = blockchain:head_block(Chain),
    {ok, PrevHash} = blockchain:head_hash(Chain),
    Height = blockchain_block:height(HeadBlock) + 1,
    Time = blockchain_block:time(HeadBlock) + 1,
    MBlock =
        #{prev_hash => PrevHash,
          height => Height,
          transactions => Valid,
          signatures => [],
          time => Time,
          hbbft_round => 0,
          election_epoch => 1,
          epoch_start => 0,
          seen_votes => [],
          bba_completion => <<>>
         },
    Block0 = blockchain_block_v1:new(MBlock),
    BinBlock = blockchain_block:serialize(Block0),
    Signatures = signatures(Group, BinBlock),
    Block1 = blockchain_block:set_signatures(Block0, Signatures),
    %% lager:info("txns ~p", [Block1]),
    {ok, Valid, Invalid, Block1}.

block_next(#s{} = S,
           V,
           [Chain, _Group, _Transactions]) ->
    NewHeight = S#s.height + 1,
    S#s{chain = ?call(add_block, [Chain, V]),
        height = NewHeight,

        accounts = ?call(block_update_accounts, [NewHeight, S#s.accounts, S#s.pending_unstake]),

        pending_validators = [],
        validators = ?call(block_update_validators, [S#s.pending_validators, S#s.prepending_unstake, S#s.validators]),

        prepending_unstake = #{},
        pending_unstake = ?call(update_unstake, [NewHeight, S#s.prepending_unstake, S#s.pending_unstake]),
        unstaked_validators = S#s.unstaked_validators ++
            lists:flatten([ ?call(update_dead_validators, [S#s.prepending_unstake])]),

        pending_txns = ?call(update_pending, [V, S#s.pending_txns])}.

block_update_validators(PV, Unstakes, V) ->
    (V ++ PV) -- lists:flatten(maps:values(Unstakes)).

update_dead_validators(Unstakes) ->
    lists:flatten(maps:values(Unstakes)).

update_unstake(Height, PP, P) ->
    maps:remove(Height, maps:merge(PP, P)).

block_update_accounts(Height, Accounts, PendingUnstake) ->
    case maps:find(Height, PendingUnstake) of
        {ok, Vals} ->
            lists:foldl(
              fun(Val, Acc) ->
                      Bal = maps:get(Val#validator.owner, Acc),
                      Acc#{Val#validator.owner => Bal + ?bones(10000)}
              end, Accounts,
              Vals);
        _ -> Accounts
    end.

block_post(#s{pending_txns = Pend,
              validators = _vals,
              accounts = _Accounts} = _S,
              _Args,
              {ok, Valid, Invalid0, _Block}) ->
    {Invalid, _Reasons} = lists:unzip(Invalid0),
    Ret =
        maps:fold(
          fun(_ID, {valid, Txn}, Acc) ->
                  %% we either need to be in the valid txns, or not in the invalid txns, i.e. not in the
                  %% list at all
                  case lists:member(Txn, Valid) orelse
                      not lists:member(Txn, Invalid) of
                      false ->
                          [{valid, Txn}];
                      _ -> Acc
                  end;
             %% all non-'valid' reason tags are invalid
             (__ID, {Reason, Txn}, Acc) ->
                  %% we either need to be in the valid txns, or not in the invalid txns, i.e. not in the
                  %% list at all
                  case lists:member(Txn, Invalid) orelse
                      not lists:member(Txn, Valid) of
                      false ->
                          [{Reason, Txn}];
                      _ -> Acc
                  end
          end,
          [],
          Pend),
    case Ret of
        [] -> true;
        _ -> Ret
    end.

%% -- Property ---------------------------------------------------------------
prop_stake() ->
    ?FORALL(
       %% default to longer commands sequences for better coverage
       %% Cmds, more_commands(5, commands(?M)),
       Cmds, noshrink(more_commands(5, commands(?M))),
       with_parameters(
         [{show_states, false},  % make true to print state at each transition
          {print_counterexample, true}],
         aggregate(command_names(Cmds),
                   begin
                       Env = init_chain_env(),
                       {H, S, Res} = run_commands(Cmds, Env),
                       eqc_statem:pretty_commands(?M,
                                                  Cmds,
                                                  {H, S, Res},
                                                  Env,
                                                  cleanup(eqc_symbolic:eval(S), Env)
                                                  andalso Res == ok)
                   end))).

%% @doc Run property repeatedly to find as many different bugs as
%% possible. Runs for 10 seconds before giving up finding more bugs.
-spec bugs() -> [eqc_statem:bug()].
bugs() -> bugs(10).

%% @doc Run property repeatedly to find as many different bugs as
%% possible. Runs for N seconds before giving up finding more bugs.
-spec bugs(non_neg_integer()) -> [eqc_statem:bug()].
bugs(N) -> bugs(N, []).

%% @doc Run property repeatedly to find as many different bugs as
%% possible. Takes testing time and already found bugs as arguments.
-spec bugs(non_neg_integer(), [eqc_statem:bug()]) -> [eqc_statem:bug()].
bugs(Time, Bugs) ->
    more_bugs(eqc:testing_time(Time, prop_stake()), 20, Bugs).

%%% helpers

select([]) ->
    error(zero_len_list);
select(Lst0) ->
    Lst = lists:flatten(Lst0),
    Len = length(Lst),
    lists:nth(rand:uniform(Len), Lst).

signatures(Members, Bin) ->
    lists:foldl(
      fun({A, {_, _, F}}, Acc) ->
              Sig = F(Bin),
              [{A, Sig}|Acc]
      end, [], Members).

cleanup(#s{}, Env) ->
    Dir = maps:get(base_dir, maps:from_list(Env)),
    lager:info("entering cleanup"),
    PWD = string:trim(os:cmd("pwd")),
    os:cmd("rm -r " ++ PWD ++ "/" ++ Dir),
    true.
