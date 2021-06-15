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
         account_addrs,

         height = 1,
         pending_txns = #{},
         prepending_unstake = #{},
         pretransfer = [],
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
         stake,
         sig_fun
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
-define(num_accounts, 5).
-define(initial_validators, 4).
-define(min_stake, ?bones(10000)).
-define(initial_balance, ?min_stake * 2).

-define(val, blockchain_ledger_validator_v1).

%% @doc Returns the state in which each test case starts. (Unless a different
%%      initial state is supplied explicitly to, e.g. commands/2.)
-spec initial_state() -> eqc_statem:symbolic_state().
initial_state() ->
    #s{
       accounts = maps:from_list(
                    lists:zip(lists:seq(1, ?num_accounts),
                              lists:duplicate(?num_accounts, {?initial_balance,
                                                              ?initial_balance})))
      }.

init_chain_env() ->
    %% these should be idempotent
    _ = application:ensure_all_started(lager),
    _ = blockchain_lock:start_link(),
    application:set_env(blockchain, test_mode, true),

    %% create a chain
    BaseDir = make_base_dir(),

    {no_genesis, Chain} = blockchain:new(BaseDir, "", undefined, undefined),

    {InitialVars, _MasterKeys} = blockchain_ct_utils:create_vars(val_vars()),

    GenesisMembers =
        [#validator{owner = 0,
                    addr = Addr,
                    sig_fun = SigFun,
                    stake = ?min_stake}
         || {Addr, {_Pub, _Priv, SigFun}} <- test_utils:generate_keys(?initial_validators)],

    Balance = ?initial_balance,
    [GenOwner |
     Accounts] = [#account{id = ID,
                           address = Addr,
                           %% give all accounts 1 val stake
                           balance = Balance,
                           sig_fun = SigFun,
                           pub = Pub,
                           priv = Priv}
                  || {ID, {Addr, {Pub, Priv, SigFun}}} <- lists:zip(lists:seq(0, ?num_accounts),
                                                                    test_utils:generate_keys(?num_accounts + 1))],

    GenPaymentTxs = [blockchain_txn_coinbase_v1:new(Addr, Balance)
                     || #account{address = Addr} <- Accounts],

    Addresses = [Addr || #validator{addr = Addr} <- GenesisMembers],

    InitialConsensusTxn =
        [blockchain_txn_gen_validator_v1:new(Addr, GenOwner#account.address, ?min_stake)
         || Addr <- Addresses],

    GenConsensusGroupTx = blockchain_txn_consensus_group_v1:new(
                            [Addr || #validator{addr = Addr} <- GenesisMembers], <<"proof">>, 1, 0),
    Txs = InitialVars ++
        GenPaymentTxs ++
        InitialConsensusTxn ++
        [GenConsensusGroupTx],

    GenesisBlock = blockchain_block:new_genesis_block(Txs),
    ok = blockchain:integrate_genesis(GenesisBlock, Chain),

    ?assertEqual({ok, 1}, blockchain:height(Chain)),

    [{chain, Chain},
     {base_dir, BaseDir},
     {accounts, maps:from_list([{ID, Acct} || #account{id = ID} = Acct <- Accounts])},
     {validators, GenesisMembers},
     {group, GenesisMembers}].

make_base_dir() ->
    "stake-txns-dir-"++integer_to_list(rand:uniform(8999999) + 1000000).

val_vars() ->
    #{
      ?election_version => 5,
      ?validator_version => 3,
      ?validator_minimum_stake => ?min_stake,
      ?validator_liveness_grace_period => 100,
      ?validator_liveness_interval => 2000,
      ?stake_withdrawal_cooldown => 5,
      ?stake_withdrawal_max => 500,
      %?tenure_penalty => 1.0,
      ?dkg_penalty => 1.0,
      ?num_consensus_members => 4,
      ?election_bba_penalty => 0.5,
      ?election_seen_penalty => 0.5,
      ?tenure_penalty => 0.5,
      ?validator_penalty_filter => 1.0,
      ?penalty_history_limit => 100,
      ?election_interval => 3
     }.

%% -- Generators -------------------------------------------------------------

weight(_S, init) ->
    1;
weight(_S, transfer) ->
    50;
weight(_S, stake) ->
    30;
weight(_S, unstake) ->
    30;
weight(_S, election) ->
    100;
weight(_S, block) ->
    100.

command_precondition_common(S, Cmd) ->
    S#s.init /= false orelse Cmd == init.

invariant(#s{chain = undefined}) ->
    true;
invariant(#s{chain = Chain,
             validators = Vals,
             pending_unstake = Pends,
             accounts = Accounts,
             account_addrs = Addrs
            }) ->
    Ledger = blockchain:ledger(Chain),
    Circ = blockchain_ledger_v1:query_circulating_hnt(Ledger),
    Cool = blockchain_ledger_v1:query_cooldown_hnt(Ledger),
    Staked = blockchain_ledger_v1:query_staked_hnt(Ledger),
    LedgerVals0 = blockchain_ledger_v1:snapshot_validators(Ledger),
    LedgerVals =
        lists:map(fun({Addr, BinVal}) ->
                          {Addr, blockchain_ledger_validator_v1:deserialize(BinVal)}
                  end, LedgerVals0),

    Stake = ?min_stake,

    NumPends = length(lists:flatten(maps:values(Pends))),
    NumVals = maps:size(Vals),
    lager:debug("circ ~p cool ~p staked ~p vals ~p pend ~p",
                [Circ, Cool, Staked, NumVals, NumPends]),

    try
        %% check that the ledger amounts match the expected state from the model
        ExpCool = NumPends * Stake,
        case ExpCool == Cool of
            false -> throw({cool, ExpCool, Cool, Pends});
            _ -> ok
        end,
        ExpStaked = NumVals * Stake,
        case ExpStaked == Staked of
            false -> throw({staked, ExpStaked, Staked, Vals});
            _ -> ok
        end,
        ExpCirc = (?num_accounts * 2 - (NumVals - ?initial_validators) - NumPends) * Stake,
        case ExpCirc == Circ of
            false -> throw({circ, ExpCirc, Circ});
            _ -> ok
        end,

        %% make sure that all balances are correct
        ExpBals0 =
            lists:foldl(
              fun({_Addr, V}, Acc) ->
                      StakeAmt = ?val:stake(V),
                      Owner = ?val:owner_address(V),
                      %% we have 4 initial validators not owned by modeled accounts who we don't
                      %% care about from a balance perspective
                      case maps:find(Owner, Acc) of
                          {ok, {Bal, Tot}} ->
                              case ?val:status(V) of
                                  staked ->
                                      Acc#{Owner => {Bal, Tot + StakeAmt}};
                                  unstaked ->
                                      Acc;
                                  cooldown ->
                                      Acc#{Owner => {Bal, Tot + StakeAmt}}
                              end;
                          _ -> Acc
                      end
              end,
              maps:from_list([begin
                                  {ok, Ent} = blockchain_ledger_v1:find_entry(Addr, Ledger),
                                  Bal = blockchain_ledger_entry_v1:balance(Ent),
                                  {Addr, {Bal, Bal}}
                              end || Addr <- maps:keys(Addrs)]),
              LedgerVals),
        ExpBals =
            maps:fold(fun(Addr, BalTot, Acc) ->
                              case maps:find(Addr, Addrs) of
                                  {ok, ID} ->
                                      Acc#{ID => BalTot};
                                  _ -> Acc
                              end
                      end, #{}, ExpBals0),
        case ExpBals == Accounts of
            true ->
                ok;
            false ->
                throw({balance_issue, ExpBals, Accounts})
        end,
        true
    catch throw:E ->
            E
    end.

%% doing this for the side-effects, not sure if it's right :/
add_block(Chain, {ok, _Valid, _Invalid, Block}) ->
    ok = blockchain:add_block(Block, Chain),
    Chain;
add_block(Chain, {_NewGroup, Block}) ->
    ok = blockchain:add_block(Block, Chain),
    Chain.

%% generalize?
add_pending({_ok, _Addr, Txn}, ID, Pending, Reason) ->
    Pending#{ID => {Reason, Txn}}.

update_pending({ok, Valid, Invalid0, _Block}, Pending) ->
    {Invalid, _Reasons} = lists:unzip(Invalid0),
    ToRemove = Valid ++ Invalid,
    maps:filter(
      fun(_ID, {_tag, Txn}) ->
              not lists:member(Txn, ToRemove)
      end, Pending).

%% -- Commands ---------------------------------------------------------------
init_pre(S, _) ->
    S#s.init == false.

init_args(_S) ->
    [{var, chain}, {var, accounts}, {var, group}].

init(Chain, Accounts, Group) ->
    {Chain, Accounts, Group}.

init_next(S, R, _) ->
    S#s{init = true,
        chain = {call, erlang, element, [1, R]},
        group = ?call(init_group, [R]),
        validators = ?call(init_vals, [R]),
        account_addrs = ?call(init_accts, [R])}.

init_accts({_Chain, Accounts, _Group}) ->
    maps:fold(fun(ID, #account{address = Addr}, Acc) ->
                      Acc#{Addr => ID}
              end, #{}, Accounts).

init_group({_Chain, _Accounts, Group}) ->
    [Addr || #validator{addr = Addr} <- Group].

init_vals({_Chain, _Accounts, Group}) ->
    maps:from_list(
      [{Addr, V} || V = #validator{addr = Addr} <- Group]).

lt_stake(_, {Bones, _}) ->
    Bones < ?min_stake.

ge_stake(_, {Bones, _}) ->
    Bones >= ?min_stake.

%% stake command
stake_dynamicpre(#s{unstaked_validators = Dead0}, [_, _, _, bad_validator]) ->
    Dead = lists:flatten(Dead0),
    Dead /= [];
stake_dynamicpre(_S, [Accounts, _Dead, _DynAccts, balance]) ->
    maps:size(maps:filter(fun lt_stake/2, Accounts)) =/= 0;
%% we need at least one possible staker for these others to be reasonable
stake_dynamicpre(_S, [Accounts, _Dead, _DynAccts, _]) ->
    maps:size(maps:filter(fun ge_stake/2, Accounts)) =/= 0.

stake_args(S) ->
    oneof([[S#s.accounts, S#s.unstaked_validators, {var, accounts}, valid],
           [S#s.accounts, S#s.unstaked_validators, {var, accounts}, balance],
           [S#s.accounts, S#s.unstaked_validators, {var, accounts}, bad_sig],
           [S#s.accounts, S#s.unstaked_validators, {var, accounts}, bad_validator],
           [S#s.accounts, S#s.unstaked_validators, {var, accounts}, bad_owner]]).

stake(SymAccts, Dead, Accounts, Reason) ->
    %% todo rich accounts vs poor accounts
    Filter = case Reason of
                 balance -> fun lt_stake/2;
                 _ -> fun ge_stake/2
             end,
    {Val, Addr, Account} =
        case Reason of
            bad_validator ->
                V = select(Dead),
                {V, V#validator.addr, V#validator.owner};
            _ ->
                Acct = select(maps:keys(maps:filter(Filter, SymAccts))),
                [{Address, {_, _, Sig}}] = test_utils:generate_keys(1),
                {#validator{owner = Acct,
                            addr = Address,
                            sig_fun = Sig,
                            stake = ?min_stake},
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
            ?min_stake,
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
           [_SymAccounts, _Dead, _Accounts, Reason]) ->
    S#s{%% accounts = ?call(update_accounts, [stake, SymAccounts, Reason, V]),
        pending_txns = ?call(add_pending, [V, S#s.txn_ctr, S#s.pending_txns, Reason]),
        pending_validators = ?call(update_validators, [S#s.pending_validators, Reason, V]),
        txn_ctr = S#s.txn_ctr + 1}.

update_validators(Validators, Reason, {ok, Val, _Txn}) ->
    case Reason of
        valid -> Validators ++ [Val];
        _ -> Validators
    end.

%% unstake command
unstake_dynamicpre(#s{unstaked_validators = Dead0},
                   [_, _, _, _, _, _, _, bad_validator]) ->
    Dead = lists:flatten(Dead0),
    lager:info("XXX ~p ~p", [Dead, Dead0]),
    Dead /= [];
unstake_dynamicpre(#s{pretransfer = Pretransfer0,
                      prepending_unstake = Unstaked,
                      group = Group0,
                      validators = Validators0},
                   [_, _, _, _, _, _, _, in_group]) ->
    Group = selectable_group_vals(Group0, Validators0),
    Validators = maps:values(maps:filter(fun no_id0/2, Validators0)),
    {Pretransfer, _Amts, _NewVals} = lists:unzip3(Pretransfer0),
    Group =/= []
        andalso (Validators -- (Pretransfer ++ lists:flatten(maps:values(Unstaked)))) /= [];
unstake_dynamicpre(#s{pretransfer = Pretransfer0,
                      prepending_unstake = Unstaked,
                      group = Group0,
                      validators = Validators0},
                   [_, _, _, _, _, _, _, _Reason]) ->
    Group = selectable_group_vals(Group0, Validators0),
    Validators = maps:values(maps:filter(fun no_id0/2, Validators0)),
    {Pretransfer, _Amts, _NewVals} = lists:unzip3(Pretransfer0),
    (Validators -- (Group ++ Pretransfer ++ lists:flatten(maps:values(Unstaked)))) /= [].

no_id0(_, #validator{owner = 0}) ->
    false;
no_id0(_, _) ->
    true.

unstake_args(S) ->
    oneof([
           [S#s.height, {var, accounts}, S#s.group, S#s.prepending_unstake,
            S#s.pretransfer, S#s.validators, S#s.unstaked_validators, valid],
           [S#s.height, {var, accounts},  S#s.group, S#s.prepending_unstake,
            S#s.pretransfer, S#s.validators, S#s.unstaked_validators, bad_account],
           [S#s.height, {var, accounts},  S#s.group, S#s.prepending_unstake,
            S#s.pretransfer, S#s.validators, S#s.unstaked_validators, bad_sig],
           [S#s.height, {var, accounts}, S#s.group, S#s.prepending_unstake,
            S#s.pretransfer, S#s.validators, S#s.unstaked_validators, wrong_account],
           [S#s.height, {var, accounts}, S#s.group, S#s.prepending_unstake,
            S#s.pretransfer, S#s.validators, S#s.unstaked_validators, in_group],
           [S#s.height, {var, accounts}, S#s.group, S#s.prepending_unstake,
            S#s.pretransfer, S#s.validators, S#s.unstaked_validators, bad_validator]
          ]).

unstake(Height, Accounts, Group0, Unstaked, Pretransfer0, SymVals, Dead, Reason) ->
    {Pretransfer, _Amts, _NewVals} = lists:unzip3(Pretransfer0),
    Group = selectable_group_vals(Group0, SymVals),
    Val = case Reason of
              bad_validator -> select(Dead);
              in_group -> select(Group);
              _ -> select(maps:values(maps:filter(fun no_id0/2, SymVals))
                          -- (Group ++ Pretransfer ++ lists:flatten(maps:values(Unstaked))))
          end,
    lager:info("unstake ~p reas ~p ded ~p ok ~p",
               [Val, Reason, Dead,
                maps:values(maps:filter(fun no_id0/2, SymVals))
                -- (Group ++ Pretransfer ++ lists:flatten(maps:values(Unstaked)))]),
    Txn = unstake_txn(Val, Accounts, Height, Reason),
    {ok, Val, Txn}.

unstake_next(#s{} = S,
             V,
             [_Height, _Accounts, _Group, _Vals, _, _, _, Reason]) ->
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
            ?min_stake,
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
%%%%
%%% transfer command
%%%%

transfer_dynamicpre(#s{pretransfer = Pretransfer0,
                       group = Group,
                       accounts = Accounts,
                       prepending_unstake = Unstaked,
                       validators = Validators0}, [_, _, _, _, _, _, InAddr, in_group]) ->
    Amt = case InAddr of
              true -> 0;
              false -> 0;
              amount -> ?bones(9000)
          end,
    Validators = maps:values(maps:filter(fun no_id0/2, Validators0)),
    {Pretransfer, _Amts, _NewVals} = lists:unzip3(Pretransfer0),
    Possible = (Validators -- (Pretransfer ++ lists:flatten(maps:values(Unstaked)))),
    PossibleAddrs = [Addr || #validator{addr = Addr} <- Possible],
    Possible /= []
        andalso lists:any(fun(X) -> lists:member(X, Group) end, PossibleAddrs)
        andalso maps:size(maps:filter(fun(_, {Bal, _Tot}) -> Bal >= Amt end, Accounts)) =/= 0;
transfer_dynamicpre(#s{pretransfer = Pretransfer0,
                       accounts = Accounts,
                       prepending_unstake = Unstaked,
                       validators = Validators0,
                       unstaked_validators = Dead0}, [_, _, _, _, _, _, InAddr, bad_validator]) ->
    Dead = lists:flatten(Dead0),
    Amt = case InAddr of
              true -> 0;
              false -> 0;
              amount -> ?bones(9000)
          end,
    Validators = maps:values(maps:filter(fun no_id0/2, Validators0)),
    {Pretransfer, _Amts, _NewVals} = lists:unzip3(Pretransfer0),
    (Validators -- (Pretransfer ++ lists:flatten(maps:values(Unstaked)))) /= []
        andalso maps:size(maps:filter(fun(_, {Bal, _Tot}) -> Bal >= Amt end, Accounts)) =/= 0
        andalso Dead /= [];
transfer_dynamicpre(#s{pretransfer = Pretransfer0,
                       group = Group0,
                       accounts = Accounts,
                       prepending_unstake = Unstaked,
                       validators = Validators0}, [_, _, _, _, _, _, amount, valid]) ->
    %% when amount + valid, we need to make sure that there is something it's possible to transfer
    Group = selectable_group_vals(Group0, Validators0),
    Validators = maps:values(maps:filter(fun no_id0/2, Validators0)),
    {Pretransfer, _Amts, _NewVals} = lists:unzip3(Pretransfer0),
    (Validators -- (Group ++ Pretransfer ++ lists:flatten(maps:values(Unstaked)))) /= []
        andalso
        %% and that there is an account with enough balance to cover the amount
        maps:size(maps:filter(fun(_, {Bal, _Tot}) -> Bal > ?bones(9000) end, Accounts)) =/= 0;
transfer_dynamicpre(#s{pretransfer = Pretransfer0,
                       group = Group0,
                       prepending_unstake = Unstaked,
                       validators = Validators0}, _Args) ->
    Group = selectable_group_vals(Group0, Validators0),
    Validators = maps:values(maps:filter(fun no_id0/2, Validators0)),
    {Pretransfer, _Amts, _NewVals} = lists:unzip3(Pretransfer0),
    (Validators -- (Group ++ Pretransfer ++ lists:flatten(maps:values(Unstaked)))) /= [].

acct_transfer() ->
    oneof([true, false, amount]).

transfer_args(S) ->
    oneof([
           [{var, accounts}, S#s.group, S#s.prepending_unstake, S#s.pretransfer,
            S#s.validators, S#s.unstaked_validators, acct_transfer(), valid],
           [{var, accounts}, S#s.group, S#s.prepending_unstake, S#s.pretransfer,
            S#s.validators, S#s.unstaked_validators, acct_transfer(), bad_sig],
           [{var, accounts}, S#s.group, S#s.prepending_unstake, S#s.pretransfer,
            S#s.validators, S#s.unstaked_validators, acct_transfer(), bad_account],
           [{var, accounts}, S#s.group, S#s.prepending_unstake, S#s.pretransfer,
            S#s.validators, S#s.unstaked_validators, acct_transfer(), wrong_account],
           [{var, accounts}, S#s.group, S#s.prepending_unstake, S#s.pretransfer,
            S#s.validators, S#s.unstaked_validators, acct_transfer(), in_group],
           [{var, accounts}, S#s.group, S#s.prepending_unstake, S#s.pretransfer,
            S#s.validators, S#s.unstaked_validators, acct_transfer(), bad_validator]
          ]).

transfer(Accounts, Group0, Unstaked, Pretransfer0, SymVals, Dead, IntraAddr, Reason) ->
    lager:info("xxx ~p",[Pretransfer0]),
    Group = selectable_group_vals(Group0, SymVals),
    {Pretransfer, _Amts, _NewVals} = lists:unzip3(Pretransfer0),
    OldVal = case Reason of
                 bad_validator -> select(Dead);
                 in_group -> select(Group);
                 _ -> select(maps:values(maps:filter(fun no_id0/2, SymVals))
                             -- (Pretransfer ++ lists:flatten(maps:values(Unstaked))))
             end,
    [{Address, {_, _, Sig}}] = test_utils:generate_keys(1),
    {Txn, Amt, NewVal} = transfer_txn(OldVal, Address, Sig, IntraAddr, Accounts, Reason),
    {OldVal, NewVal, Amt, Txn}.

transfer_next(#s{} = S,
              V,
              [_Accounts, _Group, _Vals, _, _, _, _IntraAddr, Reason]) ->
    S#s{pretransfer = ?call(update_pretransfer, [S#s.pretransfer, Reason, V]),
        pending_txns = ?call(add_trans_pending, [V, S#s.txn_ctr, S#s.pending_txns, Reason]),
        %% pending_validators = ?call(transfer_update_validators, [S#s.pending_validators, Reason, V]),
        txn_ctr = S#s.txn_ctr + 1}.

add_trans_pending({_Old, _New, _Amt, Txn}, ID, Pending, Reason) ->
    Pending#{ID => {Reason, Txn}}.

update_pretransfer(Pretransfer, valid, {Val, NewVal, Amt, _Txn}) ->
    [{Val, Amt, NewVal} | Pretransfer];
update_pretransfer(Pretransfer, _Reason, _Res) ->
    Pretransfer.

transfer_txn(#validator{owner = Owner, addr = Addr},
             NewValAddr, NewValSig,
             IntraAddr,
             Accounts,
             Reason) ->
    %% can't do bad_account because that will just create a new account
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

    {{ID, NewAccount}, Amt} =
        case IntraAddr of
            false -> {{Account#account.id, #account{address = <<>>}}, 0};
            true -> {select(maps:to_list(maps:remove(Owner, Accounts))), 0};
            amount -> {select(maps:to_list(maps:remove(Owner, Accounts))), ?bones(9000)}
        end,

    NewVal = #validator{owner = ID,
                        addr = NewValAddr,
                        sig_fun = NewValSig,
                        stake = ?min_stake},

    Txn = blockchain_txn_transfer_validator_stake_v1:new(
            Addr, NewValAddr,
            Account#account.address,
            NewAccount#account.address,
            ?min_stake,
            Amt,
            35000
           ),
    STxn0 = blockchain_txn_transfer_validator_stake_v1:sign(Txn, Account#account.sig_fun),
    STxn = case IntraAddr of
               OK when OK == true; OK == amount ->
                   blockchain_txn_transfer_validator_stake_v1:new_owner_sign(
                     STxn0,
                     NewAccount#account.sig_fun);
               false -> STxn0
           end,
    FinalTxn =
        case Reason of
            bad_sig ->
                blockchain_txn_transfer_validator_stake_v1:old_owner_signature(<<0:512>>, Txn);
            _ ->
                STxn
        end,
    {FinalTxn, Amt, NewVal}.

%% election psuedo-commands
election_pre(S, _) ->
    S#s.height rem 3 == 0.

election_args(S) ->
    [S#s.height, {var, chain},  S#s.validators, S#s.group].

election(Height, Chain, Validators, CurrGroup)  ->
    Ledger = blockchain:ledger(Chain),
    {ok, Block} = blockchain:get_block(Height, Chain),
    Hash = blockchain_block:hash_block(Block),
    NewGroup = blockchain_election:new_group(Ledger, Hash, 4, 0),
    lager:info("groups ~p -> ~p",
               [lists:map(fun blockchain_utils:addr2name/1, CurrGroup),
                lists:map(fun blockchain_utils:addr2name/1, NewGroup)]),

    Artifact = term_to_binary(NewGroup),

    Signatures = signatures(NewGroup, Validators, Artifact),
    Proof = term_to_binary(Signatures, [compressed]),

    GroupTxn = blockchain_txn_consensus_group_v1:new(
                 NewGroup, Proof, Height, 0),

    {ok, _Valid, _Invalid, NewBlock} = block(Chain, CurrGroup, Validators, #{whatever => {valid, GroupTxn}}),

    {NewGroup, NewBlock}.

election_next(#s{pending_txns = PendTxns} = S,
              V,
              _Args) ->
    NewHeight = S#s.height + 1,
    Update = ?call(fixup_txns, [S#s.group, V, PendTxns,
                                S#s.prepending_unstake, S#s.pretransfer]),
    S#s{chain = ?call(add_block, [S#s.chain, V]),
        group = ?call(update_group, [V]),
        pretransfer = {call, erlang, element, [3, Update]},
        prepending_unstake = {call, erlang, element, [2, Update]},
        pending_txns = {call, erlang, element, [1, Update]},
        height = NewHeight}.

update_group({NewGroup, _Block}) ->
    NewGroup.

fixup_txns(OldGroup, {NewGroup, _}, Pending, Unstake, Pretransfer) ->
    maps:fold(
      fun(K, {Reason, Txn} = Orig, {Acc, Uns, Pre}) ->
              case blockchain_txn:type(Txn) of
                  blockchain_txn_transfer_validator_stake_v1 ->
                      OldVal = blockchain_txn_transfer_validator_stake_v1:old_validator(Txn),
                      case Reason of
                          in_group ->
                              %% if this is an invalid txn but an election happens, it becomes valid
                              %% if the validator was elected out
                              case lists:member(OldVal, OldGroup) andalso
                                  not lists:member(OldVal, NewGroup) of
                                  true ->
                                      %% this becomes valid, but for now the new val is lost?
                                      %%Pre1 = [{OldV, Amt, NewV} | Pre],
                                      %%{Acc#{K => {valid, Txn}}, Uns, Pre1};
                                      {Acc, Uns, Pre};
                                  _ ->
                                      {Acc#{K => Orig}, Uns, Pre}
                              end;
                          valid ->
                              case lists:member(OldVal, NewGroup) of
                                  true ->
                                      %% this becomes invalid, so needs to be removed from pretransfer
                                      Pre1 = lists:filter(fun({#validator{addr = Addr}, _, _}) ->
                                                                  Addr /= OldVal
                                                          end, Pre),
                                      {Acc#{K => {in_group, Txn}}, Uns, Pre1};
                                  _ ->
                                      {Acc#{K => Orig}, Uns, Pre}
                              end;
                          _ -> {Acc#{K => Orig}, Uns, Pre}
                      end;
                  blockchain_txn_unstake_validator_v1 ->
                      OldVal = blockchain_txn_unstake_validator_v1:address(Txn),
                      case Reason of
                          in_group ->
                              %% if this is an invalid txn but an election happens, it becomes valid
                              %% if the validator was elected out
                              case lists:member(OldVal, OldGroup) andalso
                                  not lists:member(OldVal, NewGroup) of
                                  true ->
                                      %% this becomes valid, so needs to be added to prepending
                                      %% unstake, but the height is lost, so we drop it?
                                      %% {valid, Txn};
                                      {Acc, Uns, Pre};
                                  _ ->
                                      {Acc#{K => Orig}, Uns, Pre}
                             end;
                          valid ->
                              case lists:member(OldVal, NewGroup) of
                                  true ->
                                      %% this becomes invalid, so needs to be removed from unstake
                                      Uns1 = maps:map(fun(_Ht, Lst) ->
                                                              lists:filter(fun(#validator{addr = Addr}) ->
                                                                                   Addr /= OldVal
                                                                           end,
                                                                           Lst)
                                                      end, Uns),
                                      {Acc#{K => {in_group, Txn}}, Uns1, Pre};
                                  _ ->
                                      {Acc#{K => Orig}, Uns, Pre}
                              end;
                          _ -> {Acc#{K => Orig}, Uns, Pre}
                      end;
                  _ ->
                      {Acc#{K => Orig}, Uns, Pre}
              end
      end,
      {#{}, Unstake, Pretransfer},
      Pending).

%% block commands
block_pre(S, _) ->
    S#s.height rem 5 =/= 0.

block_args(S) ->
    [{var, chain}, S#s.group, S#s.validators, S#s.pending_txns].

block(Chain, Group, Validators, Txns) ->
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
    Signatures = signatures(Group, Validators, BinBlock),
    Block1 = blockchain_block:set_signatures(Block0, Signatures),
    %% lager:info("txns ~p", [Block1]),
    {ok, Valid, Invalid, Block1}.

block_next(#s{} = S,
           V,
           [Chain, _Group, _, _Transactions]) ->
    NewHeight = S#s.height + 1,
    S#s{chain = ?call(add_block, [Chain, V]),
        height = NewHeight,

        accounts = ?call(block_update_accounts, [NewHeight, S#s.accounts,
                                                 S#s.pending_validators,
                                                 S#s.pretransfer,
                                                 S#s.pending_unstake]),

        pending_validators = [],
        validators = ?call(block_update_validators, [S#s.pending_validators,
                                                     S#s.prepending_unstake,
                                                     S#s.pretransfer,
                                                     S#s.validators]),

        pretransfer = [],
        prepending_unstake = #{},
        pending_unstake = ?call(update_unstake, [NewHeight, S#s.prepending_unstake, S#s.pending_unstake]),
        unstaked_validators = S#s.unstaked_validators ++
            lists:flatten([ ?call(update_dead_validators, [S#s.prepending_unstake])]),

        pending_txns = ?call(update_pending, [V, S#s.pending_txns])}.

block_update_validators(PV, Unstakes, Pretransfer0, V) ->
    {Pretransfer, _Amts, NewVals} = lists:unzip3(Pretransfer0),
    New = (PV ++ NewVals),
    V2 = lists:foldl(
           fun(Val = #validator{addr = Addr}, Acc) ->
                   Acc#{Addr => Val}
           end,
           V,
           New),
    lists:foldl(
      fun(#validator{addr = Addr}, Acc) ->
              maps:remove(Addr, Acc)
      end,
      V2,
      Pretransfer ++ lists:flatten(maps:values(Unstakes))).

update_dead_validators(Unstakes) ->
    lists:flatten(maps:values(Unstakes)).

update_unstake(Height, PP, P) ->
    maps:remove(Height, maps:merge(PP, P)).

block_update_accounts(Height, Accounts, PendingValidators, PendingTransfer, PendingUnstake) ->
    Accounts1 =
        case maps:find(Height, PendingUnstake) of
            {ok, Vals} ->
                lists:foldl(
                  fun(Val, Acc) ->
                          {Bal, Tot} = maps:get(Val#validator.owner, Acc),
                          Acc#{Val#validator.owner => {Bal + ?min_stake, Tot}}
                  end, Accounts,
                  Vals);
            _ -> Accounts
        end,
    Accounts2 =
        lists:foldl(
          fun(Val, Acc) ->
                  {Bal, Tot} = maps:get(Val#validator.owner, Acc),
                  Acc#{Val#validator.owner => {Bal - ?min_stake, Tot}}
          end, Accounts1,
          PendingValidators),
    lists:foldl(
      fun({OldVal, Amt, NewVal}, Acc) ->
              case OldVal#validator.owner == NewVal#validator.owner of
                  true ->
                      Acc; % no overall change
                  false ->
                      {OldBal, OldTot} = maps:get(OldVal#validator.owner, Acc),
                      {NewBal, NewTot} = maps:get(NewVal#validator.owner, Acc),
                      Acc#{OldVal#validator.owner => {OldBal + Amt, OldTot + Amt - ?min_stake},
                           NewVal#validator.owner => {NewBal - Amt, NewTot - Amt + ?min_stake}}
              end
      end,
      Accounts2,
      PendingTransfer).

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
       Cmds, more_commands(25, commands(?M)),
       %% Cmds, noshrink(more_commands(5, commands(?M))),
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

signatures(Members, Validators, Bin) ->
    lists:foldl(
      fun(Addr, Acc) ->
              #validator{sig_fun = F} = maps:get(Addr, Validators),
              Sig = F(Bin),
              [{Addr, Sig}|Acc]
      end, [], Members).

selectable_group_vals(Group, Vals0) ->
    Vals = maps:filter(fun no_id0/2, Vals0),
    lists:foldl(
      fun(Mem, Acc) ->
              case maps:find(Mem, Vals) of
                  {ok, V} -> [V | Acc];
                  _ -> Acc
              end
      end,
      [], Group).

cleanup(#s{}, Env) ->
    Dir = maps:get(base_dir, maps:from_list(Env)),
    lager:info("entering cleanup"),
    PWD = string:trim(os:cmd("pwd")),
    os:cmd("rm -r " ++ PWD ++ "/" ++ Dir),
    true.
