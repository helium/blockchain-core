%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain CLI Ledger ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_cli_ledger).

-behavior(clique_handler).

-export([register_cli/0]).

-include("blockchain.hrl").

register_cli() ->
    register_all_usage(),
    register_all_cmds().

register_all_usage() ->
    lists:foreach(fun(Args) ->
                          apply(clique, register_usage, Args)
                  end,
                  [
                   ledger_balance_usage(),
                   ledger_export_usage(),
                   ledger_gateways_usage(),
                   ledger_validators_usage(),
                   ledger_variables_usage(),
                   ledger_usage()
                  ]).

register_all_cmds() ->
    lists:foreach(fun(Cmds) ->
                          [apply(clique, register_command, Cmd) || Cmd <- Cmds]
                  end,
                  [
                   ledger_balance_cmd(),
                   ledger_export_cmd(),
                   ledger_gateways_cmd(),
                   ledger_validators_cmd(),
                   ledger_variables_cmd(),
                   ledger_cmd()
                  ]).

%%--------------------------------------------------------------------
%% ledger
%%--------------------------------------------------------------------
ledger_usage() ->
    [["ledger"],
     ["blockchain ledger commands\n\n",
      "  ledger balance             - Get the balance for one or all addresses.\n"
      "  ledger export              - Export transactions from the ledger to <file>.\n"
      "  ledger gateways            - Display the list of active gateways.\n"
      "  ledger variables           - Interact with chain variables.\n"
     ]
    ].

ledger_cmd() ->
    [
     [["ledger"], [], [], fun(_, _, _) -> usage end]
    ].


%%--------------------------------------------------------------------
%% ledger balance
%%--------------------------------------------------------------------
ledger_balance_cmd() ->
    [
     [["ledger", "balance", '*'], [], [], fun ledger_balance/3],
     [["ledger", "balance"], [],
      [ {htlc, [{shortname, "p"}, {longname, "htlc"}]}
      ], fun ledger_balance/3]
    ].

ledger_balance_usage() ->
    [["ledger", "balance"],
     ["ledger balance [<address> | -p]\n\n",
      "  Retrieve the current balanace for a given <address>, all known addresses or just htlc balances.\n\n"
      "Options\n\n",
      "  -p, --htlc\n",
      "    Display balances for all known HTLCs.\n"
     ]
    ].

ledger_balance(["ledger", "balance", Str], [], []) ->
    Ledger = get_ledger(),
    case (catch libp2p_crypto:b58_to_bin(Str)) of
        {'EXIT', _} -> usage;
        Addr ->
            {ok, Entry} = blockchain_ledger_v1:find_entry(Addr, Ledger),
            R = [format_ledger_balance({Addr, Entry})],
            [clique_status:table(R)]
    end;
ledger_balance(_CmdBase, [], []) ->
    Entries = maps:filter(fun(K, _V) ->
                                   is_binary(K)
                           end, blockchain_ledger_v1:entries(get_ledger())),
    R = [format_ledger_balance({A, E}) || {A, E} <- maps:to_list(Entries)],
    [clique_status:table(R)];
ledger_balance(_CmdBase, [], [{htlc, _}]) ->
    HTLCs = maps:filter(fun(K, _V) ->
                                   is_binary(K)
                           end, blockchain_ledger_v1:htlcs(get_ledger())),
    R = [format_htlc_balance({A, H}) || {A, H} <- maps:to_list(HTLCs)],
    [clique_status:table(R)].

-spec format_ledger_balance({libp2p_crypto:pubkey_bin(), blockchain_ledger_entry_v1:entry() | {error, any()}}) -> list().
format_ledger_balance({Addr, Entry}) ->
    [{p2p, libp2p_crypto:pubkey_bin_to_p2p(Addr)},
     {nonce, integer_to_list(blockchain_ledger_entry_v1:nonce(Entry))},
     {balance, integer_to_list(blockchain_ledger_entry_v1:balance(Entry))}
    ].

format_htlc_balance({Addr, HTLC}) ->
    [{address, libp2p_crypto:bin_to_b58(?B58_HTLC_VER, Addr)},
     {payer, libp2p_crypto:pubkey_bin_to_p2p(blockchain_ledger_htlc_v1:payer(HTLC))},
     {payee, libp2p_crypto:pubkey_bin_to_p2p(blockchain_ledger_htlc_v1:payee(HTLC))},
     {hashlock, blockchain_utils:bin_to_hex(blockchain_ledger_htlc_v1:hashlock(HTLC))},
     {timelock, integer_to_list(blockchain_ledger_htlc_v1:timelock(HTLC))},
     {amount, integer_to_list(blockchain_ledger_htlc_v1:balance(HTLC))}
    ].

get_ledger() ->
    case blockchain_worker:blockchain() of
        undefined ->
            blockchain_ledger_v1:new("data");
        Chain ->
            blockchain:ledger(Chain)
    end.

%%--------------------------------------------------------------------
%% ledger export
%%--------------------------------------------------------------------
ledger_export_cmd() ->
    [
     [["ledger", "export", '*'], [], [], fun ledger_export/3]
    ].

ledger_export_usage() ->
    [["ledger", "export"],
     ["ledger export <filename>\n\n",
      "  Export transactions from the ledger to a given <filename>.\n"
     ]
    ].

ledger_export(["ledger", "export", Filename], [], []) ->
    case (catch file:write_file(Filename,
                                io_lib:fwrite("~p.\n", [blockchain_ledger_exporter_v1:export(get_ledger())]))) of
        {'EXIT', _} ->
            usage;
        ok ->
            [clique_status:text(io_lib:format("ok, transactions written to ~p", [Filename]))];
        {error, Reason} ->
            [clique_status:text(io_lib:format("~p", [Reason]))]
    end;
ledger_export(_, _, _) ->
    usage.

%%--------------------------------------------------------------------
%% ledger gateways
%%--------------------------------------------------------------------
ledger_gateways_cmd() ->
    [
     [["ledger", "gateways"], [], [], fun ledger_gateways/3],
     [["ledger", "gateways"], [],
      [{verbose, [{shortname, "v"},
                  {longname, "verbose"}]}],
      fun ledger_gateways/3]
    ].

ledger_gateways_usage() ->
    [["ledger", "gateways"],
     ["ledger gateways\n\n",
      "  Retrieve all currently active gateways and their H3 index.\n"
     ]
    ].

ledger_gateways(_CmdBase, [], []) ->
    R = ledger_gw_fold(false),
    [clique_status:table(R)];
ledger_gateways(_CmdBase, [], [{verbose, _}]) ->
    R = ledger_gw_fold(true),
    [clique_status:table(R)].

ledger_gw_fold(Verbose) ->
    Ledger = get_ledger(),
    blockchain_ledger_v1:cf_fold(
      active_gateways,
      fun({Addr, BinGw}, Acc) ->
              Gw = blockchain_ledger_gateway_v2:deserialize(BinGw),
              [format_ledger_gateway_entry({Addr, Gw}, Ledger, Verbose) | Acc]
      end,
      [],
      Ledger).

ledger_validators_cmd() ->
    [
     [["ledger", "validators"], [], [], fun ledger_validators/3],
     [["ledger", "validators"], [],
      [{verbose, [{shortname, "v"},
                  {longname, "verbose"}]}],
      fun ledger_validators/3]
    ].

ledger_validators_usage() ->
    [["ledger", "validators"],
     ["ledger validators\n\n",
      "  Retrieve all validators and a table of information about them.\n"
     ]
    ].

ledger_validators(_CmdBase, [], []) ->
    R = ledger_val_fold(false),
    [clique_status:table(R)];
ledger_validators(_CmdBase, [], [{verbose, _}]) ->
    R = ledger_val_fold(true),
    [clique_status:table(R)].

ledger_val_fold(Verbose) ->
    Ledger = get_ledger(),
    blockchain_ledger_v1:cf_fold(
      validators,
      fun({Addr, BinVal}, Acc) ->
              Val = blockchain_ledger_validator_v1:deserialize(BinVal),
              [format_ledger_validator(Addr, Val, Ledger, Verbose) | Acc]
      end,
      [],
      Ledger).

format_ledger_gateway_entry({GatewayAddr, Gateway}, Ledger, Verbose) ->
    {ok, Name} = erl_angry_purple_tiger:animal_name(libp2p_crypto:pubkey_to_b58(libp2p_crypto:bin_to_pubkey(GatewayAddr))),
    [{gateway_address, libp2p_crypto:pubkey_bin_to_p2p(GatewayAddr)},
     {name, Name} |
     blockchain_ledger_gateway_v2:print(GatewayAddr, Gateway, Ledger, Verbose)].

format_ledger_validator(ValAddr, Validator, Ledger, Verbose) ->
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
    [{name, blockchain_utils:addr2name(ValAddr)} |
     blockchain_ledger_validator_v1:print(Validator, Height, Verbose, Ledger)].

%% ledger variables

ledger_variables_cmd() ->
    [
     [["ledger", "variables", '*'], [], [], fun ledger_variables/3],
     [["ledger", "variables"], [],
      [
       {all, [{shortname, "a"},
              {longname, "all"}]}
      ], fun ledger_variables/3]
    ].

ledger_variables_usage() ->
    [["ledger", "variables"],
     ["ledger variables <variable name>\n\n",
      "  Retrieve a chain-stored variable.\n",
      "Options\n\n",
      "  -a, --all\n",
      "    Display all variables.\n"
     ]
    ].

ledger_variables(Cmd, [], Flags) ->
    Ledger = get_ledger(),
    try
        case Cmd of
            [_, _, Name] ->
                NameAtom = list_to_atom(Name),
                case blockchain_ledger_v1:config(NameAtom, Ledger) of
                    {ok, Var} ->
                        [clique_status:text(io_lib:format("~p", [Var]))];
                    {error, not_found} ->
                        [clique_status:text("variable not found")]
                end;
            [_, _] when Flags == [{all, undefined}] ->
                Vars = blockchain_ledger_v1:snapshot_vars(Ledger),
                [clique_status:text(
                   [io_lib:format("~s: ~p~n", [N, V])
                    || {N, V} <- lists:sort(Vars)])];
            _ ->
                usage
        end
    catch _:_ ->
            [clique_status:text("invalid variable name")]
    end.
