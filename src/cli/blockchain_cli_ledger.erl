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
    register_all_usage()
    ,register_all_cmds().

register_all_usage() ->
    lists:foreach(fun(Args) ->
                          apply(clique, register_usage, Args)
                  end,
                  [
                   ledger_pay_usage(),
                   ledger_create_htlc_usage(),
                   ledger_redeem_htlc_usage(),
                   ledger_balance_usage(),
                   ledger_export_usage(),
                   ledger_gateways_usage(),
                   ledger_add_gateway_usage(),
                   ledger_assert_loc_request_usage(),
                   ledger_assert_loc_txn_usage(),
                   ledger_usage()
                  ]).

register_all_cmds() ->
    lists:foreach(fun(Cmds) ->
                          [apply(clique, register_command, Cmd) || Cmd <- Cmds]
                  end,
                  [
                   ledger_pay_cmd(),
                   ledger_create_htlc_cmd(),
                   ledger_redeem_htlc_cmd(),
                   ledger_balance_cmd(),
                   ledger_export_cmd(),
                   ledger_gateways_cmd(),
                   ledger_add_gateway_cmd(),
                   ledger_assert_loc_request_cmd(),
                   ledger_assert_loc_txn_cmd(),
                   ledger_cmd()
                  ]).

%%--------------------------------------------------------------------
%% ledger
%%--------------------------------------------------------------------
ledger_usage() ->
    [["ledger"],
     ["blockchain ledger commands\n\n",
      "  ledger balance             - Get the balance for this or all addresses.\n"
      "  ledger export              - Export transactions from the ledger to <file>.\n"
      "  ledger pay                 - Transfer tokens to a crypto address.\n"
      "  ledger create_htlc         - Create or a hashed timelock address.\n"
      "  ledger redeem_htlc         - Redeem from a hashed timelock address.\n"
      "  ledger gateways            - Display the list of active gateways.\n"
      "  ledger add_gateway         - Register a gateway.\n"
      "  ledger assert_loc_request  - Request the assertion of a gateway's location.\n"
      "  ledger assert_loc_txn      - Countersign the assertion of a gateway's location and submit it.\n"
     ]
    ].

ledger_cmd() ->
    [
     [["ledger"], [], [], fun(_, _, _) -> usage end]
    ].


%%--------------------------------------------------------------------
%% ledger pay
%%--------------------------------------------------------------------
ledger_pay_cmd() ->
    [
     [["ledger", "pay", '*', '*', '*'], [], [], fun ledger_pay/3]
    ].

ledger_pay_usage() ->
    [["ledger", "pay"],
     ["ledger pay <address> <amount> <fee>\n\n",
      "  Transfer given <amount> to the target <address> with a <fee> for the miners.\n"
     ]
    ].

ledger_pay(["ledger", "pay", Addr, Amount, F], [], []) ->
    case (catch {libp2p_crypto:b58_to_bin(Addr),
                 list_to_integer(Amount), list_to_integer(F)}) of
        {'EXIT', _} ->
            usage;
        {Recipient, Value, Fee} when Value > 0 ->
            blockchain_worker:spend(Recipient, Value, Fee),
            [clique_status:text("ok")];
        _ -> usage
    end;
ledger_pay(_, _, _) ->
    usage.

%%--------------------------------------------------------------------
%% ledger create_htlc
%%--------------------------------------------------------------------
ledger_create_htlc_cmd() ->
    [
     [["ledger", "create_htlc"], '_', [
                                       {payee, [{shortname, "p"}, {longname, "payee"}]},
                                       {value, [{shortname, "v", {longname, "value"}}]},
                                       {hashlock, [{shortname, "h"}, {longname, "hashlock"}]},
                                       {timelock, [{shortname, "t"}, {longname, "timelock"}]},
                                       {fee, [{shortname, "f"}, {longname, "fee"}]}
                                      ], fun ledger_create_htlc/3]
    ].

ledger_create_htlc_usage() ->
    [["ledger", "create_htlc"],
     ["ledger create_htlc\n\n",
      "  Creates a new HTLC address with a specified hashlock and timelock (in block height), and transfers a value of tokens to it.\n"
      "Required:\n\n"
      "  -p, --payee <value>\n",
      "  The base58 address of the intended payee for this HTLC\n",
      "  -v, --value <value>\n",
      "  The amount of tokens to transfer to the contract address\n",
      "  -h, --hashlock <sha256hash>\n",
      "  A SHA256 digest of a secret value (called a preimage) that locks this contract\n",
      "  -t, --timelock <blockheight>\n",
      "  A specific blockheight after which the payer (you) can redeem their tokens\n",
      "  -f --fee <fee>\n",
      "  The fee for the miners\n"
     ]
    ].

ledger_create_htlc(_CmdBase, _, []) ->
    usage;
ledger_create_htlc(_CmdBase, _Keys, Flags) ->
    % generate 32 random bytes for an address as no keys are needed
    Address = crypto:strong_rand_bytes(32),
    case (catch ledger_create_htlc_helper(Flags, Address)) of
        {'EXIT', _Reason} ->
            usage;
        ok ->
            Text = io_lib:format("Created HTLC at address ~p", [libp2p_crypto:bin_to_b58(?B58_HTLC_VER, Address)]),
            [clique_status:text(Text)];
        _ -> usage
    end.

ledger_create_htlc_helper(Flags, Address) ->
    Payee = libp2p_crypto:b58_to_bin(clean(proplists:get_value(payee, Flags))),
    Amount = list_to_integer(clean(proplists:get_value(value, Flags))),
    Hashlock = blockchain_util:hex_to_bin(list_to_binary(clean(proplists:get_value(hashlock, Flags)))),
    Timelock = list_to_integer(clean(proplists:get_value(timelock, Flags))),
    Fee = list_to_integer(clean(proplists:get_value(fee, Flags))),
    blockchain_worker:create_htlc_txn(Payee, Address, Hashlock, Timelock, Amount, Fee).

%%--------------------------------------------------------------------
%% ledger redeem
%%--------------------------------------------------------------------
ledger_redeem_htlc_cmd() ->
    [
     [["ledger", "redeem_htlc"], '_', [
                                       {address, [{shortname, "a"}, {longname, "address"}]},
                                       {preimage, [{shortname, "p"}, {longname, "preimage"}]},
                                       {fee, [{shortname, "f"}, {longname, "fee"}]}
                                      ], fun ledger_redeem_htlc/3]
    ].

ledger_redeem_htlc_usage() ->
    [["ledger", "redeem_htlc"],
     ["Redeem the balance from an HTLC address with the specified preimage for the Hashlock.\n"
      "Required:\n\n"
      "  -a, --address <address>\n",
      "  The address of the Hashed TimeLock Contract to redeem from\n",
      "  -p --preimage <preimage>\n",
      "  The preimage used to create the Hashlock for this contract address\n",
      "  -f --fee <fee>\n",
      "  The fee for the miners\n"
     ]
    ].

ledger_redeem_htlc(_CmdBase, _, []) ->
    usage;
ledger_redeem_htlc(_CmdBase, _Keys, Flags) ->
    case (catch ledger_redeem_htlc_helper(Flags)) of
        {'EXIT', _Reason} ->
            usage;
        ok ->
            [clique_status:text("ok")];
        _ -> usage
    end.

ledger_redeem_htlc_helper(Flags) ->
    Address = libp2p_crypto:b58_to_bin(clean(proplists:get_value(address, Flags))),
    Preimage = list_to_binary(clean(proplists:get_value(preimage, Flags))),
    Fee = list_to_integer(clean(proplists:get_value(fee, Flags))),
    blockchain_worker:redeem_htlc_txn(Address, Preimage, Fee).

%%--------------------------------------------------------------------
%% ledger add_gateway
%%--------------------------------------------------------------------
ledger_add_gateway_cmd() ->
    [
     [
      ["ledger", "add_gateway"], '_', [
                                       {address, [{shortname, "a"}, {longname, "address"}]},
                                       {request, [{shortname, "r"}, {longname, "request"}]},
                                       {token, [{shortname, "t"}, {longname, "token"}]}
                                      ], fun ledger_add_gateway/3
     ]
    ].

ledger_add_gateway_usage() ->
    [["ledger", "add_gateway"],
     ["ledger add_gateway\n\n",
      "  Request the addition of the current node as a gateway owned by the given owner.\n"
      "  The authorization is signed by the given authorization address and the given\n"
      "  authorization token is passed through to with the authorization request.\n"
      "  Use key=value args to set options.\n\n",
      "Required:\n\n"
      "  -a, --address [P2PAddress]\n",
      "   The p2paddress of the node (usually a wallet) that will authorize this request\n",
      "  -o, --owner [OwnerAddress]\n",
      "   The crypto address of the owner of the gateway. Used as the payee for mined tokens\n"
      "  -t, --token [Token]\n",
      "   The token given by the running wallet to identify the authorization request\n"
     ]
    ].

ledger_add_gateway(_CmdBase, _, Flags) ->
    case (catch ledger_add_gateway_helper(Flags)) of
        {'EXIT', _Reason} ->
            usage;
        ok ->
            [clique_status:text("ok")];
        {error, Reason} ->
            ReasonText = clique_status:text(io_lib:format("~p", Reason)),
            [clique_status:alert([ReasonText])];
        _ -> usage
    end.

ledger_add_gateway_helper(Flags) ->
    OwnerAddress = libp2p_crypto:b58_to_bin(clean(proplists:get_value(owner, Flags))),
    AuthAddress = clean(proplists:get_value(address, Flags)),
    AuthToken = clean(proplists:get_value(token, Flags)),
    blockchain_worker:add_gateway_request(OwnerAddress, AuthAddress, AuthToken).

%%--------------------------------------------------------------------
%% ledger balance
%%--------------------------------------------------------------------
ledger_balance_cmd() ->
    [
     [["ledger", "balance", '*'], [], [], fun ledger_balance/3],
     [["ledger", "balance"], [],
      [ {htlc, [{shortname, "p"},
              {longname, "htlc"}]},
        {all, [{shortname, "a"},
              {longname, "all"}]}
      ], fun ledger_balance/3]
    ].

ledger_balance_usage() ->
    [["ledger", "balance"],
     ["ledger balance [<address> | -a | -h]\n\n",
      "  Retrieve the current balanace for this node, all nodes,\n",
      "  or a given <address>.\n\n"
      "Options\n\n",
      "  -a, --all\n",
      "    Display balances for all known addresses.\n"
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
    PubkeyBin = blockchain_swarm:pubkey_bin(),
    Ledger = get_ledger(),
    {ok, Entry} = blockchain_ledger_v1:find_entry(PubkeyBin, Ledger),
    R = [format_ledger_balance({PubkeyBin, Entry})],
    [clique_status:table(R)];
ledger_balance(_CmdBase, [], [{htlc, _}]) ->
    HTLCs = maps:filter(fun(K, _V) ->
                                   is_binary(K)
                           end, blockchain_ledger_v1:htlcs(get_ledger())),
    R = [format_htlc_balance({A, H}) || {A, H} <- maps:to_list(HTLCs)],
    [clique_status:table(R)];
ledger_balance(_CmdBase, [], [{all, _}]) ->
    Entries = maps:filter(fun(K, _V) ->
                                   is_binary(K)
                           end, blockchain_ledger_v1:entries(get_ledger())),
    R = [format_ledger_balance({A, E}) || {A, E} <- maps:to_list(Entries)],
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
     {hashlock, blockchain_util:bin_to_hex(blockchain_ledger_htlc_v1:hashlock(HTLC))},
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
     [["ledger", "gateways"], [], [], fun ledger_gateways/3]
    ].

ledger_gateways_usage() ->
    [["ledger", "gateways"],
     ["ledger gateways\n\n",
      "  Retrieve all currently active gateways and their H3 index.\n"
     ]
    ].

ledger_gateways(_CmdBase, [], []) ->
    Gateways = blockchain_ledger_v1:active_gateways(get_ledger()),
    R = [format_ledger_gateway_entry(G) || G <- maps:to_list(Gateways)],
    [clique_status:table(R)].

format_ledger_gateway_entry({GatewayAddr, Gateway}) ->
    [{gateway_address, libp2p_crypto:pubkey_bin_to_p2p(GatewayAddr)} |
     blockchain_ledger_gateway_v1:print(Gateway)].

%%--------------------------------------------------------------------
%% ledger assert_loc_request
%%--------------------------------------------------------------------
ledger_assert_loc_request_cmd() ->
    [
     [["ledger", "assert_loc_request", '*', '*'], [], [], fun ledger_assert_loc_request/3]
    ].

ledger_assert_loc_request_usage() ->
    [["ledger", "assert_loc_request"],
     ["ledger assert_loc_request <b58> <loc>\n\n",
      "  Request to assert <loc> of the current node as a gateway owned by <b58>.\n"
     ]
    ].

ledger_assert_loc_request(["ledger", "assert_loc_request", Addr, Location], [], []) ->
    case (catch libp2p_crypto:b58_to_bin(Addr)) of
        {'EXIT', _Reason} ->
            usage;
        Owner when is_binary(Owner) ->
            case blockchain_worker:assert_location_request(Owner, Location) of
                {error, Reason} ->
                    [clique_status:text(io_lib:format("~p", [Reason]))];
                Txn ->
                    [clique_status:text(base58:binary_to_base58(term_to_binary(Txn)))]
            end;
        _ ->
            usage
    end;
ledger_assert_loc_request(_, _, _) ->
    usage.


%%--------------------------------------------------------------------
%% ledger assert_location_txn
%%--------------------------------------------------------------------
ledger_assert_loc_txn_cmd() ->
    [
     [["ledger", "assert_loc_txn", '*'], [], [], fun ledger_assert_loc_txn/3]
    ].

ledger_assert_loc_txn_usage() ->
    [["ledger", "assert_loc_txn"],
     ["ledger assert_loc_txn <txn>\n\n",
      "  Countersign the assert_loc transaction <txn> and submit it.\n"
     ]
    ].

ledger_assert_loc_txn(["ledger", "assert_loc_txn", AssertLocRequest], [], []) ->
    case (catch binary_to_term(base58:base58_to_binary(AssertLocRequest))) of
        {'EXIT', _} ->
            usage;
        Txn ->
            blockchain_worker:assert_location_txn(Txn),
            [clique_status:text("ok")]
            %_ -> usage
    end;
ledger_assert_loc_txn(_, _, _) ->
    usage.

%% NOTE: I noticed that giving a shortname to the flag would end up adding a leading "="
%% Presumably none of the flags would be _having_ a leading "=" intentionally!
clean(String) ->
    case string:split(String, "=", leading) of
        [[], S] -> S;
        [S] -> S;
        _ -> error
    end.
