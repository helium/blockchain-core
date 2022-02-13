%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain CLI Txn ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_cli_txn).

-behavior(clique_handler).

-export([register_cli/0]).

-include("blockchain.hrl").

register_cli() ->
    register_all_usage(), register_all_cmds().

register_all_usage() ->
    lists:foreach(fun(Args) ->
                          apply(clique, register_usage, Args)
                  end,
                  [
                   txn_add_gateway_usage(),
                   txn_assert_location_usage(),
                   txn_queue_usage(),
                   txn_usage()
                  ]).

register_all_cmds() ->
    lists:foreach(fun(Cmds) ->
                          [apply(clique, register_command, Cmd) || Cmd <- Cmds]
                  end,
                  [
                   txn_add_gateway_cmd(),
                   txn_assert_location_cmd(),
                   txn_queue_cmd(),
                   txn_cmd()
                  ]).

%%--------------------------------------------------------------------
%% txn
%%--------------------------------------------------------------------
txn_usage() ->
    [["txn"],
     ["blockchain txn commands\n\n",
      "  txn queue             - Show enqueued transactions in the txn_queue.\n"
      "  txn add_gateway       - Create an add gateway transaction.\n"
      "  txn assert_location   - Create an assert location transaction.\n"
     ]
    ].

txn_cmd() ->
    [
     [["txn"], [], [], fun(_, _, _) -> usage end]
    ].

%%--------------------------------------------------------------------
%% txn queue
%%--------------------------------------------------------------------
txn_queue_cmd() ->
    [
     [["txn", "queue"], [], [], fun txn_queue/3]
    ].

txn_queue_usage() ->
    [["txn", "queue"],
     ["txn queue\n\n",
      "  Show enqueued transactions for the miner.\n"
     ]
    ].

txn_queue(["txn", "queue"], [], []) ->
    case (catch blockchain_txn_mgr:txn_list()) of
        {'EXIT', _} ->
            [clique_status:text("timeout")];
        [] ->
            [clique_status:text("empty")];
        TxnList ->
            R = format_txn_list(TxnList),
            [clique_status:table(R)]
    end;
txn_queue([], [], []) ->
    usage.

format_txn_list(TxnList) ->
    maps:fold(fun(Txn, TxnData, Acc) ->
                    TxnMod = blockchain_txn:type(Txn),
                    TxnHash = blockchain_txn:hash(Txn),
                    Acceptions = proplists:get_value(acceptions, TxnData, []),
                    Rejections = proplists:get_value(rejections, TxnData, []),
                    RecvBlockHeight = proplists:get_value(recv_block_height, TxnData, undefined),
                      [
                       [{txn_type, atom_to_list(TxnMod)},
                       {txn_hash, io_lib:format("~p", [libp2p_crypto:bin_to_b58(TxnHash)])},
                       {acceptions, length(Acceptions)},
                       {rejections, length(Rejections)},
                       {accepted_block_height, RecvBlockHeight}]
                      | Acc]
              end, [], TxnList).

%%
%% txn add_gateway
%%
txn_add_gateway_cmd() ->
    [
     [["txn", "add_gateway"],
      [       
        {owner, [{shortname, "o"}, {longname, "owner"},
                {datatype, string}, {validator, fun validate_b58/1}]}
      ],
      [
       {payer, [{longname, "payer"},
                {datatype, string}, {validator, fun validate_b58/1}]},
       {fee, [{longname, "fee"},
                {datatype, integer}, {validator, fun validate_pos/1}]},
       {staking_fee, [{longname, "staking_fee"},
                {datatype, integer}, {validator, fun validate_pos/1}]}
      ],
      fun txn_add_gateway/3]
    ].

txn_add_gateway_usage() ->
    [["txn", "add_gateway"],
     ["txn add_gateway owner=<owner> [--payer <payer>] [--fee <fee>] [--staking_fee <staking_fee>]\n\n",
      "  Creates a signed add gateway transaction required to add a new Gateway to the Helium network.\n"
      "  Requires an owner address. Optionally takes a payer address if the payer of the cost and fee is \n"
      "  not the same as the owner. Also optionally takes the transaction and staking fees (in DC) if the \n"
      "  caller knows these ahead of time.\n\n"
      "  Returns a Base64 encoded transaction to be used as an input to either the\n"
      "  Helium mobile application or the wallet CLI for signing by the owner, and payer if provided, and \n"
      "  final submission to the blockchain.\n\n"
      "Required:\n\n"
      "  <owner>\n"
      "    The b58 address of the owner of the gateway to be added.\n\n"
      "Options:\n\n"
      "  --payer <address>\n",
      "    The b58 address of the payer of the fees. Defaults to the provided owner address\n"
      "  --staking_fee <dc_staking_fee>\n",
      "    The transaction fee in DC. Defaults to a chain computed staking fee\n"
      "  --fee <dc_fee>\n",
      "    The transaction fee in DC. Defaults to a chain computed fee\n"
     ]
    ].

txn_add_gateway(_CmdBase, [], _) ->
    usage;
txn_add_gateway(_CmdBase, Keys, Flags) ->
    try
        %% Get key arguments
        Owner = proplists:get_value(owner, Keys),
        %% Get options
        Payer = proplists:get_value(payer, Flags, undefined),
        StakingFee = proplists:get_value(staking_fee, Flags, undefined),
        Fee = proplists:get_value(fee, Flags, undefined),

        {ok, TxnBin} = blockchain:add_gateway_txn(Owner, Payer, Fee, StakingFee),
        TxnB64 = base64:encode_to_string(TxnBin),
        print_txn_result(TxnB64)
    catch
        What:Why:Stack ->
            Fmt = io_lib:format("error: ~p~n", [{What, Why, Stack}]),
            [clique_status:alert([clique_status:text(Fmt)])]
    end.

%%
%% txn assert_location
%%

txn_assert_location_cmd() ->
    [
     [["txn", "assert_location"],
      [
       {location, [{longname, "location"},
                   {typecast, fun list_to_location/1}, {validator, fun validate_location/1}]},
       {owner, [{longname, "owner"},
                {datatype, string}, {validator, fun validate_b58/1}]}
      ],
      [
       {payer, [{longname, "payer"},
                {datatype, string}, {validator, fun validate_b58/1}]},
       {nonce, [{longname, "nonce"},
                {datatype, integer}, {validator, fun validate_pos/1}]}
      ],
      fun txn_assert_location/3]
    ].

txn_assert_location_usage() ->
    [["txn", "assert_location"],
     ["txn assert_location owner=<owner> location=<location> [--payer <payer>] [--nonce <nonce]\n\n",
      "  Creates a signed location assertion required to declare the location of a Gateway on the Helium network.\n"
      "  Requires an owner address and location (in lat,lon or h3 form).\n"
      "  Optionally takes a payer address if the payer of the cost and fee is not the same as the owner.\n"
      "  Optionally takes a nonce.\n\n"
      "  Returns a Base64 encoded transaction to be used as an input to either the\n"
      "  Helium mobile application or the wallet CLI for signing by the owner, and payer if provided, and \n"
      "  final submission to the blockchain.\n\n"
      "Required:\n\n"
      "  <owner>\n"
      "    The b58 address of the owner of the gateway to be asserted.\n"
      "  <location>\n"
      "    Either a h3 index or a comma separated geo location of the form <lat>,<lon>\n\n"
      "Options:\n\n"
      "  --payer <address>\n"
      "    The b58 address of the payer of the fees. Defaults to the provided owner address\n"
      "  --nonce <nonce>\n"
      "    The assert_location nonce use for the transaction. (default 1)\n"
     ]
    ].

txn_assert_location(_CmdBase, [], _) ->
    usage;
txn_assert_location(_CmdBase, Keys, Flags) ->
    try
        %% Get keys
        Owner = proplists:get_value(owner, Keys),
        H3String = proplists:get_value(location, Keys),
        %% Get options
        Payer = proplists:get_value(payer, Flags, undefined),
        Nonce = proplists:get_value(nonce, Flags, 1),
        %% Construct txn and encode as b64
        {ok, TxnBin} = blockchain:assert_loc_txn(H3String, Owner, Payer, Nonce),
        TxnB64 = base64:encode_to_string(TxnBin),
        print_txn_result(TxnB64)
    catch
        What:Why:Stack ->
            Fmt = io_lib:format("error: ~p~n", [{What, Why, Stack}]),
            [clique_status:alert([clique_status:text(Fmt)])]
    end.


%%
%% Validators/Typecasts
%%

validate_pos(N) when N > 0 ->
    ok;
validate_pos(N) ->
    {error, {invalid_value, N}}.

validate_b58(Str) ->
    try
        libp2p_crypto:b58_to_bin(Str),
        ok
    catch
        _:_ ->
            {error, {invalid_value, Str}}
    end.

list_to_location(Str) ->
    try string:split(Str, ",") of
        [LatStr, LonStr] ->
            Lat = list_to_float(LatStr),
            Lon = list_to_float(LonStr),
            h3:to_string(h3:from_geo({Lat, Lon}, 12));
        [Str] ->
            h3:from_string(Str),
            Str;
         _ ->
            {error, {invalid_value, Str}}
    catch
        _:_ ->
            {error, {invalid_value, Str}}
    end.

validate_location(Str) ->
    case list_to_location(Str) of
        {error, Error} ->
            {error, Error};
        _ ->
            ok
    end.

print_txn_result(V) ->
    [clique_status:text(V)].
