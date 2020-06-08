%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain CLI SC ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_cli_sc).

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
                   sc_active_usage(),
                   sc_list_usage(),
                   sc_usage()
                  ]).

register_all_cmds() ->
    lists:foreach(fun(Cmds) ->
                          [apply(clique, register_command, Cmd) || Cmd <- Cmds]
                  end,
                  [
                   sc_active_cmd(),
                   sc_list_cmd(),
                   sc_cmd()
                  ]).

%%--------------------------------------------------------------------
%% sc
%%--------------------------------------------------------------------
sc_usage() ->
    [["sc"],
     ["blockchain state channel commands\n\n",
      "  sc active             - Show currently active state channel id (base64).\n"
      "  sc list               - Show list of currently active state channels.\n"
     ]
    ].

sc_cmd() ->
    [
     [["sc"], [], [], fun(_, _, _) -> usage end]
    ].

%%--------------------------------------------------------------------
%% sc active
%%--------------------------------------------------------------------
sc_active_cmd() ->
    [
     [["sc", "active"], [], [], fun sc_active/3]
    ].

sc_active_usage() ->
    [["sc", "active"],
     ["sc active\n\n",
      "  Show currently active state channel id (base64).\n"
     ]
    ].

sc_active(["sc", "active"], [], []) ->
    case (catch blockchain_state_channels_server:active_sc_id()) of
        {'EXIT', _} ->
            [clique_status:text("timeout")];
        undefined ->
            [clique_status:text("none")];
        BinActiveID ->
            R = format_active_id(BinActiveID),
            [clique_status:text(io_lib:format("~p", [R]))]
    end;
sc_active([], [], []) ->
    usage.

format_active_id(BinActiveID) ->
    binary_to_list(base64:encode(BinActiveID)).

%%--------------------------------------------------------------------
%% sc list
%%--------------------------------------------------------------------
sc_list_cmd() ->
    [
     [["sc", "list"], [], [], fun sc_list/3]
    ].

sc_list_usage() ->
    [["sc", "list"],
     ["sc list\n\n",
      "  Show list of currently active state channels.\n"
     ]
    ].

sc_list(["sc", "list"], [], []) ->
    case (catch blockchain_state_channels_server:state_channels()) of
        {'EXIT', _} ->
            [clique_status:text("timeout")];
        M when map_size(M) == 0 ->
            [clique_status:text("none")];
        SCs ->
            R = format_sc_list(SCs),
            [clique_status:table(R)]
    end;
sc_list([], [], []) ->
    usage.

format_sc_list(SCs) ->
    maps:fold(fun(SCID, {SC, _}, Acc) ->
                      ID = binary_to_list(base64:encode(SCID)),
                      {ok, SCOwnerName} = erl_angry_purple_tiger:animal_name(libp2p_crypto:bin_to_b58(blockchain_state_channel_v1:owner(SC))),
                      SCNonce = blockchain_state_channel_v1:nonce(SC),
                      RootHash = binary_to_list(base64:encode(blockchain_state_channel_v1:root_hash(SC))),
                      State = atom_to_list(blockchain_state_channel_v1:state(SC)),
                      Summaries = format_sc_summaries(blockchain_state_channel_v1:summaries(SC)),
                      ExpireAtBlock = blockchain_state_channel_v1:expire_at_block(SC),
                      IsActive = is_active(SC),
                      [
                       [{id, io_lib:format("~p", [ID])},
                        {owner, io_lib:format("~p", [SCOwnerName])},
                        {nonce, io_lib:format("~p", [SCNonce])},
                        {state, io_lib:format("~p", [State])},
                        {is_active, io_lib:format("~p", [IsActive])},
                        {root_hash, io_lib:format("~p", [RootHash])},
                        {expire_at, io_lib:format("~p", [ExpireAtBlock])},
                        {summaries, io_lib:format("~p", [Summaries])}
                       ] | Acc]
              end, [], SCs).

format_sc_summaries(Summaries) ->
    lists:foldl(fun(Summary, Acc) ->
                        {ok, ClientName} = erl_angry_purple_tiger:animal_name(libp2p_crypto:bin_to_b58(blockchain_state_channel_summary_v1:client_pubkeybin(Summary))),
                        NumDCs = blockchain_state_channel_summary_v1:num_dcs(Summary),
                        NumPackets = blockchain_state_channel_summary_v1:num_packets(Summary),
                        [ {receiver, ClientName},
                          {num_dcs, NumDCs},
                          {num_packets, NumPackets}
                          | Acc]
                end,
                [], Summaries).

is_active(SC) ->
    ActiveSCID = blockchain_state_channels_server:active_sc_id(),
    SCID = blockchain_state_channel_v1:id(SC),
    ActiveSCID == SCID.
