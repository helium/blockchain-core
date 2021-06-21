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
    case (catch blockchain_state_channels_server:active_sc_ids()) of
        {'EXIT', _} ->
            [clique_status:text("timeout")];
        undefined ->
            [clique_status:text("none")];
        BinActiveIDs ->
            R = [format_sc_id(ID) || ID <- BinActiveIDs],
            [clique_status:text(io_lib:format("~p", [R]))]
    end;
sc_active([], [], []) ->
    usage.

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
    Chain = blockchain_worker:blockchain(),
    {ok, Height} = blockchain:height(Chain),
    maps:fold(
        fun(SCID, {SC, _}, Acc) ->
            ID = format_sc_id(SCID),
            SCNonce = blockchain_state_channel_v1:nonce(SC),
            Amount = blockchain_state_channel_v1:amount(SC),
            RootHash = erlang:binary_to_list(base64:encode(blockchain_state_channel_v1:root_hash(SC))),
            State =  erlang:atom_to_list(blockchain_state_channel_v1:state(SC)),
            {NumDCs, NumPackets, NumParticipants} = summarize(blockchain_state_channel_v1:summaries(SC)),
            ExpireAtBlock = blockchain_state_channel_v1:expire_at_block(SC),
            IsActive = is_active(SC),
            MAxP = blockchain_ledger_v1:get_sc_max_actors(blockchain:ledger(Chain)),
            [
                [
                    {id, io_lib:format("~p", [ID])},
                    {nonce, io_lib:format("~p", [SCNonce])},
                    {state, io_lib:format("~p", [State])},
                    {is_active, io_lib:format("~p", [IsActive])},
                    {expire_at, io_lib:format("~p", [ExpireAtBlock])},
                    {expired, ExpireAtBlock =< Height},
                    {amount, Amount},
                    {num_dcs, NumDCs},
                    {num_packets, NumPackets},
                    {participants, NumParticipants},
                    {max_participants, MAxP},
                    {root_hash, io_lib:format("~p", [RootHash])}
                ]
                | Acc
            ]
        end,
        [],
        SCs
    ).

summarize(Summaries) ->
    lists:foldl(fun(Summary, {DCs, Packets, Participants}) ->
                        NumDCs = blockchain_state_channel_summary_v1:num_dcs(Summary),
                        NumPackets = blockchain_state_channel_summary_v1:num_packets(Summary),
                        {DCs + NumDCs, Packets + NumPackets, Participants+1}
                end,
                {0, 0, 0}, Summaries).

is_active(SC) ->
    ActiveSCIDs = blockchain_state_channels_server:active_sc_ids(),
    SCID = blockchain_state_channel_v1:id(SC),
    lists:member(SCID, ActiveSCIDs).

format_sc_id(ID) ->
    blockchain_utils:addr2name(ID).