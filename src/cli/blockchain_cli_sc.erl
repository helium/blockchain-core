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
    lists:foreach(
        fun(Args) ->
            apply(clique, register_usage, Args)
        end,
        [
            sc_active_usage(),
            sc_list_usage(),
            sc_hotspot_cmd_usage(),
            sc_usage()
        ]
    ).

register_all_cmds() ->
    lists:foreach(
        fun(Cmds) ->
            [apply(clique, register_command, Cmd) || Cmd <- Cmds]
        end,
        [
            sc_active_cmd(),
            sc_list_cmd(),
            sc_hotspot_cmd(),
            sc_cmd()
        ]
    ).

%%--------------------------------------------------------------------
%% sc
%%--------------------------------------------------------------------
sc_usage() ->
    [
        ["sc"],
        ["blockchain state channel commands\n\n",
            "  sc active                 - Show currently active state channel id (base64).\n"
            "  sc list                   - Show list of currently active state channels.\n"
            "  sc hotspot <hotspot name> - Show list of state channels where hotspot is in.\n"
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
    [
        ["sc", "active"],
        ["sc active\n\n",
            "  Show currently active state channel id (base64).\n"
        ]
    ].

sc_active(["sc", "active"], [], []) ->
    case (catch maps:keys(blockchain_state_channels_server:get_actives())) of
        {'EXIT', _} ->
            [clique_status:text("timeout")];
        undefined ->
            [clique_status:text("none")];
        Actives ->
            R = [blockchain_state_channel_v1:name(SC) || {SC, _, _} <- maps:values(Actives)],
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
    [
        ["sc", "list"],
        ["sc list\n\n",
            "  Show list of currently active state channels.\n"
        ]
    ].

sc_list(["sc", "list"], [], []) ->
    case (catch blockchain_state_channels_server:get_all()) of
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
    {List, Total} = maps:fold(
        fun(_SCID, {SC, SCState, Pid}, {Acc, {TActive0, TExpired0, TAmount0, TDCs0, TPackets0, TActors0, TMax0}}) ->
            ID = blockchain_state_channel_v1:name(SC),
            SCNonce = blockchain_state_channel_v1:nonce(SC),
            Amount = blockchain_state_channel_v1:amount(SC),
            State =  erlang:atom_to_list(SCState),
            {NumDCs, NumPackets, NumParticipants} = summarize(blockchain_state_channel_v1:summaries(SC)),
            ExpireAtBlock = blockchain_state_channel_v1:expire_at_block(SC),
            IsActive = SCState == active,
            MAxP = blockchain_ledger_v1:get_sc_max_actors(blockchain:ledger(Chain)),
            TActive1 = case IsActive of
                true -> TActive0 + 1;
                false -> TActive0
            end,
            Expired = ExpireAtBlock =< Height,
            TExpired1 = case Expired of
                true -> TExpired0 + 1;
                false -> TExpired0
            end,
            {
                [
                    [
                        {id, io_lib:format("~p", [ID])},
                        {nonce, io_lib:format("~p", [SCNonce])},
                        {state, io_lib:format("~p", [State])},
                        {is_active, io_lib:format("~p", [IsActive])},
                        {expire_in, ExpireAtBlock - Height},
                        {expire_at, io_lib:format("~p", [ExpireAtBlock])},
                        {expired, ExpireAtBlock =< Height},
                        {amount, Amount},
                        {num_dcs, NumDCs},
                        {num_packets, NumPackets},
                        {participants, NumParticipants},
                        {max_participants, MAxP},
                        {pid, io_lib:format("~p", [Pid])}
                    ]
                    | Acc
                ],
                {
                    TActive1,
                    TExpired1,
                    TAmount0+Amount,
                    TDCs0+NumDCs,
                    TPackets0+NumPackets,
                    TActors0+NumParticipants,
                    TMax0+MAxP
                }
            }
        end,
        {[], {0, 0, 0, 0, 0, 0, 0}},
        SCs
    ),
    {TActive, TExpired, TAmount, TDCs, TPackets, TActors, TMax} = Total,
    SortedList =
        lists:sort(
            fun(A, B) ->
                proplists:get_value(expire_at, A) < proplists:get_value(expire_at, B)
            end,
            List
        ),
    [
        [
            {id, "Total"},
            {nonce, "X"},
            {state, "X"},
            {is_active, TActive},
            {expire_in, "X"},
            {expire_at, "X"},
            {expired, TExpired},
            {amount, TAmount},
            {num_dcs, TDCs},
            {num_packets, TPackets},
            {participants, TActors},
            {max_participants, TMax},
            {pid, "X"}
        ]
        | SortedList
    ].

%%--------------------------------------------------------------------
%% sc hotspot
%%--------------------------------------------------------------------

sc_hotspot_cmd() ->
    [
        [["sc", "hotspot", '*'], [], [], fun sc_hotspot/3]
    ].

sc_hotspot_cmd_usage() ->
    [   
        ["sc", "hotspot"],
        ["sc hotspot <hotspot name>\n\n",
            "  Show list of state channels where hotspot is in.\n"
        ]
    ].

sc_hotspot(["sc", "hotspot", HotspotName], [], []) ->
    case (catch get_sc_for_hotspot(HotspotName)) of
        {'EXIT', _} ->
            [clique_status:text("timeout")];
        not_found ->
            [clique_status:text("not_found")];
        List ->
            [clique_status:table(List)]
    end;
sc_hotspot([], [], []) ->
    usage.

get_sc_for_hotspot(HotspotName) ->
    SCs = blockchain_state_channels_server:get_all(),
    case
        lists:filtermap(
            fun({SC, SCState, Pid}) ->
                case
                    lists:filtermap(
                        fun(Summary) ->
                            PubKeyBin = blockchain_state_channel_summary_v1:client_pubkeybin(Summary),
                            case blockchain_utils:addr2name(PubKeyBin) == HotspotName of
                                true -> {true, PubKeyBin};
                                false -> false
                            end
                        end,
                        blockchain_state_channel_v1:summaries(SC)
                    )
                of
                    [] ->
                        false;
                    [PubKeyBin] ->
                        {true, {SC, SCState, Pid, PubKeyBin}}
                end
            end,
            maps:values(SCs)
        )
    of
        [] ->
            not_found;
        Found ->
            Chain = blockchain_worker:blockchain(),
            {ok, Height} = blockchain:height(Chain),
            lists:map(
                fun({SC, SCState, Pid, PubKeyBin}) ->
                    {ok, Summary} = blockchain_state_channel_v1:get_summary(PubKeyBin, SC),
                    ExpireAtBlock = blockchain_state_channel_v1:expire_at_block(SC),
                    Amount = blockchain_state_channel_v1:amount(SC),
                    {NumDCs, _NumPackets, NumParticipants} = summarize(blockchain_state_channel_v1:summaries(SC)),
                    MAxP = blockchain_ledger_v1:get_sc_max_actors(blockchain:ledger(Chain)),
                    [
                        {hotspot_name, HotspotName},
                        {hotspot_b58, libp2p_crypto:bin_to_b58(PubKeyBin)},
                        {hotspot_num_dcs, blockchain_state_channel_summary_v1:num_dcs(Summary)},
                        {hotspot_num_packets, blockchain_state_channel_summary_v1:num_packets(Summary)},
                        {sc_name, blockchain_state_channel_v1:name(SC)},
                        {sc_state, SCState},
                        {sc_expire_in, ExpireAtBlock - Height},
                        {sc_dc_left, Amount-NumDCs},
                        {sc_participants_left, MAxP-NumParticipants},
                        {sc_pid, io_lib:format("~p", [Pid])}
                    ]
                end,
                Found
            )
    end.

summarize(Summaries) ->
    lists:foldl(
        fun(Summary, {DCs, Packets, Participants}) ->
            NumDCs = blockchain_state_channel_summary_v1:num_dcs(Summary),
            NumPackets = blockchain_state_channel_summary_v1:num_packets(Summary),
            {DCs + NumDCs, Packets + NumPackets, Participants+1}
        end,
        {0, 0, 0},
        Summaries
    ).
