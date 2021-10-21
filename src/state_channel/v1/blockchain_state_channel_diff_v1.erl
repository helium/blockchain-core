%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel Diff ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channel_diff_v1).

-export([
    new/2,
    sign/2,
    encode/1, decode/1
]).

-include("blockchain.hrl").
-include_lib("helium_proto/include/blockchain_state_channel_v1_pb.hrl").

-define(SUM_CLIENT(Sum), blockchain_state_channel_summary_v1:client_pubkeybin(Sum)).
-define(SUM_DCS(Sum), blockchain_state_channel_summary_v1:num_dcs(Sum)).
-define(SUM_PACKETS(Sum), blockchain_state_channel_summary_v1:num_packets(Sum)).

-type diff() :: #blockchain_state_channel_diff_v1_pb{}.
-export_type([diff/0]).

-spec new(
    OldSC :: blockchain_state_channel_v1:state_channel(),
    NewSC :: blockchain_state_channel_v1:state_channel()
) -> diff().
new(OldSC, NewSC) ->
    SCID = blockchain_state_channel_v1:id(NewSC),
    case blockchain_state_channel_v1:id(OldSC) == SCID of
        false ->
            erlang:throw(bad_id);
        true ->
            #blockchain_state_channel_diff_v1_pb{
                id = SCID,
                add_nonce = blockchain_state_channel_v1:nonce(NewSC) - blockchain_state_channel_v1:nonce(OldSC),
                diffs = calculate_diffs(OldSC, NewSC)
            }
    end.

-spec sign(diff(), function()) -> diff().
sign(Diff, SigFun) ->
    EncodedDiff = ?MODULE:encode(Diff#blockchain_state_channel_diff_v1_pb{signature= <<>>}),
    Signature = SigFun(EncodedDiff),
    Diff#blockchain_state_channel_diff_v1_pb{signature=Signature}.

-spec encode(diff()) -> binary().
encode(#blockchain_state_channel_diff_v1_pb{}=Diff) ->
    blockchain_state_channel_v1_pb:encode_msg(Diff).

-spec decode(binary()) -> diff().
decode(Binary) ->
    blockchain_state_channel_v1_pb:decode_msg(Binary, blockchain_state_channel_diff_v1_pb).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

-spec calculate_diffs(
    OldSC :: blockchain_state_channel_v1:state_channel(),
    NewSC :: blockchain_state_channel_v1:state_channel()
) -> list().
calculate_diffs(OldSC, NewSC) ->
    OldSumMap = summaries_to_map(blockchain_state_channel_v1:summaries(OldSC)),
    NewSummaries = blockchain_state_channel_v1:summaries(NewSC),
    {Appends, Updates} = lists:foldl(
        fun({Index, Summary}, {Appends, Updates}) ->
            HotspotID = ?SUM_CLIENT(Summary),
            case maps:get(HotspotID, OldSumMap, undefined) of
                undefined ->
                    Append = #blockchain_state_channel_diff_append_summary_v1_pb{
                        client_pubkeybin=HotspotID,
                        num_packets=?SUM_PACKETS(Summary),
                        num_dcs=?SUM_DCS(Summary)
                    },
                    [{append, Append}|Appends];
                {OldNumPackets, OldNumDCs} ->
                    Update = #blockchain_state_channel_diff_update_summary_v1_pb{
                        client_index=Index,
                        add_packets=?SUM_PACKETS(Summary)-OldNumPackets,
                        add_dcs=?SUM_DCS(Summary)-OldNumDCs
                    },
                    [{add, Update}|Updates]
            end
        end,
        {[], []},
        lists:zip(lists:seq(0, erlang:length(NewSummaries)-1), NewSummaries)
    ),
    Updates ++ lists:reverse(Appends).

-spec summaries_to_map(Summaries :: [blockchain_state_channel_summary_v1:summary()]) ->
    #{libp2p_crypto:pubkey_bin() => {non_neg_integer(), non_neg_integer()}}.
summaries_to_map(Summaries) ->
    lists:foldl(
        fun(Summary, Acc) ->
            maps:put(?SUM_CLIENT(Summary), {?SUM_PACKETS(Summary), ?SUM_DCS(Summary)}, Acc )
        end,
        #{},
        Summaries
    ).

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").


-endif.
