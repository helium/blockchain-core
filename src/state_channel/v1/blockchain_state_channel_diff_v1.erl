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
-type entry() :: #blockchain_state_channel_diff_entry_v1_pb{}.
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
) -> [entry()].
calculate_diffs(OldSC, NewSC) ->
    OldSumMap = summaries_to_map(blockchain_state_channel_v1:summaries(OldSC)),
    NewSummaries = blockchain_state_channel_v1:summaries(NewSC),
    {Appends, Updates} = lists:foldl(
        fun({Index, Summary}, {Appends, Updates}) ->
            HotspotID = ?SUM_CLIENT(Summary),
            case maps:get(HotspotID, OldSumMap, undefined) of
                undefined ->
                    Entry = new_append_entry(
                        HotspotID,
                        ?SUM_PACKETS(Summary),
                        ?SUM_DCS(Summary)
                    ),
                    {[Entry|Appends], Updates};
                {OldNumPackets, OldNumDCs} ->
                    Entry = new_add_entry(
                        Index,
                        ?SUM_PACKETS(Summary)-OldNumPackets,
                        ?SUM_DCS(Summary)-OldNumDCs
                    ),
                    {Appends, [Entry|Updates]}
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

-spec new_append_entry(
    CPubKeyBin ::libp2p_crypto:pubkey_bin(), NumP :: pos_integer(), NumD :: pos_integer()
) -> entry().
new_append_entry(CPubKeyBin, NumP, NumD) ->
    Append = #blockchain_state_channel_diff_append_summary_v1_pb{
        client_pubkeybin=CPubKeyBin,
        num_packets=NumP,
        num_dcs=NumD
    },
    #blockchain_state_channel_diff_entry_v1_pb{
        entry={append, Append}
    }.

-spec new_add_entry(
    Index ::pos_integer(), AddP :: pos_integer(), AddD :: pos_integer()
) -> entry().
new_add_entry(Index, AddP, AddD) ->
    Add = #blockchain_state_channel_diff_update_summary_v1_pb{
        client_index=Index,
        add_packets=AddP,
        add_dcs=AddD
    },
    #blockchain_state_channel_diff_entry_v1_pb{
        entry={add, Add}
    }.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

new_test() ->
    SCID = crypto:strong_rand_bytes(32),
    Owner = crypto:strong_rand_bytes(32),
    OldActors = [crypto:strong_rand_bytes(32), crypto:strong_rand_bytes(32)],
    OldSummaries = lists:map(
        fun(Actor) ->
            blockchain_state_channel_summary_v1:new(Actor, 1, 1)
        end,
        OldActors
    ),
    OldSC0 = blockchain_state_channel_v1:new(SCID, Owner, 100),
    OldSC1 = blockchain_state_channel_v1:summaries(OldSummaries, OldSC0),
    OldSC2 = blockchain_state_channel_v1:nonce(2, OldSC1),

    %% We are adding 2 new actors and updating the 2 current ones so nonce+4
    NewActors = [crypto:strong_rand_bytes(32), crypto:strong_rand_bytes(32)],
    NewSummaries = lists:map(
        fun(Actor) ->
            blockchain_state_channel_summary_v1:new(Actor, 2, 2)
        end,
        OldActors ++ NewActors
    ),
    NewSC0 = blockchain_state_channel_v1:new(SCID, Owner, 100),
    NewSC1 = blockchain_state_channel_v1:summaries(NewSummaries, NewSC0),
    NewSC2 = blockchain_state_channel_v1:nonce(6, NewSC1),

    ExpectedDiff = #blockchain_state_channel_diff_v1_pb{
        id = SCID,
        add_nonce = 4,
        diffs = [
            new_add_entry(1, 1, 1),
            new_add_entry(0, 1, 1),
            new_append_entry(lists:nth(1, NewActors), 2, 2),
            new_append_entry(lists:nth(2, NewActors), 2, 2)
        ]
    },

    ?assertEqual(ExpectedDiff, ?MODULE:new(OldSC2, NewSC2)),
    ok.

-endif.
