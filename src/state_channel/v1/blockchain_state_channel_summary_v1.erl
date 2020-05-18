%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel Summary ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channel_summary_v1).

-behavior(blockchain_json).
-include("blockchain_json.hrl").

-export([
    new/1, new/3,
    client_pubkeybin/1,
    num_dcs/1, num_dcs/2,
    num_packets/1, num_packets/2,
    update/3, validate/1,
    to_json/2
]).

-include_lib("helium_proto/include/blockchain_state_channel_v1_pb.hrl").

-type summary() :: #blockchain_state_channel_summary_v1_pb{}.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-spec new(ClientPubkeyBin :: libp2p_crypto:pubkey_bin()) -> summary().
new(ClientPubkeyBin) ->
    #blockchain_state_channel_summary_v1_pb{
       client_pubkeybin=ClientPubkeyBin,
       num_dcs=0,
       num_packets=0
    }.

-spec new(ClientPubkeyBin :: libp2p_crypto:pubkey_bin(),
          NumPackets :: non_neg_integer(),
          NumDCs :: non_neg_integer()) -> summary().
new(ClientPubkeyBin, NumPackets, NumDCs) ->
    #blockchain_state_channel_summary_v1_pb{
       client_pubkeybin=ClientPubkeyBin,
       num_dcs=NumDCs,
       num_packets=NumPackets
    }.

-spec client_pubkeybin(Summary :: summary()) -> libp2p_crypto:pubkey_bin().
client_pubkeybin(#blockchain_state_channel_summary_v1_pb{client_pubkeybin=ClientPubkeyBin}) ->
    ClientPubkeyBin.

-spec num_packets(Summary :: summary()) -> non_neg_integer().
num_packets(#blockchain_state_channel_summary_v1_pb{num_packets=NumPackets}) ->
    NumPackets.

-spec num_packets(NumPackets :: non_neg_integer(),
                  Summary :: summary()) -> summary().
num_packets(NumPackets, Summary) ->
    Summary#blockchain_state_channel_summary_v1_pb{num_packets=NumPackets}.

-spec num_dcs(Summary :: summary()) -> non_neg_integer().
num_dcs(#blockchain_state_channel_summary_v1_pb{num_dcs=NumDCs}) ->
    NumDCs.

-spec num_dcs(NumDCs :: non_neg_integer(),
              Summary :: summary()) -> summary().
num_dcs(NumDCs, Summary) ->
    Summary#blockchain_state_channel_summary_v1_pb{num_dcs=NumDCs}.

-spec update(NumDCs :: non_neg_integer(),
             NumPackets :: non_neg_integer(),
             Summary :: summary()) -> summary().
update(NumDCs, NumPackets, Summary) ->
    Summary#blockchain_state_channel_summary_v1_pb{num_dcs=NumDCs, num_packets=NumPackets}.

validate(Summary) ->
    try libp2p_crypto:bin_to_pubkey(client_pubkeybin(Summary)) of
        _ ->
            case num_dcs(Summary) >= num_packets(Summary) of
                true ->
                    case num_packets(Summary) > 0 of
                        true ->
                            ok;
                        false ->
                            {error, zero_packet_summary}
                    end;
                false ->
                    {error, more_packets_than_dcs_in_summary}
            end
    catch
        _:_ ->
            {error, invalid_address_in_summary}
    end.


-spec to_json(summary(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Summary, _Opts) ->
    #{
       client => ?BIN_TO_B58(client_pubkeybin(Summary)),
       num_dcs => num_dcs(Summary),
       num_packets => num_packets(Summary)
     }.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    ClientPubkeyBin = <<"client">>,
    Summary = new(ClientPubkeyBin),
    Expected = #blockchain_state_channel_summary_v1_pb{client_pubkeybin=ClientPubkeyBin,
                                                       num_dcs=0,
                                                       num_packets=0},
    ?assertEqual(Expected, Summary).

new2_test() ->
    ClientPubkeyBin = <<"client">>,
    Summary = new(ClientPubkeyBin, 10, 10),
    Expected = #blockchain_state_channel_summary_v1_pb{client_pubkeybin=ClientPubkeyBin,
                                                       num_dcs=10,
                                                       num_packets=10},
    ?assertEqual(Expected, Summary).

client_pubkeybin_test() ->
    ClientPubkeyBin = <<"client">>,
    Summary = new(ClientPubkeyBin, 10, 10),
    ?assertEqual(ClientPubkeyBin, client_pubkeybin(Summary)).

num_packets_test() ->
    ClientPubkeyBin = <<"client">>,
    Summary = new(ClientPubkeyBin, 10, 10),
    ?assertEqual(10, num_packets(Summary)).

num_dcs_test() ->
    ClientPubkeyBin = <<"client">>,
    Summary = new(ClientPubkeyBin, 10, 10),
    ?assertEqual(10, num_dcs(Summary)).

set_num_packets_test() ->
    ClientPubkeyBin = <<"client">>,
    Summary = new(ClientPubkeyBin, 10, 10),
    ?assertEqual(20, num_packets(num_packets(20, Summary))).

set_num_dcs_test() ->
    ClientPubkeyBin = <<"client">>,
    Summary = new(ClientPubkeyBin, 10, 10),
    ?assertEqual(20, num_dcs(num_dcs(20, Summary))).

update_test() ->
    ClientPubkeyBin = <<"client">>,
    Summary = new(ClientPubkeyBin, 10, 10),
    ?assertEqual(ClientPubkeyBin, client_pubkeybin(Summary)),
    ?assertEqual(10, num_dcs(Summary)),
    ?assertEqual(10, num_packets(Summary)),
    UpdatedSummary = update(20, 20, Summary),
    ?assertEqual(ClientPubkeyBin, client_pubkeybin(UpdatedSummary)),
    ?assertEqual(20, num_dcs(UpdatedSummary)),
    ?assertEqual(20, num_packets(UpdatedSummary)).

to_json_test() ->
    ClientPubkeyBin = <<"client">>,
    Summary = new(ClientPubkeyBin, 10, 10),
    Json = to_json(Summary, []),
    ?assert(lists:all(fun(K) -> maps:is_key(K, Json) end,
                      [client, num_dcs, num_packets])).


-endif.
