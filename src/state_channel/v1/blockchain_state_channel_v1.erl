%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channel_v1).

-behavior(blockchain_json).
-include("blockchain_json.hrl").

-export([
    new/3, new/5,
    id/1,
    owner/1,
    nonce/1, nonce/2,
    amount/1, amount/2,
    root_hash/1, root_hash/2,
    state/1, state/2,
    expire_at_block/1, expire_at_block/2,
    signature/1, sign/2, validate/1,
    encode/1, decode/1,
    save/3, fetch/2,
    summaries/1, summaries/2, update_summary_for/3,

    add_payload/3,
    get_summary/2,
    num_packets_for/2, num_dcs_for/2,
    total_packets/1, total_dcs/1,

    to_json/2,

    compare_causality/2,
    merge/2
]).

-include_lib("helium_proto/include/blockchain_state_channel_v1_pb.hrl").

-define(MAX_UNIQ_CLIENTS, 1000).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type state_channel() :: #blockchain_state_channel_v1_pb{}.
-type id() :: binary().
-type state() :: open | closed.
-type summaries() :: [blockchain_state_channel_summary_v1:summary()].
-type temporal_relation() :: equal | effect_of | caused | conflict.

-export_type([state_channel/0, id/0]).

-spec new(ID :: id(),
          Owner :: libp2p_crypto:pubkey_bin(),
          Amount :: non_neg_integer()) -> state_channel().
new(ID, Owner, Amount) ->
    #blockchain_state_channel_v1_pb{
        id=ID,
        owner=Owner,
        nonce=0,
        credits=Amount,
        summaries=[],
        root_hash= <<>>,
        state=open,
        expire_at_block=0
    }.

-spec new(ID :: id(),
          Owner :: libp2p_crypto:pubkey_bin(),
          Amount :: non_neg_integer(),
          BlockHash :: binary(),
          ExpireAtBlock :: pos_integer()) -> {state_channel(), skewed:skewed()}.
new(ID, Owner, Amount, BlockHash, ExpireAtBlock) ->
    SC = #blockchain_state_channel_v1_pb{
            id=ID,
            owner=Owner,
            nonce=0,
            credits=Amount,
            summaries=[],
            root_hash= <<>>,
            state=open,
            expire_at_block=ExpireAtBlock
           },
    Skewed = skewed:new(BlockHash),
    {SC, Skewed}.

-spec id(state_channel()) -> binary().
id(#blockchain_state_channel_v1_pb{id=ID}) ->
    ID.

-spec owner(state_channel()) -> libp2p_crypto:pubkey_bin().
owner(#blockchain_state_channel_v1_pb{owner=Owner}) ->
    Owner.

-spec nonce(state_channel()) -> non_neg_integer().
nonce(#blockchain_state_channel_v1_pb{nonce=Nonce}) ->
    Nonce.

-spec nonce(non_neg_integer(), state_channel()) -> state_channel().
nonce(Nonce, SC) ->
    SC#blockchain_state_channel_v1_pb{nonce=Nonce}.

-spec amount(state_channel()) -> non_neg_integer().
amount(#blockchain_state_channel_v1_pb{credits=Amount}) ->
    Amount.

-spec amount(non_neg_integer(), state_channel()) -> state_channel().
amount(Amount, SC) ->
    SC#blockchain_state_channel_v1_pb{credits=Amount}.

-spec summaries(state_channel()) -> summaries().
summaries(#blockchain_state_channel_v1_pb{summaries=Summaries}) ->
    Summaries.

-spec summaries(Summaries :: summaries(),
                SC :: state_channel()) -> state_channel().
summaries(Summaries, SC) ->
    SC#blockchain_state_channel_v1_pb{summaries=Summaries}.

-spec update_summary_for(ClientPubkeyBin :: libp2p_crypto:pubkey_bin(),
                         NewSummary :: blockchain_state_channel_summary_v1:summary(),
                         SC :: state_channel()) -> state_channel().
update_summary_for(ClientPubkeyBin, NewSummary, #blockchain_state_channel_v1_pb{summaries=Summaries}=SC) ->
    case get_summary(ClientPubkeyBin, SC) of
        {error, not_found} ->
            SC#blockchain_state_channel_v1_pb{summaries=[NewSummary | Summaries]};
        {ok, _Summary} ->
            case can_fit(ClientPubkeyBin, Summaries) of
                false ->
                    %% Cannot fit this into summaries
                    SC;
                true ->
                    NewSummaries = lists:keyreplace(ClientPubkeyBin,
                                                    #blockchain_state_channel_summary_v1_pb.client_pubkeybin,
                                                    Summaries,
                                                    NewSummary),
                    SC#blockchain_state_channel_v1_pb{summaries=NewSummaries}
            end
    end.

-spec get_summary(ClientPubkeyBin :: libp2p_crypto:pubkey_bin(),
                  SC :: state_channel()) -> {ok, blockchain_state_channel_summary_v1:summary()} |
                                            {error, not_found}.
get_summary(ClientPubkeyBin, #blockchain_state_channel_v1_pb{summaries=Summaries}) ->
    case lists:keyfind(ClientPubkeyBin,
                       #blockchain_state_channel_summary_v1_pb.client_pubkeybin,
                       Summaries) of
        false ->
            {error, not_found};
        Summary ->
            {ok, Summary}
    end.

-spec num_packets_for(ClientPubkeyBin :: libp2p_crypto:pubkey_bin(),
                      SC :: state_channel()) -> {ok, non_neg_integer()} | {error, not_found}.
num_packets_for(ClientPubkeyBin, SC) ->
    case get_summary(ClientPubkeyBin, SC) of
        {error, _}=E -> E;
        {ok, Summary} ->
            {ok, blockchain_state_channel_summary_v1:num_packets(Summary)}
    end.

-spec total_packets(SC :: state_channel()) -> non_neg_integer().
total_packets(#blockchain_state_channel_v1_pb{summaries=Summaries}) ->
    lists:foldl(fun(Summary, Acc) ->
                        Acc + blockchain_state_channel_summary_v1:num_packets(Summary)
                end, 0, Summaries).

-spec num_dcs_for(ClientPubkeyBin :: libp2p_crypto:pubkey_bin(),
                  SC :: state_channel()) -> {ok, non_neg_integer()} | {error, not_found}.
num_dcs_for(ClientPubkeyBin, SC) ->
    case get_summary(ClientPubkeyBin, SC) of
        {error, _}=E -> E;
        {ok, Summary} ->
            {ok, blockchain_state_channel_summary_v1:num_dcs(Summary)}
    end.

-spec total_dcs(SC :: state_channel()) -> non_neg_integer().
total_dcs(#blockchain_state_channel_v1_pb{summaries=Summaries}) ->
    lists:foldl(fun(Summary, Acc) ->
                        Acc + blockchain_state_channel_summary_v1:num_dcs(Summary)
                end, 0, Summaries).

-spec root_hash(state_channel()) -> skewed:hash().
root_hash(#blockchain_state_channel_v1_pb{root_hash=RootHash}) ->
    RootHash.

-spec root_hash(skewed:hash(), state_channel()) -> state_channel().
root_hash(RootHash, SC) ->
    SC#blockchain_state_channel_v1_pb{root_hash=RootHash}.

-spec state(state_channel()) -> state().
state(#blockchain_state_channel_v1_pb{state=State}) ->
    State.

-spec state(state(), state_channel()) -> state_channel().
state(closed, SC) ->
    SC#blockchain_state_channel_v1_pb{state=closed};
state(open, SC) ->
    SC#blockchain_state_channel_v1_pb{state=open}.

-spec expire_at_block(state_channel()) -> pos_integer().
expire_at_block(#blockchain_state_channel_v1_pb{expire_at_block=ExpireAt}) ->
    ExpireAt.

-spec expire_at_block(pos_integer(), state_channel()) -> state_channel().
expire_at_block(ExpireAt, SC) ->
    SC#blockchain_state_channel_v1_pb{expire_at_block=ExpireAt}.

-spec signature(state_channel()) -> binary().
signature(#blockchain_state_channel_v1_pb{signature=Signature}) ->
    Signature.

-spec sign(state_channel(), function()) -> state_channel().
sign(SC, SigFun) ->
    EncodedSC = ?MODULE:encode(SC#blockchain_state_channel_v1_pb{signature= <<>>}),
    Signature = SigFun(EncodedSC),
    SC#blockchain_state_channel_v1_pb{signature=Signature}.

-spec validate(state_channel()) -> ok | {error, any()}.
validate(SC) ->
    BaseSC = SC#blockchain_state_channel_v1_pb{signature = <<>>},
    EncodedSC = ?MODULE:encode(BaseSC),
    Signature = ?MODULE:signature(SC),
    Owner = ?MODULE:owner(SC),
    PubKey = libp2p_crypto:bin_to_pubkey(Owner),
    case libp2p_crypto:verify(EncodedSC, Signature, PubKey) of
        false -> {error, bad_signature};
        true -> validate_summaries(summaries(SC))
    end.

validate_summaries([]) ->
    ok;
validate_summaries([H|T]) ->
    case blockchain_state_channel_summary_v1:validate(H) of
        ok ->
            validate_summaries(T);
        Error ->
            Error
    end.

-spec encode(state_channel()) -> binary().
encode(#blockchain_state_channel_v1_pb{}=SC) ->
    blockchain_state_channel_v1_pb:encode_msg(SC).

-spec decode(binary()) -> state_channel().
decode(Binary) ->
    blockchain_state_channel_v1_pb:decode_msg(Binary, blockchain_state_channel_v1_pb).

-spec save(DB :: rocksdb:db_handle(),
           SC :: state_channel(),
           Skewed :: skewed:skewed()) -> ok.
save(DB, SC, Skewed) ->
    ID = ?MODULE:id(SC),
    lager:info("saving state_channel: ~p", [SC]),
    ok = rocksdb:put(DB, ID, term_to_binary({?MODULE:encode(SC), Skewed}), [{sync, true}]).

-spec fetch(rocksdb:db_handle(), id()) -> {ok, {state_channel(), skewed:skewed()}} | {error, any()}.
fetch(DB, ID) ->
    case rocksdb:get(DB, ID, [{sync, true}]) of
        {ok, Bin} ->
            {BinarySC, Skewed} = binary_to_term(Bin),
            SC = ?MODULE:decode(BinarySC),
            {ok, {SC, Skewed}};
        not_found -> {error, not_found};
        Error -> Error
    end.

-spec add_payload(Payload :: binary(), SC :: state_channel(), Skewed :: skewed:skewed()) -> {state_channel(),
                                                                                             skewed:skewed()}.
add_payload(Payload, SC, Skewed) ->
    %% Check if we have already seen this payload in skewed
    %% If yes, don't do anything, otherwise, add to skewed and return new state_channel
    case skewed:contains(Skewed, Payload) of
        true ->
            {SC, Skewed};
        false ->
            NewSkewed = skewed:add(Payload, Skewed),
            NewRootHash = skewed:root_hash(NewSkewed),
            {SC#blockchain_state_channel_v1_pb{root_hash=NewRootHash}, NewSkewed}
    end.


-spec to_json(state_channel(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(SC, _Opts) ->
    #{
      id => ?BIN_TO_B64(id(SC)),
      owner => ?BIN_TO_B58(owner(SC)),
      nonce => nonce(SC),
      summaries => [blockchain_state_channel_summary_v1:to_json(B, []) || B <- summaries(SC)],
      root_hash => ?BIN_TO_B64(root_hash(SC)),
      state => case(state(SC)) of
                   open -> <<"open">>;
                   closed -> <<"closed">>
               end,
      expire_at_block => expire_at_block(SC)
     }.

%%--------------------------------------------------------------------
%% @doc
%% Get causal relation between older SC and the current SC.
%%
%% If current dominates older, return caused
%% If current == older, return equal
%% In all other scenarios, return conflict
%%
%% Important: The expected older state channel should be passed <b>first</b>.
%% @end
%%--------------------------------------------------------------------
-spec compare_causality(OlderSC :: state_channel(),
                        CurrentSC :: state_channel()) -> temporal_relation().
compare_causality(OlderSC, CurrentSC) ->
    OlderNonce = ?MODULE:nonce(OlderSC),
    CurrentNonce = ?MODULE:nonce(CurrentSC),

    {_, Res} = case {OlderNonce, CurrentNonce} of
                   {OlderNonce, OlderNonce} ->
                       %% nonces are equal, summaries must be equal
                       OlderSummaries = ?MODULE:summaries(OlderSC),
                       CurrentSummaries = ?MODULE:summaries(CurrentSC),
                       case OlderSummaries == CurrentSummaries of
                           false ->
                               %% If the nonces are the same but the summaries are not
                               %% then that's a conflict.
                               {done, conflict};
                           true ->
                               {done, equal}
                       end;
                   {OlderNonce, CurrentNonce} when CurrentNonce > OlderNonce ->
                       %% Every hotspot in older summary must have either
                       %% the same/lower packet and dc count when compared
                       %% to current summary
                       check_causality(?MODULE:summaries(OlderSC), CurrentSC);
                   {OlderNonce, CurrentNonce} when CurrentNonce < OlderNonce ->
                       %% The current nonce is smaller than the older nonce;
                       %% that's a conflict
                       {done, conflict}
               end,
    Res.


merge(SCA, SCB) ->
    lager:info("merging state channels"),
    [SC1, SC2] = lists:sort(fun(A, B) -> ?MODULE:nonce(A) =< ?MODULE:nonce(B) end, [SCA, SCB]),

    lists:foldl(fun(Summary, SCAcc) ->
                        case get_summary(blockchain_state_channel_summary_v1:client_pubkeybin(Summary), SCAcc) of
                            {error, not_found} ->
                                update_summary_for(blockchain_state_channel_summary_v1:client_pubkeybin(Summary), Summary, SCAcc);
                            {ok, OurSummary} ->
                                case blockchain_state_channel_summary_v1:num_dcs(OurSummary) < blockchain_state_channel_summary_v1:num_dcs(Summary) of
                                    true ->
                                        update_summary_for(blockchain_state_channel_summary_v1:client_pubkeybin(Summary), Summary, SCAcc);
                                    false ->
                                        SCAcc
                                end
                        end
                end, SC2, summaries(SC1)).


%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
-spec can_fit(ClientPubkeyBin :: libp2p_crypto:pubkey_bin(),
              Summaries :: summaries()) -> boolean().
can_fit(ClientPubkeyBin, Summaries) ->
    Clients = [blockchain_state_channel_summary_v1:client_pubkeybin(S) || S <- Summaries],
    CanFit = length(lists:usort(Clients)) =< ?MAX_UNIQ_CLIENTS,
    IsKnownClient = lists:member(ClientPubkeyBin, Clients),

    case {CanFit, IsKnownClient} of
        {false, false} ->
            %% Cannot fit, do not have this client either
            false;
        {false, true} ->
            %% Cannot fit any new ones, but know about this client
            true;
        {true, _} ->
            %% Can fit, doesn't matter if we don't know this client
            true
    end.

-spec check_causality(SCSummaries :: summaries(),
                      OtherSC :: state_channel()) -> {done, temporal_relation()}.
check_causality(SCSummaries, OtherSC) ->
    lists:foldl(fun(_SCSummary, {done, Acc}) ->
                        {done, Acc};
                   (SCSummary, {not_done, Acc}) ->
                        ClientPubkeyBin = blockchain_state_channel_summary_v1:client_pubkeybin(SCSummary),
                        case ?MODULE:get_summary(ClientPubkeyBin, OtherSC) of
                            {error, _} ->
                                %% OtherSC does not have this client's summary, conflict
                                {done, conflict};
                            {ok, OtherSCSummary} ->
                                %% We found summary for this client in OtherSC
                                %% Check balances
                                SC1NumDCs = blockchain_state_channel_summary_v1:num_dcs(SCSummary),
                                SC1NumPackets = blockchain_state_channel_summary_v1:num_dcs(SCSummary),
                                OtherSCNumDCs = blockchain_state_channel_summary_v1:num_dcs(OtherSCSummary),
                                OtherSCNumPackets = blockchain_state_channel_summary_v1:num_dcs(OtherSCSummary),
                                Check = (OtherSCNumPackets >= SC1NumPackets) andalso (OtherSCNumDCs >= SC1NumDCs),
                                case Check of
                                    false ->
                                        {done, conflict};
                                    true ->
                                        {not_done, Acc}
                                end
                        end
                end, {not_done, caused}, SCSummaries).

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    SC = #blockchain_state_channel_v1_pb{
        id= <<"1">>,
        owner= <<"owner">>,
        nonce=0,
        credits=0,
        summaries=[],
        root_hash= <<>>,
        state=open,
        expire_at_block=0
    },
    ?assertEqual(SC, new(<<"1">>, <<"owner">>, 0)).

new2_test() ->
    SC = #blockchain_state_channel_v1_pb{
        id= <<"1">>,
        owner= <<"owner">>,
        nonce=0,
        credits=0,
        summaries=[],
        root_hash= <<>>,
        state=open,
        expire_at_block=100
    },
    BlockHash = <<"yolo">>,
    Skewed = skewed:new(BlockHash),
    ?assertEqual({SC, Skewed}, new(<<"1">>, <<"owner">>, 0, <<"yolo">>, 100)).

id_test() ->
    SC = new(<<"1">>, <<"owner">>, 0),
    ?assertEqual(<<"1">>, id(SC)).

owner_test() ->
    SC = new(<<"1">>, <<"owner">>, 0),
    ?assertEqual(<<"owner">>, owner(SC)).

nonce_test() ->
    SC = new(<<"1">>, <<"owner">>, 0),
    ?assertEqual(0, nonce(SC)),
    ?assertEqual(1, nonce(nonce(1, SC))).

summaries_test() ->
    #{public := PubKey} = libp2p_crypto:generate_keys(ecc_compact),
    PubKeyBin = libp2p_crypto:pubkey_to_bin(PubKey),
    SC = new(<<"1">>, <<"owner">>, 0),
    ?assertEqual([], summaries(SC)),
    Summary = blockchain_state_channel_summary_v1:new(PubKeyBin),
    ExpectedSummaries = [Summary],
    NewSC = blockchain_state_channel_v1:summaries(ExpectedSummaries, SC),
    ?assertEqual({ok, Summary}, get_summary(PubKeyBin, NewSC)),
    ?assertEqual(ExpectedSummaries, blockchain_state_channel_v1:summaries(NewSC)).

summaries_not_found_test() ->
    #{public := PubKey} = libp2p_crypto:generate_keys(ecc_compact),
    PubKeyBin = libp2p_crypto:pubkey_to_bin(PubKey),
    SC = new(<<"1">>, <<"owner">>, 0),
    ?assertEqual({error, not_found}, get_summary(PubKeyBin, SC)).

update_summaries_test() ->
    #{public := PubKey} = libp2p_crypto:generate_keys(ecc_compact),
    PubKeyBin = libp2p_crypto:pubkey_to_bin(PubKey),
    SC = new(<<"1">>, <<"owner">>, 0),
    io:format("Summaries0: ~p~n", [summaries(SC)]),
    ?assertEqual([], summaries(SC)),
    Summary = blockchain_state_channel_summary_v1:new(PubKeyBin),
    ExpectedSummaries = [Summary],
    NewSC = blockchain_state_channel_v1:summaries(ExpectedSummaries, SC),
    io:format("Summaries1: ~p~n", [summaries(NewSC)]),
    ?assertEqual({ok, Summary}, get_summary(PubKeyBin, NewSC)),
    NewSummary = blockchain_state_channel_summary_v1:new(PubKeyBin, 1, 1),
    NewSC1 = blockchain_state_channel_v1:update_summary_for(PubKeyBin, NewSummary, NewSC),
    io:format("Summaries2: ~p~n", [summaries(NewSC1)]),
    ?assertEqual({ok, NewSummary}, get_summary(PubKeyBin, NewSC1)).

root_hash_test() ->
    SC = new(<<"1">>, <<"owner">>, 0),
    ?assertEqual(<<>>, root_hash(SC)),
    ?assertEqual(<<"root_hash">>, root_hash(root_hash(<<"root_hash">>, SC))).

state_test() ->
    SC = new(<<"1">>, <<"owner">>, 0),
    ?assertEqual(open, state(SC)),
    ?assertEqual(closed, state(state(closed, SC))).

expire_at_block_test() ->
    SC = new(<<"1">>, <<"owner">>, 0),
    ?assertEqual(0, expire_at_block(SC)),
    ?assertEqual(1234567, expire_at_block(expire_at_block(1234567, SC))).

encode_decode_test() ->
    SC0 = new(<<"1">>, <<"owner">>, 0),
    ?assertEqual(SC0, decode(encode(SC0))),
    #{public := PubKey0} = libp2p_crypto:generate_keys(ecc_compact),
    PubKeyBin0 = libp2p_crypto:pubkey_to_bin(PubKey0),
    #{public := PubKey1} = libp2p_crypto:generate_keys(ecc_compact),
    PubKeyBin1 = libp2p_crypto:pubkey_to_bin(PubKey1),
    Summary0 = blockchain_state_channel_summary_v1:new(PubKeyBin0),
    Summary1 = blockchain_state_channel_summary_v1:new(PubKeyBin1),
    SC1 = summaries([Summary1, Summary0], SC0),
    SC2 = summaries([Summary0, Summary1], SC0),
    ?assertEqual(SC1, decode(encode(SC1))),
    ?assertEqual(SC2, decode(encode(SC2))).

save_fetch_test() ->
    BaseDir = test_utils:tmp_dir("save_fetch_test"),
    {ok, DB} = open_db(BaseDir),
    SC = new(<<"1">>, <<"owner">>, 0),
    Skewed = skewed:new(<<"yolo">>),
    ?assertEqual(ok, save(DB, SC, Skewed)),
    ?assertEqual({ok, {SC, Skewed}}, fetch(DB, <<"1">>)).

open_db(Dir) ->
    DBDir = filename:join(Dir, "state_channels.db"),
    ok = filelib:ensure_dir(DBDir),
    GlobalOpts = application:get_env(rocksdb, global_opts, []),
    DBOptions = [{create_if_missing, true}] ++ GlobalOpts,
    {ok, _DB} = rocksdb:open(DBDir, DBOptions).

causality_test() ->
    BaseDir = test_utils:tmp_dir("causality_test"),
    {ok, _DB} = open_db(BaseDir),
    #{public := PubKey} = libp2p_crypto:generate_keys(ecc_compact),
    PubKeyBin = libp2p_crypto:pubkey_to_bin(PubKey),
    #{public := PubKey1} = libp2p_crypto:generate_keys(ecc_compact),
    PubKeyBin1 = libp2p_crypto:pubkey_to_bin(PubKey1),
    Summary1 = blockchain_state_channel_summary_v1:num_packets(2, blockchain_state_channel_summary_v1:num_dcs(2, blockchain_state_channel_summary_v1:new(PubKeyBin))),
    Summary2 = blockchain_state_channel_summary_v1:num_packets(4, blockchain_state_channel_summary_v1:num_dcs(4, blockchain_state_channel_summary_v1:new(PubKeyBin))),
    Summary3 = blockchain_state_channel_summary_v1:num_packets(1, blockchain_state_channel_summary_v1:num_dcs(1, blockchain_state_channel_summary_v1:new(PubKeyBin1))),

    %% base, equal
    BaseSC1 = new(<<"1">>, <<"owner">>, 1),
    BaseSC2 = new(<<"1">>, <<"owner">>, 1),
    ?assertEqual(equal, compare_causality(BaseSC1, BaseSC2)),

    %% no summary, increasing nonce
    SC1 = new(<<"1">>, <<"owner">>, 1),
    SC2 = nonce(1, new(<<"1">>, <<"owner">>, 1)),
    ?assertEqual(caused, compare_causality(SC1, SC2)),
    ?assertEqual(conflict, compare_causality(SC2, SC1)),

    %% 0 (same) nonce, with differing summary, conflict
    SC3 = summaries([Summary2], new(<<"1">>, <<"owner">>, 1)),
    SC4 = summaries([Summary1], new(<<"1">>, <<"owner">>, 1)),
    ?assertEqual(conflict, compare_causality(SC3, SC4)),
    ?assertEqual(conflict, compare_causality(SC4, SC3)),

    %% SC6 is allowed after SC5
    SC5 = new(<<"1">>, <<"owner">>, 1),
    SC6 = summaries([Summary1], nonce(1, new(<<"1">>, <<"owner">>, 1))),
    ?assertEqual(caused, compare_causality(SC5, SC6)),
    ?assertEqual(conflict, compare_causality(SC6, SC5)),

    %% SC7 is allowed after SC5
    %% NOTE: skipped a nonce here (should this actually be allowed?)
    SC7 = summaries([Summary1], nonce(2, new(<<"1">>, <<"owner">>, 1))),
    ?assertEqual(caused, compare_causality(SC5, SC7)),
    ?assertEqual(conflict, compare_causality(SC7, SC5)),

    %% SC9 has higher nonce than SC8, however,
    %% SC9 does not have summary which SC8 has, conflict
    SC8 = summaries([Summary1], nonce(8, new(<<"1">>, <<"owner">>, 1))),
    SC9 = nonce(9, new(<<"1">>, <<"owner">>, 1)),
    ?assertEqual(conflict, compare_causality(SC8, SC9)),
    ?assertEqual(conflict, compare_causality(SC9, SC8)),

    %% same non-zero nonce, with differing summary, conflict
    SC10 = nonce(10, new(<<"1">>, <<"owner">>, 1)),
    SC11 = summaries([Summary1], nonce(10, new(<<"1">>, <<"owner">>, 1))),
    ?assertEqual(conflict, compare_causality(SC10, SC11)),
    ?assertEqual(conflict, compare_causality(SC11, SC10)),

    %% natural progression
    SC12 = summaries([Summary1], nonce(10, new(<<"1">>, <<"owner">>, 1))),
    SC13 = summaries([Summary2], nonce(11, new(<<"1">>, <<"owner">>, 1))),
    ?assertEqual(caused, compare_causality(SC12, SC13)),
    ?assertEqual(conflict, compare_causality(SC13, SC12)),

    %% definite conflict, since summary for a client is missing in higher nonce sc
    SC14 = summaries([Summary1], nonce(11, new(<<"1">>, <<"owner">>, 1))),
    SC15 = summaries([Summary2], nonce(10, new(<<"1">>, <<"owner">>, 1))),
    ?assertEqual(conflict, compare_causality(SC14, SC15)),
    ?assertEqual(conflict, compare_causality(SC15, SC14)),

    %% another natural progression
    SC16 = summaries([Summary1], nonce(10, new(<<"1">>, <<"owner">>, 1))),
    SC17 = summaries([Summary2, Summary3], nonce(11, new(<<"1">>, <<"owner">>, 1))),
    ?assertEqual(caused, compare_causality(SC16, SC17)),
    ?assertEqual(conflict, compare_causality(SC17, SC16)),

    %% another definite conflict, since higher nonce sc does not have previous summary
    SC18 = summaries([Summary1, Summary3], nonce(10, new(<<"1">>, <<"owner">>, 1))),
    SC19 = summaries([Summary2], nonce(11, new(<<"1">>, <<"owner">>, 1))),
    ?assertEqual(conflict, compare_causality(SC18, SC19)),
    ?assertEqual(conflict, compare_causality(SC19, SC18)),

    %% yet another conflict, since a higher nonce sc has an older client summary
    SC20 = summaries([Summary2, Summary3], nonce(10, new(<<"1">>, <<"owner">>, 1))),
    SC21 = summaries([Summary1, Summary3], nonce(11, new(<<"1">>, <<"owner">>, 1))),
    ?assertEqual(conflict, compare_causality(SC20, SC21)),
    ?assertEqual(conflict, compare_causality(SC21, SC20)),

    %% natural progression with nonce skip
    SC22 = summaries([Summary1], nonce(10, new(<<"1">>, <<"owner">>, 1))),
    SC23 = summaries([Summary2, Summary3], nonce(12, new(<<"1">>, <<"owner">>, 1))),
    ?assertEqual(caused, compare_causality(SC22, SC23)),
    ?assertEqual(conflict, compare_causality(SC23, SC22)),

    ok.

-endif.
