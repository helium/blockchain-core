%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channel_v1).

-export([
    new/2, new/4,
    id/1,
    owner/1,
    nonce/1, nonce/2,
    root_hash/1, root_hash/2,
    state/1, state/2,
    expire_at_block/1, expire_at_block/2,
    signature/1, sign/2, validate/1,
    encode/1, decode/1,
    save/2, get/2,
    summaries/1, summaries/2, update_summaries/3,

    add_payload/2,
    skewed/1, skewed/2,
    get_summary/2,
    num_packets_for/2, num_dcs_for/2
]).

-include_lib("helium_proto/include/blockchain_state_channel_v1_pb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type state_channel() :: #blockchain_state_channel_v1_pb{}.
-type id() :: binary().
-type state() :: open | closed.
-type summaries() :: [blockchain_state_channel_summary_v1:summary()].

-export_type([state_channel/0, id/0]).

-spec new(binary(), libp2p_crypto:pubkey_bin()) -> state_channel().
new(ID, Owner) ->
    #blockchain_state_channel_v1_pb{
        id=ID,
        owner=Owner,
        nonce=0,
        summaries=[],
        root_hash= <<>>,
        skewed=undefined,
        state=open,
        expire_at_block=0
    }.

-spec new(ID :: binary(),
          Owner :: libp2p_crypto:pubkey_bin(),
          BlockHash :: binary(),
          ExpireAtBlock :: pos_integer()) -> state_channel().
new(ID, Owner, BlockHash, ExpireAtBlock) ->
    #blockchain_state_channel_v1_pb{
        id=ID,
        owner=Owner,
        nonce=0,
        summaries=[],
        root_hash= <<>>,
        skewed=skewed:new(BlockHash),
        state=open,
        expire_at_block=ExpireAtBlock
    }.

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

-spec summaries(state_channel()) -> summaries().
summaries(#blockchain_state_channel_v1_pb{summaries=Summaries}) ->
    Summaries.

-spec summaries(Summaries :: summaries(),
                SC :: state_channel()) -> state_channel().
summaries(Summaries, SC) ->
    SC#blockchain_state_channel_v1_pb{summaries=Summaries}.

-spec update_summaries(ClientPubkeyBin :: libp2p_crypto:pubkey_bin(),
                       NewSummary :: blockchain_state_channel_summary_v1:summary(),
                       SC :: state_channel()) -> state_channel().
update_summaries(ClientPubkeyBin, NewSummary, #blockchain_state_channel_v1_pb{summaries=Summaries}=SC) ->
    case get_summary(ClientPubkeyBin, SC) of
        {error, not_found} ->
            SC#blockchain_state_channel_v1_pb{summaries=[NewSummary | Summaries]};
        {ok, _Summary} ->
            NewSummaries = lists:keyreplace(ClientPubkeyBin,
                                            #blockchain_state_channel_summary_v1_pb.client_pubkeybin,
                                            Summaries,
                                            NewSummary),
            SC#blockchain_state_channel_v1_pb{summaries=NewSummaries}
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

-spec num_dcs_for(ClientPubkeyBin :: libp2p_crypto:pubkey_bin(),
                  SC :: state_channel()) -> {ok, non_neg_integer()} | {error, not_found}.
num_dcs_for(ClientPubkeyBin, SC) ->
    case get_summary(ClientPubkeyBin, SC) of
        {error, _}=E -> E;
        {ok, Summary} ->
            {ok, blockchain_state_channel_summary_v1:num_dcs(Summary)}
    end.

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
        true -> ok
    end.

-spec encode(state_channel()) -> binary().
encode(#blockchain_state_channel_v1_pb{}=SC) ->
    blockchain_state_channel_v1_pb:encode_msg(SC).

-spec decode(binary()) -> state_channel().
decode(Binary) ->
    blockchain_state_channel_v1_pb:decode_msg(Binary, blockchain_state_channel_v1_pb).

-spec save(rocksdb:db_handle(), state_channel()) -> ok.
save(DB, SC) ->
    ID = ?MODULE:id(SC),
    ok = rocksdb:put(DB, ID, ?MODULE:encode(SC), [{sync, true}]).

-spec get(rocksdb:db_handle(), id()) -> {ok, state_channel()} | {error, any()}.
get(DB, ID) ->
    case rocksdb:get(DB, ID, [{sync, true}]) of
        {ok, BinarySC} -> {ok, ?MODULE:decode(BinarySC)};
        not_found -> {error, not_found};
        Error -> Error
    end.

-spec skewed(state_channel()) -> skewed:skewed().
skewed(#blockchain_state_channel_v1_pb{skewed=Skewed}) ->
    Skewed.

-spec skewed(undefined | skewed:skewed(), state_channel()) -> state_channel().
skewed(Skewed, SC) ->
    SC#blockchain_state_channel_v1_pb{skewed=Skewed}.

-spec add_payload(Payload :: binary(), state_channel()) -> state_channel().
add_payload(Payload, #blockchain_state_channel_v1_pb{skewed=undefined}=SC) ->
    %% Don't have a skewed, create new one
    Skewed0 = skewed:new(),
    Skewed = skewed:add(Payload, Skewed0),
    RootHash = skewed:root_hash(Skewed),
    SC#blockchain_state_channel_v1_pb{skewed=Skewed, root_hash=RootHash};
add_payload(Payload, #blockchain_state_channel_v1_pb{skewed=Skewed}=SC) ->
    %% Already have a skewed
    %% Check if we have already seen this payload in skewed
    %% If yes, don't do anything, otherwise, add to skewed and return new state_channel
    case skewed:contains(Skewed, Payload) of
        true ->
            SC;
        false ->
            NewSkewed = skewed:add(Payload, Skewed),
            NewRootHash = skewed:root_hash(NewSkewed),
            SC#blockchain_state_channel_v1_pb{skewed=NewSkewed, root_hash=NewRootHash}
    end.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    SC = #blockchain_state_channel_v1_pb{
        id= <<"1">>,
        owner= <<"owner">>,
        nonce=0,
        summaries=[],
        root_hash= <<>>,
        skewed=undefined,
        state=open,
        expire_at_block=0
    },
    ?assertEqual(SC, new(<<"1">>, <<"owner">>)).

id_test() ->
    SC = new(<<"1">>, <<"owner">>),
    ?assertEqual(<<"1">>, id(SC)).

owner_test() ->
    SC = new(<<"1">>, <<"owner">>),
    ?assertEqual(<<"owner">>, owner(SC)).

nonce_test() ->
    SC = new(<<"1">>, <<"owner">>),
    ?assertEqual(0, nonce(SC)),
    ?assertEqual(1, nonce(nonce(1, SC))).

summaries_test() ->
    #{public := PubKey} = libp2p_crypto:generate_keys(ecc_compact),
    PubKeyBin = libp2p_crypto:pubkey_to_bin(PubKey),
    SC = new(<<"1">>, <<"owner">>),
    ?assertEqual([], summaries(SC)),
    Summary = blockchain_state_channel_summary_v1:new(PubKeyBin),
    ExpectedSummaries = [Summary],
    NewSC = blockchain_state_channel_v1:summaries(ExpectedSummaries, SC),
    ?assertEqual({ok, Summary}, get_summary(PubKeyBin, NewSC)),
    ?assertEqual(ExpectedSummaries, blockchain_state_channel_v1:summaries(NewSC)).

summaries_not_found_test() ->
    #{public := PubKey} = libp2p_crypto:generate_keys(ecc_compact),
    PubKeyBin = libp2p_crypto:pubkey_to_bin(PubKey),
    SC = new(<<"1">>, <<"owner">>),
    ?assertEqual({error, not_found}, get_summary(PubKeyBin, SC)).

update_summaries_test() ->
    #{public := PubKey} = libp2p_crypto:generate_keys(ecc_compact),
    PubKeyBin = libp2p_crypto:pubkey_to_bin(PubKey),
    SC = new(<<"1">>, <<"owner">>),
    io:format("Summaries0: ~p~n", [summaries(SC)]),
    ?assertEqual([], summaries(SC)),
    Summary = blockchain_state_channel_summary_v1:new(PubKeyBin),
    ExpectedSummaries = [Summary],
    NewSC = blockchain_state_channel_v1:summaries(ExpectedSummaries, SC),
    io:format("Summaries1: ~p~n", [summaries(NewSC)]),
    ?assertEqual({ok, Summary}, get_summary(PubKeyBin, NewSC)),
    NewSummary = blockchain_state_channel_summary_v1:new(PubKeyBin, 1, 1),
    NewSC1 = blockchain_state_channel_v1:update_summaries(PubKeyBin, NewSummary, NewSC),
    io:format("Summaries2: ~p~n", [summaries(NewSC1)]),
    ?assertEqual({ok, NewSummary}, get_summary(PubKeyBin, NewSC1)).

root_hash_test() ->
    SC = new(<<"1">>, <<"owner">>),
    ?assertEqual(<<>>, root_hash(SC)),
    ?assertEqual(<<"root_hash">>, root_hash(root_hash(<<"root_hash">>, SC))).

state_test() ->
    SC = new(<<"1">>, <<"owner">>),
    ?assertEqual(open, state(SC)),
    ?assertEqual(closed, state(state(closed, SC))).

expire_at_block_test() ->
    SC = new(<<"1">>, <<"owner">>),
    ?assertEqual(0, expire_at_block(SC)),
    ?assertEqual(1234567, expire_at_block(expire_at_block(1234567, SC))).

encode_decode_test() ->
    SC0 = new(<<"1">>, <<"owner">>),
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

save_get_test() ->
    BaseDir = test_utils:tmp_dir("save_get_test"),
    {ok, DB} = open_db(BaseDir),
    SC = new(<<"1">>, <<"owner">>),
    ?assertEqual(ok, save(DB, SC)),
    ?assertEqual({ok, SC}, get(DB, <<"1">>)).

open_db(Dir) ->
    DBDir = filename:join(Dir, "state_channels.db"),
    ok = filelib:ensure_dir(DBDir),
    GlobalOpts = application:get_env(rocksdb, global_opts, []),
    DBOptions = [{create_if_missing, true}] ++ GlobalOpts,
    {ok, _DB} = rocksdb:open(DBDir, DBOptions).

-endif.
