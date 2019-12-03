%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channel_v1).

-export([
    new/2,
    zero_id/0, id/1,
    owner/1,
    credits/1, credits/2,
    nonce/1, nonce/2,
    balances/1, balances/2, balance/2, balance/3,
    packets/1, packets/2,
    signature/1, sign/2, validate/1,
    encode/1, decode/1,
    save/2, get/2,
    validate_request/2,
    add_request/3
]).

-include_lib("helium_proto/src/pb/helium_state_channel_v1_pb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(ZERO_SC_ID, <<0>>).

-type state_channel() :: #helium_state_channel_v1_pb{}.
-type balances() :: [{string(), non_neg_integer()}].
-type id() :: binary().

-export_type([state_channel/0, id/0]).

-spec new(binary(), libp2p_crypto:pubkey_bin()) -> state_channel().
new(ID, Owner) ->
    #helium_state_channel_v1_pb{
        id=ID,
        owner=Owner,
        credits=0,
        nonce=0,
        balances=[],
        packets= <<>>
    }.

-spec zero_id() -> binary().
zero_id() ->
    ?ZERO_SC_ID.

-spec id(state_channel()) -> binary().
id(#helium_state_channel_v1_pb{id=ID}) ->
    ID.

-spec owner(state_channel()) -> libp2p_crypto:pubkey_bin().
owner(#helium_state_channel_v1_pb{owner=Owner}) ->
    Owner.

-spec credits(state_channel()) -> non_neg_integer().
credits(#helium_state_channel_v1_pb{credits=Credits}) ->
    Credits.

-spec credits(non_neg_integer(), state_channel()) -> state_channel().
credits(Credits, SC) ->
    SC#helium_state_channel_v1_pb{credits=Credits}.

-spec nonce(state_channel()) -> non_neg_integer().
nonce(#helium_state_channel_v1_pb{nonce=Nonce}) ->
    Nonce.

-spec nonce(non_neg_integer(), state_channel()) -> state_channel().
nonce(Nonce, SC) ->
    SC#helium_state_channel_v1_pb{nonce=Nonce}.

-spec balances(state_channel()) -> balances().
balances(#helium_state_channel_v1_pb{balances=Balances}) ->
    lists:map(
        fun({PayeeB58, Balance}) ->
            {libp2p_crypto:b58_to_bin(PayeeB58), Balance}
        end,
        Balances
    ).

-spec balances(balances(), state_channel()) -> state_channel().
balances(Balances, SC) ->
    SC#helium_state_channel_v1_pb{balances=Balances}.

-spec balance(libp2p_crypto:pubkey_bin(), state_channel()) -> non_neg_integer().
balance(Payee, SC) ->
    Balances = blockchain_state_channel_v1:balances(SC),
    case lists:keyfind(Payee, 1, Balances) of
        {Payee, Balance} -> Balance;
        false -> 0
    end.

-spec balance(string(), non_neg_integer(), state_channel()) -> state_channel().
balance(Payee, Balance, SC) ->
    PayeeB58 = libp2p_crypto:bin_to_b58(Payee),
    Balances0 = ?MODULE:balances(SC),
    Balances1 = lists:keystore(PayeeB58, 1, Balances0, {PayeeB58, Balance}),
    ?MODULE:balances(Balances1, SC).

-spec packets(state_channel()) -> binary().
packets(#helium_state_channel_v1_pb{packets=Packets}) ->
    Packets.

-spec packets(binary(), state_channel()) -> state_channel().
packets(Packets, SC) ->
    SC#helium_state_channel_v1_pb{packets=Packets}.

-spec signature(state_channel()) -> binary().
signature(#helium_state_channel_v1_pb{signature=Signature}) ->
    Signature.

-spec sign(state_channel(), function()) -> state_channel().
sign(SC, SigFun) ->
    EncodedSC = ?MODULE:encode(SC#helium_state_channel_v1_pb{signature= <<>>}),
    Signature = SigFun(EncodedSC),
    SC#helium_state_channel_v1_pb{signature=Signature}.

-spec validate(state_channel()) -> true | {error, any()}.
validate(SC) ->
    BaseSC = SC#helium_state_channel_v1_pb{signature = <<>>},
    EncodedSC = ?MODULE:encode(BaseSC),
    Signature = ?MODULE:signature(SC),
    Owner = ?MODULE:owner(SC),
    PubKey = libp2p_crypto:bin_to_pubkey(Owner),
    libp2p_crypto:verify(EncodedSC, Signature, PubKey).

-spec encode(state_channel()) -> binary().
encode(#helium_state_channel_v1_pb{}=SC) ->
    helium_state_channel_v1_pb:encode_msg(SC).

-spec decode(binary()) -> state_channel().
decode(Binary) ->
    helium_state_channel_v1_pb:decode_msg(Binary, helium_state_channel_v1_pb).

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

-spec validate_request(blockchain_state_channel_request_v1:request(), state_channel()) -> ok | {error, any()}.
validate_request(Request, SC) ->
    ReqAmount = blockchain_state_channel_request_v1:amount(Request),
    SCCredits = ?MODULE:credits(SC),
    case SCCredits-ReqAmount >= 0 of
        false -> {error, not_enough_credits};
        true -> ok
    end.

-spec add_request(blockchain_state_channel_request_v1:request(), function(), state_channel()) -> state_channel().
add_request(Request, SigFun, SC0) ->
    Credits = ?MODULE:credits(SC0),
    Nonce = ?MODULE:nonce(SC0),
    Amount = blockchain_state_channel_request_v1:amount(Request),
    Payee = blockchain_state_channel_request_v1:payee(Request),
    Balance = ?MODULE:balance(Payee, SC0),
    SC1 = ?MODULE:credits(Credits-Amount, SC0),
    SC2 = ?MODULE:nonce(Nonce+1, SC1),
    SC3 = ?MODULE:balance(Payee, Balance+Amount, SC2),
    ?MODULE:sign(SC3, SigFun).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    SC = #helium_state_channel_v1_pb{
        id= <<"1">>,
        owner= <<"owner">>,
        credits=0,
        nonce=0,
        balances=[],
        packets= <<>>
    },
    ?assertEqual(SC, new(<<"1">>, <<"owner">>)).

id_test() ->
    SC = new(<<"1">>, <<"owner">>),
    ?assertEqual(<<"1">>, id(SC)).

owner_test() ->
    SC = new(<<"1">>, <<"owner">>),
    ?assertEqual(<<"owner">>, owner(SC)).

credits_test() ->
    SC = new(<<"1">>, <<"owner">>),
    ?assertEqual(0, credits(SC)),
    ?assertEqual(10, credits(credits(10, SC))).

nonce_test() ->
    SC = new(<<"1">>, <<"owner">>),
    ?assertEqual(0, nonce(SC)),
    ?assertEqual(1, nonce(nonce(1, SC))).

balances_test() ->
    #{public := PubKey} = libp2p_crypto:generate_keys(ecc_compact),
    PubKeyBin = libp2p_crypto:pubkey_to_bin(PubKey),
    SC = new(<<"1">>, <<"owner">>),
    ?assertEqual([], balances(SC)),
    ?assertEqual([{PubKeyBin, 2}], balances(balances([{libp2p_crypto:bin_to_b58(PubKeyBin), 2}], SC))).

balance_test() ->
    #{public := PubKey} = libp2p_crypto:generate_keys(ecc_compact),
    PubKeyBin = libp2p_crypto:pubkey_to_bin(PubKey),
    SC = new(<<"1">>, <<"owner">>),
    ?assertEqual(0, balance(PubKeyBin, SC)),
    ?assertEqual(2, balance(PubKeyBin, balance(PubKeyBin, 2, SC))).

packets_test() ->
    SC = new(<<"1">>, <<"owner">>),
    ?assertEqual(<<>>, packets(SC)),
    ?assertEqual(<<"packets">>, packets(packets(<<"packets">>, SC))).

encode_decode_test() ->
    SC0 = new(<<"1">>, <<"owner">>),
    ?assertEqual(SC0, decode(encode(SC0))),
    SC1 = balances([{"ReqID", 12}], SC0),
    ?assertEqual(SC1, decode(encode(SC1))).

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