%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channel).

-export([
    new/1,
    owner/1,
    credits/1, credits/2,
    nonce/1, nonce/2,
    payments/1, payments/2,
    packets/1, packets/2,
    encode/1, decode/1,
    save/2, get/2,
    validate_payment/2,
    add_payment/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(SERVER, ?MODULE).

-record(state_channel, {
    owner :: libp2p_crypto:pubkey_bin(),
    credits = 0 :: non_neg_integer(),
    nonce = 0 :: non_neg_integer(),
    payments = [] :: [blockchain_dcs_payment:dcs_payment()],
    packets :: merkerl:merkle()
}).

-type state_channel() :: #state_channel{}.

-export_type([state_channel/0]).

-spec new(libp2p_crypto:pubkey_bin()) -> state_channel().
new(Owner) ->
    #state_channel{
        owner=Owner,
        credits=0,
        nonce=0,
        payments=[],
        packets=merkerl:new([], fun merkerl:hash_value/1)
    }.

-spec owner(state_channel()) -> libp2p_crypto:pubkey_bin().
owner(#state_channel{owner=Owner}) ->
    Owner.

-spec credits(state_channel()) -> non_neg_integer().
credits(#state_channel{credits=Credits}) ->
    Credits.

-spec credits(non_neg_integer(), state_channel()) -> state_channel().
credits(Credits, SC) ->
    SC#state_channel{credits=Credits}.

-spec nonce(state_channel()) -> non_neg_integer().
nonce(#state_channel{nonce=Nonce}) ->
    Nonce.

-spec nonce(non_neg_integer(), state_channel()) -> state_channel().
nonce(Nonce, SC) ->
    SC#state_channel{nonce=Nonce}.

-spec payments(state_channel()) -> [blockchain_dcs_payment:dcs_payment()].
payments(#state_channel{payments=Payments}) ->
    Payments.

-spec payments([blockchain_dcs_payment:dcs_payment()], state_channel()) -> state_channel().
payments(Payments, SC) ->
    SC#state_channel{payments=Payments}.

-spec packets(state_channel()) -> merkerl:merkle().
packets(#state_channel{packets=Packets}) ->
    Packets.

-spec packets(merkerl:merkle(), state_channel()) -> state_channel().
packets(Packets, SC) ->
    SC#state_channel{packets=Packets}.

-spec encode(state_channel()) -> binary().
encode(SC) ->
    erlang:term_to_binary(SC).

-spec decode(binary()) -> state_channel().
decode(Binary) ->
    erlang:binary_to_term(Binary).

-spec save(rocksdb:db_handle(), state_channel()) -> ok.
save(DB, SC) ->
    Owner = ?MODULE:owner(SC),
    ok = rocksdb:put(DB, Owner, ?MODULE:encode(SC), [{sync, true}]).

-spec get(rocksdb:db_handle(), libp2p_crypto:pubkey_bin()) -> {ok, state_channel()} | {error, any()}.
get(DB, Owner) ->
    case rocksdb:get(DB, Owner, [{sync, true}]) of
        {ok, BinarySC} -> {ok, ?MODULE:decode(BinarySC)};
        not_found -> {error, not_found};
        Error -> Error
    end.

-spec validate_payment(blockchain_dcs_payment:dcs_payment(), state_channel()) -> ok | {error, any()}.
validate_payment(Payment, SC) ->
    case blockchain_dcs_payment:validate(Payment) of
        false ->
            {error, invalid_payment};
        true ->
            SCNonce = blockchain_state_channel:nonce(SC),
            PaymentNonce = blockchain_dcs_payment:nonce(Payment),
            case SCNonce+1 == PaymentNonce of
                false ->
                    {error, bad_nonce};
                true ->
                    SCCredits = blockchain_state_channel:credits(SC),
                    PaymenAmount = blockchain_dcs_payment:amount(Payment),
                    case SCCredits-PaymenAmount >= 0 of
                        false -> {error, not_enough_credit};
                        true -> ok
                    end
            end
    end.

-spec add_payment(blockchain_dcs_payment:dcs_payment(), state_channel()) -> state_channel().
add_payment(Payment, SC0) ->
    Credits = ?MODULE:credits(SC0),
    Payments = ?MODULE:payments(SC0),
    Nonce = blockchain_dcs_payment:nonce(Payment),
    Amount = blockchain_dcs_payment:amount(Payment),
    SC1 = ?MODULE:credits(Credits-Amount, SC0),
    SC2 = ?MODULE:nonce(Nonce, SC1),
    ?MODULE:payments([Payment|Payments], SC2).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    SC = #state_channel{
        owner= <<"owner">>,
        credits=0,
        nonce=0,
        payments=[],
        packets=merkerl:new([], fun merkerl:hash_value/1)
    },
    ?assertEqual(SC, new(<<"owner">>)).

owner_test() ->
    SC = new(<<"owner">>),
    ?assertEqual(<<"owner">>, owner(SC)).

credits_test() ->
    SC = new(<<"owner">>),
    ?assertEqual(0, credits(SC)),
    ?assertEqual(10, credits(credits(10, SC))).

nonce_test() ->
    SC = new(<<"owner">>),
    ?assertEqual(0, nonce(SC)),
    ?assertEqual(1, nonce(nonce(1, SC))).

payments_test() ->
    SC = new(<<"owner">>),
    ?assertEqual([], payments(SC)),
    ?assertEqual([1, 2], payments(payments([1, 2], SC))).

encode_decode_test() ->
    SC = new(<<"owner">>),
    ?assertEqual(SC, decode(encode(SC))).

save_get_test() ->
    BaseDir = test_utils:tmp_dir("save_get_test"),
    {ok, DB} = open_db(BaseDir),
    SC = new(<<"owner">>),
    ?assertEqual(ok, save(DB, SC)),
    ?assertEqual({ok, SC}, get(DB, <<"owner">>)).

open_db(Dir) ->
    DBDir = filename:join(Dir, "state_channels.db"),
    ok = filelib:ensure_dir(DBDir),
    GlobalOpts = application:get_env(rocksdb, global_opts, []),
    DBOptions = [{create_if_missing, true}] ++ GlobalOpts,
    {ok, _DB} = rocksdb:open(DBDir, DBOptions).

-endif.