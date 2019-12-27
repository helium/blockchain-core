%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Ledger State Channel ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_ledger_state_channel_v1).

-export([
    new/4,
    id/1, id/2,
    owner/1, owner/2,
    amount/1, amount/2,
    expire_at_block/1, expire_at_block/2,
    serialize/1, deserialize/1
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(ledger_state_channel_v1, {
    id :: binary(),
    owner :: binary(),
    amount :: non_neg_integer(),
    expire_at_block :: pos_integer()
}).

-type state_channel() :: #ledger_state_channel_v1{}.

-export_type([state_channel/0]).

-spec new(binary(), binary(), non_neg_integer(), pos_integer()) -> state_channel().
new(ID, Owner, Amount, Timer) when ID /= undefined andalso
                                   Owner /= undefined andalso
                                   Amount /= undefined andalso
                                   Timer /= undefined ->
    #ledger_state_channel_v1{
        id=ID,
        owner=Owner,
        amount=Amount,
        expire_at_block=Timer
    }.

-spec id(state_channel()) -> binary().
id(#ledger_state_channel_v1{id=ID}) ->
    ID.

-spec id(binary(), state_channel()) -> state_channel().
id(ID, SC) ->
    SC#ledger_state_channel_v1{id=ID}.

-spec owner(state_channel()) -> binary().
owner(#ledger_state_channel_v1{owner=Owner}) ->
    Owner.

-spec owner(binary(), state_channel()) -> state_channel().
owner(Owner, SC) ->
    SC#ledger_state_channel_v1{owner=Owner}.

-spec amount(state_channel()) -> non_neg_integer().
amount(#ledger_state_channel_v1{amount=Amount}) ->
    Amount.

-spec amount(non_neg_integer(), state_channel()) -> state_channel().
amount(Amount, SC) ->
    SC#ledger_state_channel_v1{amount=Amount}.

-spec expire_at_block(state_channel()) -> pos_integer().
expire_at_block(#ledger_state_channel_v1{expire_at_block=Timer}) ->
    Timer.

-spec expire_at_block(pos_integer(), state_channel()) -> state_channel().
expire_at_block(Timer, SC) ->
    SC#ledger_state_channel_v1{expire_at_block=Timer}.

%%--------------------------------------------------------------------
%% @doc
%% Version 1
%% @end
%%--------------------------------------------------------------------
-spec serialize(state_channel()) -> binary().
serialize(SC) ->
    BinSC = erlang:term_to_binary(SC),
    <<1, BinSC/binary>>.

%%--------------------------------------------------------------------
%% @doc
%% Later _ could becomre 1, 2, 3 for different versions.
%% @end
%%--------------------------------------------------------------------
-spec deserialize(binary()) -> state_channel().
deserialize(<<_:1/binary, Bin/binary>>) ->
    erlang:binary_to_term(Bin).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    SC = #ledger_state_channel_v1{
        id = <<"id">>,
        owner = <<"owner">>,
        amount = 1,
        expire_at_block = 10
    },
    ?assertEqual(SC, new(<<"id">>, <<"owner">>, 1, 10)).

id_test() ->
    SC = new(<<"id">>, <<"owner">>, 1, 10),
    ?assertEqual(<<"id">>, id(SC)),
    ?assertEqual(<<"id2">>, id(id(<<"id2">>, SC))).

owner_test() ->
    SC = new(<<"id">>, <<"owner">>, 1, 10),
    ?assertEqual(<<"owner">>, owner(SC)),
    ?assertEqual(<<"owner2">>, owner(owner(<<"owner2">>, SC))).

amount_test() ->
    SC = new(<<"id">>, <<"owner">>, 1, 10),
    ?assertEqual(1, amount(SC)),
    ?assertEqual(2, amount(amount(2, SC))).

expire_at_block_test() ->
    SC = new(<<"id">>, <<"owner">>, 1, 10),
    ?assertEqual(10, expire_at_block(SC)),
    ?assertEqual(20, expire_at_block(expire_at_block(20, SC))).

-endif.
