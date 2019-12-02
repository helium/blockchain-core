%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Ledger State Channel ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_ledger_state_channel_v1).

-export([
    new/3,
    id/1, id/2,
    owner/1, owner/2,
    amount/1, amount/2,
    serialize/1, deserialize/1
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(state_channel_v1, {
    id :: binary(),
    owner :: binary(),
    amount :: non_neg_integer()
}).

-type state_channel() :: #state_channel_v1{}.

-export_type([state_channel/0]).

-spec new(binary(), binary(), non_neg_integer()) -> state_channel().
new(ID, Owner, Amount) when ID /= undefined andalso
                            Owner /= undefined andalso
                            Amount /= undefined ->
    #state_channel_v1{
        id=ID,
        owner=Owner,
        amount=Amount
    }.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec id(state_channel()) -> binary().
id(#state_channel_v1{id=ID}) ->
    ID.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec id(binary(), state_channel()) -> state_channel().
id(ID, SC) ->
    SC#state_channel_v1{id=ID}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec owner(state_channel()) -> binary().
owner(#state_channel_v1{owner=Owner}) ->
    Owner.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec owner(binary(), state_channel()) -> state_channel().
owner(Owner, SC) ->
    SC#state_channel_v1{owner=Owner}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec amount(state_channel()) -> non_neg_integer().
amount(#state_channel_v1{amount=Amount}) ->
    Amount.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec amount(non_neg_integer(), state_channel()) -> state_channel().
amount(Amount, SC) ->
    SC#state_channel_v1{amount=Amount}.

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
    SC = #state_channel_v1{
        id = <<"id">>,
        owner = <<"owner">>,
        amount = 1
    },
    ?assertEqual(SC, new(<<"id">>, <<"owner">>, 1)).

id_test() ->
    SC = new(<<"id">>, <<"owner">>, 1),
    ?assertEqual(<<"id">>, id(SC)),
    ?assertEqual(<<"id2">>, id(id(<<"id2">>, SC))).

owner_test() ->
    SC = new(<<"id">>, <<"owner">>, 1),
    ?assertEqual(<<"owner">>, owner(SC)),
    ?assertEqual(<<"owner2">>, owner(owner(<<"owner2">>, SC))).

amount_test() ->
    SC = new(<<"id">>, <<"owner">>, 1),
    ?assertEqual(1, amount(SC)),
    ?assertEqual(2, amount(amount(2, SC))).

-endif.
