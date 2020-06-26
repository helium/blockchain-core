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
    nonce/1, nonce/2,
    expire_at_block/1, expire_at_block/2,
    serialize/1, deserialize/1,
    is_v1/1
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(ledger_state_channel_v1, {
    id :: binary(),
    owner :: binary(),
    expire_at_block :: pos_integer(),
    nonce :: non_neg_integer()
}).

-type state_channel() :: #ledger_state_channel_v1{}.

-export_type([state_channel/0]).

-spec new(ID :: binary(),
          Owner :: binary(),
          ExpireAtBlock :: pos_integer(),
          Nonce :: non_neg_integer()) -> state_channel().
new(ID, Owner, ExpireAtBlock, Nonce) ->
    #ledger_state_channel_v1{
       id=ID,
       owner=Owner,
       expire_at_block=ExpireAtBlock,
       nonce=Nonce
      }.

-spec id(state_channel()) -> binary().
id(#ledger_state_channel_v1{id=ID}) ->
    ID.

-spec id(ID :: binary(), SC :: state_channel()) -> state_channel().
id(ID, SC) ->
    SC#ledger_state_channel_v1{id=ID}.

-spec owner(state_channel()) -> binary().
owner(#ledger_state_channel_v1{owner=Owner}) ->
    Owner.

-spec owner(Owner :: binary(), SC :: state_channel()) -> state_channel().
owner(Owner, SC) ->
    SC#ledger_state_channel_v1{owner=Owner}.

-spec expire_at_block(state_channel()) -> pos_integer().
expire_at_block(#ledger_state_channel_v1{expire_at_block=ExpireAtBlock}) ->
    ExpireAtBlock.

-spec expire_at_block(ExpireAtBlock :: pos_integer(), SC :: state_channel()) -> state_channel().
expire_at_block(ExpireAtBlock, SC) ->
    SC#ledger_state_channel_v1{expire_at_block=ExpireAtBlock}.

-spec nonce(state_channel()) -> non_neg_integer().
nonce(#ledger_state_channel_v1{nonce=Nonce}) ->
    Nonce.

-spec nonce(Nonce :: non_neg_integer(), SC :: state_channel()) -> state_channel().
nonce(Nonce, SC) ->
    SC#ledger_state_channel_v1{nonce=Nonce}.

-spec is_v1(state_channel()) -> boolean().
is_v1(#ledger_state_channel_v1{}) -> true;
is_v1(_) -> false.

%%--------------------------------------------------------------------
%% @doc
%% Version 1
%% @end
%%--------------------------------------------------------------------
-spec serialize(state_channel()) -> binary().
serialize(SC) ->
    BinSC = erlang:term_to_binary(SC, [compressed]),
    <<1, BinSC/binary>>.

%%--------------------------------------------------------------------
%% @doc
%% Deserialize for v1
%% @end
%%--------------------------------------------------------------------
-spec deserialize(binary()) -> state_channel().
deserialize(<<1, Bin/binary>>) ->
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
        expire_at_block = 10,
        nonce = 1
    },
    ?assertEqual(SC, new(<<"id">>, <<"owner">>, 10, 1)).

id_test() ->
    SC = new(<<"id">>, <<"owner">>, 10, 1),
    ?assertEqual(<<"id">>, id(SC)),
    ?assertEqual(<<"id2">>, id(id(<<"id2">>, SC))).

owner_test() ->
    SC = new(<<"id">>, <<"owner">>, 10, 1),
    ?assertEqual(<<"owner">>, owner(SC)),
    ?assertEqual(<<"owner2">>, owner(owner(<<"owner2">>, SC))).

expire_at_block_test() ->
    SC = new(<<"id">>, <<"owner">>, 10, 1),
    ?assertEqual(10, expire_at_block(SC)),
    ?assertEqual(20, expire_at_block(expire_at_block(20, SC))).

nonce_test() ->
    SC = new(<<"id">>, <<"owner">>, 10, 1),
    ?assertEqual(1, nonce(SC)),
    ?assertEqual(2, nonce(nonce(2, SC))).

is_v1_test() ->
    SC = new(<<"id">>, <<"owner">>, 10, 1),
    ?assertEqual(true, is_v1(SC)),
    ?assertEqual(false, is_v1(<<"not v1">>)).

-endif.
