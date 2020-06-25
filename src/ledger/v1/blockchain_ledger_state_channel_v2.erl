%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Ledger State Channel v2 ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_ledger_state_channel_v2).

-export([
    new/5,
    id/1, id/2,
    owner/1, owner/2,
    nonce/1, nonce/2,
    amount/1, amount/2,
    expire_at_block/1, expire_at_block/2,
    serialize/1, deserialize/1,
    close_proposal/3,
    closer/1,
    state_channel/1,
    close_state/1
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(ledger_state_channel_v2, {
    id :: binary(),
    owner :: binary(),
    expire_at_block :: pos_integer(),
    amount :: non_neg_integer(),
    nonce :: non_neg_integer(),
    closer :: libp2p_crypto:pubkey_bin(),
    sc :: blockchain_state_channel_v1:state_channel(),
    close_state :: closed | dispute
}).

-type state_channel_v2() :: #ledger_state_channel_v2{}.

-export_type([state_channel_v2/0]).

-spec new(ID :: binary(),
          Owner :: binary(),
          ExpireAtBlock :: pos_integer(),
          Amount :: non_neg_integer(),
          Nonce :: non_neg_integer()) -> state_channel_v2().
new(ID, Owner, ExpireAtBlock, Amount, Nonce) ->
    #ledger_state_channel_v2{
       id=ID,
       owner=Owner,
       expire_at_block=ExpireAtBlock,
       amount=Amount,
       nonce=Nonce
      }.

-spec id(state_channel_v2()) -> binary().
id(#ledger_state_channel_v2{id=ID}) ->
    ID.

-spec id(ID :: binary(), SC :: state_channel_v2()) -> state_channel_v2().
id(ID, SC) ->
    SC#ledger_state_channel_v2{id=ID}.

-spec owner(state_channel_v2()) -> binary().
owner(#ledger_state_channel_v2{owner=Owner}) ->
    Owner.

-spec owner(Owner :: binary(), SC :: state_channel_v2()) -> state_channel_v2().
owner(Owner, SC) ->
    SC#ledger_state_channel_v2{owner=Owner}.

-spec expire_at_block(state_channel_v2()) -> pos_integer().
expire_at_block(#ledger_state_channel_v2{expire_at_block=ExpireAtBlock}) ->
    ExpireAtBlock.

-spec expire_at_block(ExpireAtBlock :: pos_integer(), SC :: state_channel_v2()) -> state_channel_v2().
expire_at_block(ExpireAtBlock, SC) ->
    SC#ledger_state_channel_v2{expire_at_block=ExpireAtBlock}.

-spec nonce(state_channel_v2()) -> non_neg_integer().
nonce(#ledger_state_channel_v2{nonce=Nonce}) ->
    Nonce.

-spec nonce(Nonce :: non_neg_integer(), SC :: state_channel_v2()) -> state_channel_v2().
nonce(Nonce, SC) ->
    SC#ledger_state_channel_v2{nonce=Nonce}.

-spec amount(state_channel_v2()) -> non_neg_integer().
amount(#ledger_state_channel_v2{amount=Amount}) ->
    Amount.

-spec amount(Amount :: non_neg_integer(), SC :: state_channel_v2()) -> state_channel_v2().
amount(Amount, SC) ->
    SC#ledger_state_channel_v2{amount=Amount}.

%%--------------------------------------------------------------------
%% @doc
%% Version 2
%% @end
%%--------------------------------------------------------------------
-spec serialize(state_channel_v2()) -> binary().
serialize(SC) ->
    BinSC = erlang:term_to_binary(SC, [compressed]),
    <<2, BinSC/binary>>.

%%--------------------------------------------------------------------
%% @doc
%% Deserialize for v2
%% @end
%%--------------------------------------------------------------------
-spec deserialize(binary()) -> state_channel_v2().
deserialize(<<2, Bin/binary>>) ->
    erlang:binary_to_term(Bin).

-spec close_proposal( Closer :: libp2p_crypto:pubkey_bin(),
                      StateChannel :: blockchain_state_channel_v1:state_channel(),
                      SCEntry :: state_channel_v2() ) -> state_channel_v2().
close_proposal(Closer, SC, SCEntry) ->
    case close_state(SCEntry) of
        undefined ->
            %% we've never gotten a close request for this before, so...
            SCEntry#ledger_state_channel_v2{closer=Closer, sc=SC, close_state=closed};
        closed ->
            %% ok so we've already marked this entry as closed... maybe we should
            %% dispute it
            case maybe_dispute(Closer, closer(SCEntry), SC, state_channel(SCEntry)) of
                closed ->
                    SCEntry#ledger_state_channel_v2{closer=Closer, sc=SC, close_state=closed};
                {dispute, NewCloser, NewSC} ->
                    %% store the "latest" (as judged by nonce)
                    SCEntry#ledger_state_channel_v2{closer=NewCloser, sc=NewSC, close_state=dispute}
            end;
        dispute ->
            %% already marked as dispute, just keep it that way
            SCEntry
    end.

closer(SC) ->
    SC#ledger_state_channel_v2.closer.

state_channel(SC) ->
    SC#ledger_state_channel_v2.sc.

close_state(SC) ->
    SC#ledger_state_channel_v2.close_state.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
maybe_dispute(Closer, Closer, SC, SC) -> closed;
maybe_dispute(Closer, Closer, CurrentSC, PreviousSC) ->
    CurrentNonce = blockchain_state_channel_v1:nonce(CurrentSC),
    PreviousNonce = blockchain_state_channel_v1:nonce(PreviousSC),
    if
        CurrentNonce > PreviousNonce -> closed;
        CurrentNonce < PreviousNonce -> {dispute, Closer, PreviousSC};
        true -> closed
    end;
%% this could happen when some other actor sends a close that arrives
%% late and has a more recent view of the state channel than the one we
%% have in our ledger.
maybe_dispute(_Closer, Other, CurrentSC, PreviousSC) ->
    CurrentNonce = blockchain_state_channel_v1:nonce(CurrentSC),
    PreviousNonce = blockchain_state_channel_v1:nonce(PreviousSC),
    if
        CurrentNonce > PreviousNonce -> {dispute, Other, CurrentSC};
        CurrentNonce < PreviousNonce -> closed;
        true -> closed
    end.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    SC = #ledger_state_channel_v2{
        id = <<"id">>,
        owner = <<"owner">>,
        expire_at_block = 10,
        amount = 10,
        nonce = 1
    },
    ?assertEqual(SC, new(<<"id">>, <<"owner">>, 10, 10, 1)).

id_test() ->
    SC = new(<<"id">>, <<"owner">>, 10, 10, 1),
    ?assertEqual(<<"id">>, id(SC)),
    ?assertEqual(<<"id2">>, id(id(<<"id2">>, SC))).

owner_test() ->
    SC = new(<<"id">>, <<"owner">>, 10, 10, 1),
    ?assertEqual(<<"owner">>, owner(SC)),
    ?assertEqual(<<"owner2">>, owner(owner(<<"owner2">>, SC))).

expire_at_block_test() ->
    SC = new(<<"id">>, <<"owner">>, 10, 10, 1),
    ?assertEqual(10, expire_at_block(SC)),
    ?assertEqual(20, expire_at_block(expire_at_block(20, SC))).

nonce_test() ->
    SC = new(<<"id">>, <<"owner">>, 10, 10, 1),
    ?assertEqual(1, nonce(SC)),
    ?assertEqual(2, nonce(nonce(2, SC))).

amount_test() ->
    SC = new(<<"id">>, <<"owner">>, 10, 10, 1),
    ?assertEqual(10, amount(SC)),
    ?assertEqual(20, amount(amount(20, SC))).

maybe_dispute_test() ->
    SC0 = blockchain_state_channel_v1:new(<<"id1">>, <<"key1">>, 100),
    SC1 = blockchain_state_channel_v1:new(<<"id2">>, <<"key2">>, 200),
    Nonce4 = blockchain_state_channel_v1:nonce(4, SC0),
    Nonce8 = blockchain_state_channel_v1:nonce(8, SC1),
    Closer = <<"closer">>,
    ?assertEqual(closed, maybe_dispute(Closer, Closer, SC0, SC0)),
    ?assertEqual(closed, maybe_dispute(Closer, Closer, Nonce8, Nonce4)),
    ?assertEqual({dispute, Closer, Nonce8}, maybe_dispute(Closer, Closer, Nonce4, Nonce8)),
    ?assertEqual(closed, maybe_dispute(Closer, <<"other">>, Nonce4, Nonce8)),
    ?assertEqual({dispute, <<"other">>, Nonce8}, maybe_dispute(Closer, <<"other">>, Nonce8, Nonce4)).

-endif.
