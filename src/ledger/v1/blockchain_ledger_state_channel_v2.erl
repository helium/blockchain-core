%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Ledger State Channel v2 ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_ledger_state_channel_v2).

-export([
    new/6,
    id/1, id/2,
    owner/1, owner/2,
    nonce/1, nonce/2,
    amount/1, amount/2,
    original/1, original/2,
    expire_at_block/1, expire_at_block/2,
    serialize/1, deserialize/1,
    close_proposal/3,
    closer/1,
    state_channel/1,
    close_state/1,
    is_v2/1
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(ledger_state_channel_v2, {
    id :: blockchain_state_channel_v1:id(),
    owner :: libp2p_crypto:pubkey_bin(),
    expire_at_block :: pos_integer(),
    original :: non_neg_integer(),
    amount :: non_neg_integer(),
    nonce :: non_neg_integer(),
    closer :: libp2p_crypto:pubkey_bin(),
    sc :: undefined | blockchain_state_channel_v1:state_channel(),
    close_state :: closed | dispute | undefined
}).

-type state_channel_v2() :: #ledger_state_channel_v2{}.

-export_type([state_channel_v2/0]).

-spec new(ID :: blockchain_state_channel_v1:id(),
          Owner :: libp2p_crypto:pubkey_bin(),
          ExpireAtBlock :: pos_integer(),
          OriginalAmtDC :: non_neg_integer(),
          TotalAmtDC :: non_neg_integer(),
          Nonce :: non_neg_integer()) -> state_channel_v2().
new(ID, Owner, ExpireAtBlock, OriginalAmount, TotalAmount, Nonce) ->
    #ledger_state_channel_v2{
       id=ID,
       owner=Owner,
       expire_at_block=ExpireAtBlock,
       original=OriginalAmount,
       amount=TotalAmount,
       nonce=Nonce
      }.

-spec id(state_channel_v2()) -> blockchain_state_channel_v1:id().
id(#ledger_state_channel_v2{id=ID}) ->
    ID.

-spec id(ID :: blockchain_state_channel_v1:id(), SC :: state_channel_v2()) -> state_channel_v2().
id(ID, SC) ->
    SC#ledger_state_channel_v2{id=ID}.

-spec owner(state_channel_v2()) -> libp2p_crypto:pubkey_bin().
owner(#ledger_state_channel_v2{owner=Owner}) ->
    Owner.

-spec owner(Owner :: libp2p_crypto:pubkey_bin(), SC :: state_channel_v2()) -> state_channel_v2().
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

-spec original(state_channel_v2()) -> non_neg_integer().
original(#ledger_state_channel_v2{original=Original}) ->
    Original.

-spec original(Amount :: non_neg_integer(), SC :: state_channel_v2()) -> state_channel_v2().
original(Original, SC) ->
    SC#ledger_state_channel_v2{original=Original}.

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
            case is_sc_participant(Closer, SC) of
                false ->
                    %% just ignore this; leave it undefined
                    SCEntry;
                true ->
                    %% we've never gotten a close request for this before, so...
                    SCEntry#ledger_state_channel_v2{closer=Closer, sc=SC, close_state=closed}
            end;
        closed ->
            case is_sc_participant(Closer, SC) of
                false ->
                    %% ignore
                    SCEntry;
                true ->
                    %% ok so we've already marked this entry as closed... maybe we should
                    %% dispute it
                    case maybe_dispute(SC, state_channel(SCEntry)) of
                        closed ->
                            SCEntry#ledger_state_channel_v2{closer=Closer, sc=SC, close_state=closed};
                        {dispute, NewSC} ->
                            %% store the "latest" (as judged by nonce)
                            SCEntry#ledger_state_channel_v2{sc=NewSC, close_state=dispute}
                    end
            end;
        dispute ->
            %% already marked as dispute
            %% Check to see if the nonce is updated, if so replace
            CurrentNonce = blockchain_state_channel_v1:nonce(SC),
            PreviousNonce = blockchain_state_channel_v1:nonce(state_channel(SCEntry)),

            case CurrentNonce > PreviousNonce andalso is_sc_participant(Closer, SC) of
                true ->
                    SCEntry#ledger_state_channel_v2{sc=SC};
                false ->
                    %% ignore
                    SCEntry
            end
    end.

closer(SC) ->
    SC#ledger_state_channel_v2.closer.

state_channel(SC) ->
    SC#ledger_state_channel_v2.sc.

close_state(SC) ->
    SC#ledger_state_channel_v2.close_state.

is_v2(#ledger_state_channel_v2{}) -> true;
is_v2(_) -> false.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
maybe_dispute(SC, SC) -> closed;
maybe_dispute(CurrentSC, PreviousSC) ->
    CurrentNonce = blockchain_state_channel_v1:nonce(CurrentSC),
    PreviousNonce = blockchain_state_channel_v1:nonce(PreviousSC),
    if
        CurrentNonce > PreviousNonce -> closed;
        CurrentNonce < PreviousNonce -> {dispute, PreviousSC};
        true -> closed %% nonces are equal but should've been matched above
    end.

is_sc_participant(Closer, SC) ->
    Summaries = blockchain_state_channel_v1:summaries(SC),
    Clients = [blockchain_state_channel_summary_v1:client_pubkeybin(S) || S <- Summaries],
    Owner = blockchain_state_channel_v1:owner(SC),
    lists:member(Closer, [Owner | Clients]).

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
        original = 5,
        nonce = 1
    },
    ?assertEqual(SC, new(<<"id">>, <<"owner">>, 10, 5, 10, 1)).

id_test() ->
    SC = new(<<"id">>, <<"owner">>, 10, 5, 10, 1),
    ?assertEqual(<<"id">>, id(SC)),
    ?assertEqual(<<"id2">>, id(id(<<"id2">>, SC))).

owner_test() ->
    SC = new(<<"id">>, <<"owner">>, 10, 5, 10, 1),
    ?assertEqual(<<"owner">>, owner(SC)),
    ?assertEqual(<<"owner2">>, owner(owner(<<"owner2">>, SC))).

expire_at_block_test() ->
    SC = new(<<"id">>, <<"owner">>, 10, 5, 10, 1),
    ?assertEqual(10, expire_at_block(SC)),
    ?assertEqual(20, expire_at_block(expire_at_block(20, SC))).

nonce_test() ->
    SC = new(<<"id">>, <<"owner">>, 10, 5, 10, 1),
    ?assertEqual(1, nonce(SC)),
    ?assertEqual(2, nonce(nonce(2, SC))).

amount_test() ->
    SC = new(<<"id">>, <<"owner">>, 10, 5, 10, 1),
    ?assertEqual(10, amount(SC)),
    ?assertEqual(20, amount(amount(20, SC))).

original_test() ->
    SC = new(<<"id">>, <<"owner">>, 10, 5, 10, 1),
    ?assertEqual(5, original(SC)),
    ?assertEqual(7, original(original(7, SC))).

is_v2_test() ->
    SC = new(<<"id">>, <<"owner">>, 10, 5, 10, 1),
    ?assertEqual(true, is_v2(SC)),
    ?assertEqual(false, is_v2(<<"not v2">>)).

maybe_dispute_test() ->
    SC0 = blockchain_state_channel_v1:new(<<"id1">>, <<"key1">>, 100),
    SC1 = blockchain_state_channel_v1:new(<<"id2">>, <<"key2">>, 200),
    Nonce4 = blockchain_state_channel_v1:nonce(4, SC0),
    Nonce8 = blockchain_state_channel_v1:nonce(8, SC1),
    ?assertEqual(closed, maybe_dispute(SC0, SC0)),
    ?assertEqual(closed, maybe_dispute(Nonce8, Nonce4)),
    ?assertEqual({dispute, Nonce8}, maybe_dispute(Nonce4, Nonce8)).

is_sc_participant_test() ->
    Ids = [<<"key1">>, <<"key2">>, <<"key3">>],
    Summaries = [ blockchain_state_channel_summary_v1:new(I) || I <- Ids ],
    SC0 = blockchain_state_channel_v1:new(<<"id1">>, <<"owner">>, 100),
    SC1 = blockchain_state_channel_v1:summaries(Summaries, SC0),
    ?assertEqual(false, is_sc_participant(<<"nope">>, SC1)),
    ?assertEqual(true, is_sc_participant(<<"key2">>, SC1)),
    ?assertEqual(true, is_sc_participant(<<"owner">>, SC1)).

-endif.
