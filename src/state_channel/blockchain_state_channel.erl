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
    packets/1, packets/2
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

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).



-endif.