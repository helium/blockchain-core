%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel Purchase ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channel_purchase_v1).

-export([
    new/3,
    signature/1, sign/2,
    encode/1, decode/1
]).

-include("blockchain.hrl").
-include_lib("helium_proto/include/blockchain_state_channel_v1_pb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type purchase() :: #blockchain_state_channel_purchase_v1_pb{}.
-export_type([purchase/0]).

-spec new(SC :: blockchain_state_channel_v1:state_channel(),
          Hotspot :: libp2p_crypto:pubkey_bin(),
          Region :: atom()) -> purchase().
new(SC, Hotspot, Region) ->
    #blockchain_state_channel_purchase_v1_pb{
       sc=SC,
       hotspot=Hotspot,
       signature = <<>>,
       region=Region
    }.

-spec signature(purchase()) -> binary().
signature(#blockchain_state_channel_purchase_v1_pb{signature=Signature}) ->
    Signature.

-spec sign(purchase(), function()) -> purchase().
sign(Purchase, SigFun) ->
    EncodedReq = ?MODULE:encode(Purchase#blockchain_state_channel_purchase_v1_pb{signature= <<>>}),
    Signature = SigFun(EncodedReq),
    Purchase#blockchain_state_channel_purchase_v1_pb{signature=Signature}.

-spec encode(purchase()) -> binary().
encode(#blockchain_state_channel_purchase_v1_pb{}=Purchase) ->
    blockchain_state_channel_v1_pb:encode_msg(Purchase).

-spec decode(binary()) -> purchase().
decode(BinaryPurchase) ->
    blockchain_state_channel_v1_pb:decode_msg(BinaryPurchase, blockchain_state_channel_purchase_v1_pb).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    SC = blockchain_state_channel_v1:new(<<"scid">>, <<"owner">>),
    Hotspot = <<"hotspot">>,
    Region = 'US915',
    Purchase = #blockchain_state_channel_purchase_v1_pb{sc = SC, hotspot = Hotspot, region = Region},
    ?assertEqual(Purchase, new(SC, Hotspot, Region)).

encode_decode_test() ->
    SC = blockchain_state_channel_v1:new(<<"scid">>, <<"owner">>),
    Purchase = new(SC, <<"hotspot">>, 'US915'),
    ?assertEqual(Purchase, decode(encode(Purchase))).

-endif.
