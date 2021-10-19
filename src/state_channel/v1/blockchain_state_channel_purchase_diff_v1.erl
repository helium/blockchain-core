%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel Purchase Diff ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channel_purchase_diff_v1).

-export([
    new/4,
    summary/1,
    packet_hash/1,
    sc_owner/1,
    region/1,
    signature/1, sign/2,
    encode/1, decode/1
]).

-include("blockchain.hrl").
-include_lib("helium_proto/include/blockchain_state_channel_v1_pb.hrl").

-type purchase_diff() :: #blockchain_state_channel_purchase_diff_v1_pb{}.
-export_type([purchase_diff/0]).

-spec new(
    Summary :: blockchain_state_channel_summary_v1:summary(),
    PacketHash :: binary(),
    Region :: atom(),
    SCOwner :: libp2p_crypto:pubkey_bin()
) -> purchase_diff().
new(Summary, PacketHash, Region, SCOwner) ->
    #blockchain_state_channel_purchase_diff_v1_pb{
       summary=Summary,
       packet_hash=PacketHash,
       region=Region,
       sc_owner=SCOwner
    }.

-spec summary(purchase_diff()) -> blockchain_state_channel_summary_v1:summary().
summary(#blockchain_state_channel_purchase_diff_v1_pb{summary=Summary}) ->
    Summary.

-spec packet_hash(purchase_diff()) -> binary().
packet_hash(#blockchain_state_channel_purchase_diff_v1_pb{packet_hash=PH}) ->
    PH.

-spec region(purchase_diff()) -> binary().
region(#blockchain_state_channel_purchase_diff_v1_pb{region=Region}) ->
    Region.

-spec sc_owner(purchase_diff()) -> binary().
sc_owner(#blockchain_state_channel_purchase_diff_v1_pb{sc_owner=SCOwner}) ->
    SCOwner.

-spec signature(purchase_diff()) -> binary().
signature(#blockchain_state_channel_purchase_diff_v1_pb{signature=Signature}) ->
    Signature.

-spec sign(purchase_diff(), function()) -> purchase_diff().
sign(PD, SigFun) ->
    EncodedPD = ?MODULE:encode(PD#blockchain_state_channel_purchase_diff_v1_pb{signature= <<>>}),
    Signature = SigFun(EncodedPD),
    PD#blockchain_state_channel_purchase_diff_v1_pb{signature=Signature}.

-spec encode(purchase_diff()) -> binary().
encode(#blockchain_state_channel_purchase_diff_v1_pb{}=PurchaseDiff) ->
    blockchain_state_channel_v1_pb:encode_msg(PurchaseDiff).

-spec decode(binary()) -> purchase_diff().
decode(BinaryPurchaseDiff) ->
    blockchain_state_channel_v1_pb:decode_msg(BinaryPurchaseDiff, blockchain_state_channel_purchase_diff_v1_pb).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

new_test() ->
    Summary = blockchain_state_channel_summary_v1:new(<<"scid">>),
    PH = <<"packet_hash">>,
    Region = 'US915',
    SCOwner = <<"sc_owner">>,
    Purchase =
        #blockchain_state_channel_purchase_diff_v1_pb{
            summary = Summary,
            packet_hash = PH,
            region = Region,
            sc_owner=SCOwner
        },
    ?assertEqual(Purchase, new(Summary, PH, Region, SCOwner)).

encode_decode_test() ->
    Summary = blockchain_state_channel_summary_v1:new(<<"scid">>),
    PurchaseDiff = new(Summary, <<"packet_hash">>, 'US915', <<"sc_owner">>),
    ?assertEqual(PurchaseDiff, decode(encode(PurchaseDiff))).

-endif.
