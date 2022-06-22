%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Ledger Subnetwork V1 ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_ledger_subnetwork_v1).

-export([
    new/5,
    type/1,
    token_treasury/1, token_treasury/2,
    hnt_treasury/1,
    reward_server_keys/1,
    nonce/1, nonce/2,
    last_rewarded_block/1, last_rewarded_block/2,
    serialize/1,
    deserialize/1
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include_lib("helium_proto/include/blockchain_ledger_subnetwork_v1_pb.hrl").

-type subnetwork_v1() :: #blockchain_ledger_subnetwork_v1_pb{}.

-export_type([subnetwork_v1/0]).

%% ==================================================================
%% API Functions
%% ==================================================================

-spec new(
    TT :: blockchain_token_v1:type(),
    SNTreasury :: non_neg_integer(),
    HNTTreasury :: non_neg_integer(),
    SNKey :: libp2p_crypto:pubkey_bin(),
    RewardServerKeys :: [libp2p_crypto:pubkey_bin()]
) -> subnetwork_v1().
new(TT, SNTreasury, HNTTreasury, SNKey, RewardServerKeys) ->
    #blockchain_ledger_subnetwork_v1_pb{
        type = TT,
        token_treasury = SNTreasury,
        hnt_treasury = HNTTreasury,
        subnetwork_key = SNKey,
        reward_server_keys = lists:sort(RewardServerKeys)
    }.

-spec type(SN :: subnetwork_v1()) -> blockchain_token_v1:type().
type(#blockchain_ledger_subnetwork_v1_pb{type = Type}) ->
    Type.

-spec token_treasury(SN :: subnetwork_v1()) -> non_neg_integer().
token_treasury(#blockchain_ledger_subnetwork_v1_pb{token_treasury = SNT}) ->
    SNT.

-spec token_treasury(SN :: subnetwork_v1(), TokenTreasury :: non_neg_integer()) -> subnetwork_v1().
token_treasury(SN, TokenTreasury) ->
    SN#blockchain_ledger_subnetwork_v1_pb{token_treasury = TokenTreasury}.

-spec hnt_treasury(SN :: subnetwork_v1()) -> non_neg_integer().
hnt_treasury(#blockchain_ledger_subnetwork_v1_pb{hnt_treasury = SNHT}) ->
    SNHT.

-spec reward_server_keys(SN :: subnetwork_v1()) -> [libp2p_crypto:pubkey_bin()].
reward_server_keys(#blockchain_ledger_subnetwork_v1_pb{reward_server_keys = Keys}) ->
    Keys.

-spec last_rewarded_block(SN :: subnetwork_v1()) -> non_neg_integer().
last_rewarded_block(#blockchain_ledger_subnetwork_v1_pb{last_rewarded_block = LRB}) ->
    LRB.

-spec last_rewarded_block(SN :: subnetwork_v1(), LRB :: non_neg_integer()) -> subnetwork_v1().
last_rewarded_block(SN, LRB) ->
    SN#blockchain_ledger_subnetwork_v1_pb{last_rewarded_block = LRB}.

-spec nonce(SN :: subnetwork_v1()) -> non_neg_integer().
nonce(#blockchain_ledger_subnetwork_v1_pb{nonce = Nonce}) ->
    Nonce.

-spec nonce(SN :: subnetwork_v1(), Nonce :: non_neg_integer()) -> subnetwork_v1().
nonce(SN, Nonce) ->
    SN#blockchain_ledger_subnetwork_v1_pb{nonce = Nonce}.

-spec serialize(SN :: subnetwork_v1()) -> binary().
serialize(SN) ->
    blockchain_ledger_subnetwork_v1_pb:encode_msg(SN).

-spec deserialize(SNBin :: binary()) -> subnetwork_v1().
deserialize(SNBin) ->
    blockchain_ledger_subnetwork_v1_pb:decode_msg(SNBin, blockchain_ledger_subnetwork_v1_pb).
