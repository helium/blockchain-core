%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Ledger validator ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_ledger_validator_v1).

-export([
         new/3,
         owner_address/1, owner_address/2,
         stake/1, stake/2,
         description/1, description/2,
         last_heartbeat/1, last_heartbeat/2,

         serialize/1, deserialize/1
        ]).

-import(blockchain_utils, [normalize_float/1]).

-include("blockchain.hrl").
-include("blockchain_vars.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(validator_v1,
        {
         address :: libp2p_crypto:pubkey_bin(),
         owner_address :: libp2p_crypto:pubkey_bin(),
         stake :: non_neg_integer(),
         description = <<>> :: string(),
         heartbeat = 1 :: pos_integer(),
         nonce = 1 :: pos_integer(),
         status = staked :: staked | unstaked
        }).

-type validator() :: #validator_v1{}.

-export_type([validator/0]).

-spec new(Address :: libp2p_crypto:pubkey_bin(),
          OwnerAddress :: libp2p_crypto:pubkey_bin(),
          Stake :: non_neg_integer()) ->
          validator().
new(Address, OwnerAddress, Stake) ->
    #validator_v1{
       address = Address,
       owner_address = OwnerAddress,
       stake = Stake
      }.

-spec owner_address(Validator :: validator()) -> libp2p_crypto:pubkey_bin().
owner_address(Validator) ->
    Validator#validator_v1.owner_address.

-spec owner_address(OwnerAddress :: libp2p_crypto:pubkey_bin(),
                    Validator :: validator()) -> validator().
owner_address(OwnerAddress, Validator) ->
    Validator#validator_v1{owner_address = OwnerAddress}.

-spec stake(Validator :: validator()) -> non_neg_integer().
stake(Validator) ->
    Validator#validator_v1.stake.

-spec stake(Stake :: non_neg_integer(),
            Validator :: validator()) -> validator().
stake(Stake, Validator) ->
    Validator#validator_v1{stake = Stake}.

-spec description(Validator :: validator()) -> non_neg_integer().
description(Validator) ->
    Validator#validator_v1.description.

-spec description(Description :: non_neg_integer(),
            Validator :: validator()) -> validator().
description(Description, Validator) ->
    Validator#validator_v1{description = Description}.

-spec last_heartbeat(Validator :: validator()) -> non_neg_integer().
last_heartbeat(Validator) ->
    Validator#validator_v1.heartbeat.

-spec last_heartbeat(Heartbeat :: non_neg_integer(),
            Validator :: validator()) -> validator().
last_heartbeat(Heartbeat, Validator) ->
    Validator#validator_v1{heartbeat = Heartbeat}.

-spec serialize(Validator :: validator()) -> binary().
serialize(Validator) ->
    BinVal = erlang:term_to_binary(Validator, [compressed]),
    <<1, BinVal/binary>>.

-spec deserialize(binary()) -> validator().
deserialize(<<1, Bin/binary>>) ->
    erlang:binary_to_term(Bin).
