%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Ledger validator ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_ledger_validator_v1).

-export([
         new/3,
         address/1, address/2,
         owner_address/1, owner_address/2,
         stake/1, stake/2,
         last_heartbeat/1, last_heartbeat/2,
         penalty/1, penalty/2,
         add_recent_failure/4,
         recent_failures/1,
         status/1, status/2,
         nonce/1, nonce/2,
         version/1, version/2,
         serialize/1, deserialize/1,
         print/3
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
         stake = 0 :: non_neg_integer(),
         heartbeat = 1 :: pos_integer(),
         nonce = 1 :: pos_integer(),
         version = 1 :: pos_integer(),
         status = staked :: status(),
         penalty = 0.0 :: float(),
         recent_failures = [] :: [pos_integer()]
        }).

-type status() :: staked | unstaked | cooldown.

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

-spec address(Validator :: validator()) -> libp2p_crypto:pubkey_bin().
address(Validator) ->
    Validator#validator_v1.address.

-spec address(Address :: libp2p_crypto:pubkey_bin(),
              Validator :: validator()) -> validator().
address(Address, Validator) ->
    Validator#validator_v1{address = Address}.

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

-spec version(Validator :: validator()) -> pos_integer().
version(Validator) ->
    Validator#validator_v1.version.

-spec version(Version :: pos_integer(),
            Validator :: validator()) -> validator().
version(Version, Validator) ->
    Validator#validator_v1{version = Version}.

-spec last_heartbeat(Validator :: validator()) -> non_neg_integer().
last_heartbeat(Validator) ->
    Validator#validator_v1.heartbeat.

-spec last_heartbeat(Heartbeat :: non_neg_integer(),
            Validator :: validator()) -> validator().
last_heartbeat(Heartbeat, Validator) ->
    Validator#validator_v1{heartbeat = Heartbeat}.

-spec nonce(Validator :: validator()) -> non_neg_integer().
nonce(Validator) ->
    Validator#validator_v1.nonce.

-spec nonce(Nonce :: non_neg_integer(),
            Validator :: validator()) -> validator().
nonce(Nonce, Validator) ->
    Validator#validator_v1{nonce = Nonce}.

-spec status(Validator :: validator()) -> status().
status(Validator) ->
    Validator#validator_v1.status.

-spec status(Status :: status(),
            Validator :: validator()) -> validator().
status(Status, Validator) ->
    Validator#validator_v1{status = Status}.

-spec penalty(Validator :: validator()) -> float().
penalty(Validator) ->
    Validator#validator_v1.penalty.

-spec penalty(Penalty :: float(),
              Validator :: validator()) -> validator().
penalty(Penalty, Validator) ->
    Validator#validator_v1{penalty = Penalty}.

-spec add_recent_failure(Validator :: validator(),
                         Height :: pos_integer(),
                         Delay :: pos_integer(),
                         Ledger :: blockchain:ledger()) ->
          validator().
add_recent_failure(Validator, Height, Delay, Ledger) ->
    Recent0 = Validator#validator_v1.recent_failures,
    {ok, Limit} = blockchain_ledger_v1:config(?penalty_history_limit, Ledger),
    Recent = lists:filter(fun({H, _D}) -> (Height - H) =< Limit end, Recent0),
    Validator#validator_v1{recent_failures = lists:sort([{Height, Delay} | Recent])}.

-spec recent_failures(Validator :: validator()) -> [pos_integer()].
recent_failures(Validator) ->
    Validator#validator_v1.recent_failures.

-spec serialize(Validator :: validator()) -> binary().
serialize(Validator) ->
    BinVal = erlang:term_to_binary(Validator, [compressed]),
    <<1, BinVal/binary>>.

-spec deserialize(binary()) -> validator().
deserialize(<<1, Bin/binary>>) ->
    erlang:binary_to_term(Bin).

-spec print(Validator :: validator(),
            Height :: pos_integer(),
            Verbose :: boolean()) -> list().
print(Validator, Height, Verbose) ->
    case Verbose of
        true ->
            [{validator_address, libp2p_crypto:bin_to_b58(address(Validator))},
             {nonce, nonce(Validator)}];
        false -> []
    end ++
        [
         {owner_address, libp2p_crypto:bin_to_b58(owner_address(Validator))},
         {last_heartbeat, Height - last_heartbeat(Validator)},
         {stake, stake(Validator)},
         {status, status(Validator)},
         {version, version(Validator)},
         {failures, length(recent_failures(Validator))}
        ].
