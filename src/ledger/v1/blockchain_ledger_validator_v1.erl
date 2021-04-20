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
         add_penalty/5,
         penalties/1,
         calculate_penalty_value/2,
         status/1, status/2,
         nonce/1, nonce/2,
         version/1, version/2,
         serialize/1, deserialize/1,
         print/4,

         penalty_type/1,
         penalty_amount/1,
         penalty_height/1
        ]).

-include("blockchain.hrl").
-include("blockchain_vars.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type penalty() :: dkg | performance | tenure.

-record(penalty,
        {
         type :: penalty(),
         amount :: float(),
         height :: pos_integer()
        }).

-record(validator_v1,
        {
         address :: libp2p_crypto:pubkey_bin(),
         owner_address :: libp2p_crypto:pubkey_bin(),
         stake = 0 :: non_neg_integer(),
         heartbeat = 1 :: pos_integer(),
         nonce = 1 :: pos_integer(),
         version = 1 :: pos_integer(),
         status = staked :: status(),
         penalties = [] :: [#penalty{}]
        }).

-type status() :: staked | unstaked | cooldown.

-type validator() :: #validator_v1{}.

-export_type([validator/0, penalty/0]).

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

-spec add_penalty(Validator :: validator(),
                  Height :: pos_integer(),
                  Type :: penalty(),
                  Amount :: float(),
                  Limit :: pos_integer()) ->
          validator().
add_penalty(Validator, Height, Type, Amount, Limit) ->
    Recent0 = Validator#validator_v1.penalties,
    Recent = lists:filter(fun(#penalty{height = H}) -> (Height - H) =< Limit end, Recent0),
    Validator#validator_v1{penalties = lists:sort([#penalty{height = Height,
                                                            type = Type,
                                                            amount = Amount} | Recent])}.

-spec penalties(Validator :: validator()) -> [pos_integer()].
penalties(Validator) ->
    Validator#validator_v1.penalties.

-spec calculate_penalty_value(validator(), blockchain_ledger_v1:ledger()) -> non_neg_integer().
calculate_penalty_value(Val, Ledger) ->
    {ok, PenaltyLimit} = blockchain_ledger_v1:config(?penalty_history_limit, Ledger),
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
    Tot =
        lists:foldl(
          fun(#penalty{height = H, amount = Amt}, Acc) ->
                  BlocksAgo = Height - H,
                  case BlocksAgo >= PenaltyLimit of
                      true ->
                          Acc;
                      _ ->
                          %% 1 - ago/limit = linear inverse weighting for recency
                          Acc + (Amt * (1 - (BlocksAgo/PenaltyLimit)))
                  end
          end,
          0.0,
          penalties(Val)),
    blockchain_utils:normalize_float(Tot).

-spec serialize(Validator :: validator()) -> binary().
serialize(Validator) ->
    BinVal = erlang:term_to_binary(Validator, [compressed]),
    <<1, BinVal/binary>>.

-spec deserialize(binary()) -> validator().
deserialize(<<1, Bin/binary>>) ->
    erlang:binary_to_term(Bin).

-spec print(Validator :: validator(),
            Height :: pos_integer(),
            Verbose :: boolean(),
            Ledger :: blockchain_ledger_v1:ledger()) -> list().
print(Validator, Height, Verbose, Ledger) ->
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
         {penalty, calculate_penalty_value(Validator, Ledger)}
        ].

penalty_type(#penalty{type = Type}) ->
    Type.

penalty_amount(#penalty{amount = Amount}) ->
    Amount.

penalty_height(#penalty{height = Height}) ->
    Height.
