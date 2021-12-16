%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Implicit Burn ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_implicit_burn).

-include("blockchain_json.hrl").

-export([
    new/2,
    fee/1, fee/2,
    payer/1, payer/2,
    serialize/1, deserialize/1,
    to_json/2
]).

-record(implicit_burn, {
    fee :: non_neg_integer(),
    payer :: libp2p_crypto:pubkey_bin()
}).

-type implicit_burn() :: #implicit_burn{}.

-export_type([implicit_burn/0]).

-spec new(non_neg_integer(), libp2p_crypto:pubkey_bin()) -> implicit_burn().
new(Fee, Payer) ->
    #implicit_burn{fee=Fee, payer=Payer}.

-spec fee(implicit_burn()) -> non_neg_integer().
fee(#implicit_burn{fee=Fee}) ->
    Fee.

-spec fee(non_neg_integer(), implicit_burn()) -> implicit_burn().
fee(Fee, ImplicitBurn) ->
    ImplicitBurn#implicit_burn{fee=Fee}.

-spec payer(implicit_burn()) -> libp2p_crypto:pubkey_bin().
payer(#implicit_burn{payer=Payer}) ->
    Payer.

-spec payer(libp2p_crypto:pubkey_bin(), implicit_burn()) -> implicit_burn().
payer(Payer, ImplicitBurn) ->
    ImplicitBurn#implicit_burn{payer=Payer}.

-spec serialize(implicit_burn()) -> binary().
serialize(ImplicitBurn) ->
    BinEntry = erlang:term_to_binary(ImplicitBurn, [compressed]),
    <<1, BinEntry/binary>>.

-spec deserialize(binary()) -> implicit_burn().
deserialize(<<_:1/binary, Bin/binary>>) ->
    erlang:binary_to_term(Bin).

-spec to_json(implicit_burn(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(ImplicitBurn, _Opts) ->
    #{
      fee => fee(ImplicitBurn),
      payer => ?BIN_TO_B58(payer(ImplicitBurn))
    }.