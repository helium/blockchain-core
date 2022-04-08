-module(blockchain_txn_contract).

-export_type([
    t/0,
    txn_type/0
]).

-export([
    sig/0,
    addr/0,
    txn/0,
    txn/1,
    h3_string/0
]).

-type t() :: data_contract:t().

-type txn_type() ::
    any | {type, atom()}.

%-type failure_txn() ::
%      {not_a_txn, val()}
%    | {txn_wrong_type, Actual :: atom(), Required :: atom()}
%    | {txn_malformed, val()}
%    .

%% API ========================================================================

-spec sig() -> data_contract:t().
sig() ->
    {either, [
        {binary, {exactly, 0}},  % Unsigned.
        {binary, {min, sig_min_non_empty()}}
        %% single ed25519     : 64
        %% single ecc_compact : 70-72
        %% multisig           : 3 + (unbounded, at least one of the above)
    ]}.

-spec addr() -> data_contract:t().
addr() ->
    Test =
        fun (Val) ->
            try libp2p_crypto:bin_to_pubkey(Val) of
                _ -> true
            catch
                _:_ -> false
            end
        end,
    Error = invalid_address,
    {custom, Test, Error}.

-spec txn() -> data_contract:t().
txn() ->
    txn(any).

-spec txn(txn_type()) -> data_contract:t().
txn(Type) ->
    Test =
        fun (Val) ->
            Result =
                case blockchain_txn:type_check(Val) of
                    {error, not_a_known_txn_value} ->
                        {error, {not_a_txn, Val}};
                    {ok, TypeActual} ->
                        TypeRequired =
                            case Type of
                                any -> TypeActual;
                                {type, T} -> T
                            end,
                        case TypeActual =:= TypeRequired of
                            true ->
                                case TypeActual:is_well_formed(Val) of
                                    ok -> ok;
                                    {error, _} -> {error, {txn_malformed, Val}}
                                end;
                            false ->
                                {error, {txn_wrong_type, TypeActual, TypeRequired}}
                        end
                end,
            %% TODO Remove boolean conversion after {custom, _, _} removal.
            data_result:is_ok(Result)
        end,
    Error = txn_invalid_or_wrong_type,
    {custom, Test, Error}.

-spec h3_string() -> data_contract:t().
h3_string() ->
    Test =
        fun (Val) ->
            Result =
                try h3:from_string(Val) of
                    _ -> ok
                catch
                    _:_ -> {error, invalid_h3_string}
                end,
            %% TODO Remove boolean conversion after {custom, _, _} removal.
            data_result:is_ok(Result)
        end,
    Error = invalid_h3_string,
    {custom, Test, Error}.

%% Internal ===================================================================

sig_min_non_empty() -> 64. % TODO Chain var?

%% Test =======================================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

signature_test_() ->
    Data = <<"data">>,
    Min = sig_min_non_empty(),

    %% Either empty of above minimum:
    SigGoodEmpty = <<>>,
    SigGoodReal1 = (t_user:sig_fun(t_user:new(ecc_compact)))(Data),
    SigGoodReal2 = (t_user:sig_fun(t_user:new(ed25519)))(Data),
    SigGoodFake1 = list_to_binary(lists:duplicate(1024, 0)),
    SigGoodFake2 = list_to_binary(lists:duplicate(Min, 0)),

    %% Non-empty, but bellow minimum:
    SigBad1 = list_to_binary(lists:duplicate(Min - 1, 0)),
    SigBad2 = list_to_binary(lists:duplicate(Min div 2, 0)),

    [
        ?_assertEqual(ok, data_contract:check(SigGoodEmpty, sig())),
        ?_assertEqual(ok, data_contract:check(SigGoodReal1, sig())),
        ?_assertEqual(ok, data_contract:check(SigGoodReal2, sig())),
        ?_assertEqual(ok, data_contract:check(SigGoodFake1, sig())),
        ?_assertEqual(ok, data_contract:check(SigGoodFake2, sig())),
        ?_assertMatch({error, {contract_breach, _}}, data_contract:check(SigBad1, sig())),
        ?_assertMatch({error, {contract_breach, _}}, data_contract:check(SigBad2, sig()))
    ].

address_test_() ->
    Good = t_user:addr(t_user:new()),
    Bad  = <<"eggplant", Good/binary>>,
    [
        ?_assertEqual(ok, data_contract:check(Good, addr())),
        ?_assertEqual(
            {error, {contract_breach, {invalid_address, Bad}}},
            data_contract:check(Bad, addr())
        ),
        ?_assertEqual(
            ok,
            data_contract:check(
                Good,
                {forall, [
                    defined,
                    {binary, any},
                    {binary, {range, 0, 1024}},
                    {binary, {exactly, 33}},
                    addr()
                ]}
            )
        )
    ].

txn_test_() ->
    Addr = t_user:addr(t_user:new()),
    Type = blockchain_txn_add_gateway_v1,
    Txn  = Type:new(Addr, Addr),
    TxnMalformed = Type:new(<<"not addr">>, Addr),
    [
        ?_assertEqual(
            {error, {contract_breach, {txn_invalid_or_wrong_type, trust_me_im_a_txn}}},
            data_contract:check(trust_me_im_a_txn, txn(any))
        ),
        ?_assertEqual(ok, data_contract:check(Txn, txn(any))),
        ?_assertEqual(ok, data_contract:check(Txn, txn({type, Type}))),
        ?_assertEqual(
            {error, {contract_breach, {txn_invalid_or_wrong_type, Txn}}},
            data_contract:check(Txn, txn({type, not_a_txn_type}))
        ),
        ?_assertEqual(
            {error, {contract_breach, {txn_invalid_or_wrong_type, TxnMalformed}}},
            data_contract:check(TxnMalformed, txn(any))
        )
    ].

h3_string_test_() ->
    Good = h3:to_string(h3:from_geo({48.8566, 2.3522}, 9)),
    Bad = "garbage",
    [
        ?_assertMatch(ok, data_contract:check(Good, h3_string())),
        ?_assertMatch(
            {error, {contract_breach, {invalid_h3_string, Bad}}},
            data_contract:check(Bad, h3_string())
        )
    ].

-endif.
