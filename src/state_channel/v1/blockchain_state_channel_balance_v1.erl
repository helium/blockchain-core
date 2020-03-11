%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel Balance ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channel_balance_v1).

-export([
    new/2,
    payee/1,
    balance/1,
    update/2
]).

-include_lib("helium_proto/include/blockchain_state_channel_v1_pb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.


-type balance() :: #blockchain_state_channel_balance_v1_pb{}.

-export_type([balance/0]).

-spec new(libp2p_crypto:pubkey_bin(), non_neg_integer()) -> balance().
new(Payee, Balance) ->
    #blockchain_state_channel_balance_v1_pb{
        payee=Payee,
        balance=Balance
    }.

-spec payee(balance()) -> libp2p_crypto:pubkey_bin().
payee(#blockchain_state_channel_balance_v1_pb{payee=Payee}) ->
    Payee.

-spec balance(balance()) -> non_neg_integer().
balance(#blockchain_state_channel_balance_v1_pb{balance=Balance}) ->
    Balance.

-spec update(balance(), [balance()]) -> [balance(),...].
update(NewBalance, Balances) ->
    lists:keystore(NewBalance#blockchain_state_channel_balance_v1_pb.payee,
                   #blockchain_state_channel_balance_v1_pb.payee,
                   Balances, NewBalance).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Balance = #blockchain_state_channel_balance_v1_pb{
        payee= <<"1">>,
        balance= 1
    },
    ?assertEqual(Balance, new(<<"1">>, 1)).

payee_test() ->
    SC = new(<<"1">>, 1),
    ?assertEqual(<<"1">>, payee(SC)).

balance_test() ->
    SC = new(<<"1">>, 1),
    ?assertEqual(1, balance(SC)).

update_test() ->
    SC1 = new(<<"1">>, 1),
    SC2 = new(<<"2">>, 1),
    SC3 = new(<<"1">>, 1),
    SC4 = new(<<"3">>, 1),
    ?assertEqual([SC3, SC2], update(SC3, [SC1, SC2])),
    ?assertEqual([SC1, SC2, SC4], update(SC4, [SC1, SC2])),
    ?assertEqual([SC3, SC2, SC4], update(SC3, update(SC4, [SC1, SC2]))),
    ok.

-endif.
