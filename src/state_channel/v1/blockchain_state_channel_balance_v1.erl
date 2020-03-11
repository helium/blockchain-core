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
    update/3
]).

-include_lib("helium_proto/include/blockchain_state_channel_v1_pb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type balance() :: #blockchain_state_channel_balance_v1_pb{}.
-type balances() :: [balance()].

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

-spec update(Payee :: libp2p_crypto:pubkey_bin(),
             Balance :: non_neg_integer(),
             Balances :: balances()) -> balances().
update(Payee, Balance, Balances) ->
    NewBalance = ?MODULE:new(Payee, Balance),
    lists:keystore(Payee, 2, Balances, NewBalance).

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
    Balance = new(<<"1">>, 1),
    ?assertEqual(<<"1">>, payee(Balance)).

balance_test() ->
    Balance = new(<<"1">>, 1),
    ?assertEqual(1, balance(Balance)).

update_test() ->
    %% Initial balances
    B1 = new(<<"1">>, 1),
    B2 = new(<<"2">>, 1),

    %% New balance for "1"
    B3 = new(<<"1">>, 2),

    OldBalances = [B1, B2],
    NewBalances = update(<<"1">>, 2, OldBalances),
    Expected = [B3, B2],
    ?assertEqual(Expected, NewBalances),
    ?assertEqual(2, length(NewBalances)),
    ok.

-endif.
