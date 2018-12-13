%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Ledger ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_ledger_v1).

-export([
    new/0,
    current_height/1, increment_height/1,
    transaction_fee/1, update_transaction_fee/1,
    consensus_members/1, consensus_members/2,
    active_gateways/1,
    entries/1,
    htlcs/1,

    find_gateway_info/2,
    add_gateway/3, add_gateway/7,
    add_gateway_location/4,
    request_poc/2,

    find_entry/2,
    credit_account/3,
    debit_account/4,
    debit_fee/3,

    find_htlc/2,
    add_htlc/7,
    redeem_htlc/3,
    
    serialize/1, deserialize/1
]).

-include("blockchain.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(ledger_v1, {
    current_height = undefined :: undefined | pos_integer(),
    transaction_fee = 0 :: non_neg_integer(),
    consensus_members = [] :: [libp2p_crypto:address()],
    active_gateways = #{} :: active_gateways(),
    entries = #{} :: entries(),
    htlcs = #{} :: htlcs()
}).

-type ledger() :: #ledger_v1{}.
-type entries() :: #{libp2p_crypto:address() => blockchain_ledger_entry_v1:entry()}.
-type active_gateways() :: #{libp2p_crypto:address() => blockchain_ledger_gateway_v1:gateway()}.
-type htlcs() :: #{libp2p_crypto:address() => blockchain_ledger_htlc_v1:htlc()}.

-export_type([ledger/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new() -> ledger().
new() ->
    #ledger_v1{}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec current_height(ledger()) -> undefined | pos_integer().
current_height(Ledger) ->
    Ledger#ledger_v1.current_height.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec increment_height(ledger()) -> ledger().
increment_height(#ledger_v1{current_height=Height}=Ledger) when Height /= undefined ->
    Ledger#ledger_v1{current_height=(Height + 1)};
increment_height(Ledger) ->
    Ledger#ledger_v1{current_height=1}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec transaction_fee(ledger()) -> non_neg_integer().
transaction_fee(#ledger_v1{transaction_fee=Fee}) ->
    Fee.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec update_transaction_fee(ledger()) -> ledger().
update_transaction_fee(#ledger_v1{transaction_fee=Fee}=Ledger) ->
    %% TODO - this should calculate a new transaction fee for the network
    %% TODO - based on the average of usage fees
    NewFee = case ?MODULE:current_height(Ledger) /= undefined of
        true ->
            ?MODULE:current_height(Ledger) div 1000;
        false ->
            Fee
    end,
    Ledger#ledger_v1{transaction_fee=NewFee}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec consensus_members(ledger()) -> [libp2p_crypto:address()].
consensus_members(#ledger_v1{consensus_members=Members}) ->
    Members.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec consensus_members([libp2p_crypto:address()], ledger()) -> ledger().
consensus_members(Members, Ledger) ->
    Ledger#ledger_v1{consensus_members=Members}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec active_gateways(ledger()) -> active_gateways().
active_gateways(Ledger) ->
    Ledger#ledger_v1.active_gateways.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec entries(ledger()) -> entries().
entries(Ledger) ->
    Ledger#ledger_v1.entries.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec htlcs(ledger()) -> htlcs().
htlcs(Ledger) ->
    Ledger#ledger_v1.htlcs.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec find_gateway_info(libp2p_crypto:address(), ledger()) -> undefined | blockchain_ledger_gateway_v1:gateway().
find_gateway_info(GatewayAddress, Ledger) ->
    ActiveGateways = ?MODULE:active_gateways(Ledger),
    maps:get(GatewayAddress, ActiveGateways, undefined).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec add_gateway(libp2p_crypto:address(), libp2p_crypto:address(), ledger()) -> {error, gateway_already_active} | ledger().
add_gateway(OwnerAddr, GatewayAddress, Ledger) ->
    ActiveGateways = ?MODULE:active_gateways(Ledger),
    case maps:is_key(GatewayAddress, ActiveGateways) of
        true ->
            {error, gateway_already_active};
        false ->
            GwInfo = blockchain_ledger_gateway_v1:new(OwnerAddr, undefined),
            Ledger#ledger_v1{active_gateways=maps:put(GatewayAddress, GwInfo, ActiveGateways)}
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
%% NOTE: This should only be allowed when adding a gateway which was
%% added in an old blockchain and is being added via a special
%% genesis block transaction to a new chain.
-spec add_gateway(OwnerAddress :: libp2p_crypto:address(),
                  GatewayAddress :: libp2p_crypto:address(),
                  Location :: undefined | pos_integer(),
                  LastPocChallenge :: undefined | non_neg_integer(),
                  Nonce :: non_neg_integer(),
                  Score :: float(),
                  Ledger :: ledger()) -> {error, gateway_already_active} | ledger().
add_gateway(OwnerAddr,
            GatewayAddress,
            Location,
            LastPocChallenge,
            Nonce,
            Score,
            Ledger) ->
    ActiveGateways = ?MODULE:active_gateways(Ledger),
    case maps:is_key(GatewayAddress, ActiveGateways) of
        true ->
            {error, gateway_already_active};
        false ->
            GwInfo = blockchain_ledger_gateway_v1:new(OwnerAddr,
                                                   Location,
                                                   LastPocChallenge,
                                                   Nonce,
                                                   Score),
            Ledger#ledger_v1{active_gateways=maps:put(GatewayAddress, GwInfo, ActiveGateways)}
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec add_gateway_location(libp2p_crypto:address(), non_neg_integer(), non_neg_integer(), ledger()) -> {error, no_active_gateway | no_gateway_info} | ledger().
add_gateway_location(GatewayAddress, Location, Nonce, Ledger) ->
    ActiveGateways = ?MODULE:active_gateways(Ledger),
    case maps:is_key(GatewayAddress, ActiveGateways) of
        false ->
            {error, no_active_gateway};
        true ->
            case ?MODULE:find_gateway_info(GatewayAddress, Ledger) of
                undefined ->
                    %% there is no GwInfo for this gateway, assert_location sould not be allowed
                    {error, no_gateway_info};
                Gw ->
                    NewGw = case blockchain_ledger_gateway_v1:location(Gw) of
                        undefined ->
                            Gw1 = blockchain_ledger_gateway_v1:location(Location, Gw),
                            blockchain_ledger_gateway_v1:nonce(Nonce, Gw1);
                        _Loc ->
                            %%XXX: this gw already had a location asserted, do something about it here
                            Gw1 = blockchain_ledger_gateway_v1:location(Location, Gw),
                            blockchain_ledger_gateway_v1:nonce(Nonce, Gw1)
                    end,
                    Ledger#ledger_v1{active_gateways=maps:put(GatewayAddress, NewGw, ActiveGateways)}
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
request_poc(GatewayAddress, Ledger) ->
    ActiveGateways = ?MODULE:active_gateways(Ledger),
    case maps:get(GatewayAddress, ActiveGateways, undefined) of
        undefined ->
            %% there is no GwInfo for this gateway, request_poc sould not be allowed
            {error, no_gateway};
        GwInfo ->
            case blockchain_ledger_gateway_v1:location(GwInfo) of
                undefined ->
                    {error, no_gateway_location};
                _Location ->
                    case blockchain_ledger_gateway_v1:last_poc_challenge(GwInfo) > (current_height(Ledger) - 30) of
                        false ->
                            {error, too_many_challenges};
                        true ->
                            LastPocChallenge = current_height(Ledger),
                            NewGwInfo = blockchain_ledger_gateway_v1:last_poc_challenge(LastPocChallenge, GwInfo),
                            Ledger#ledger_v1{active_gateways=maps:put(GatewayAddress, NewGwInfo, ActiveGateways)}
                    end
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec find_entry(libp2p_crypto:address(), entries()) -> blockchain_ledger_entry_v1:entry().
find_entry(Address, Entries) ->
    maps:get(Address, Entries, blockchain_ledger_entry_v1:new()).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec credit_account(libp2p_crypto:address(), integer(), ledger()) -> ledger().
credit_account(Address, Amount, Ledger) ->
    Entries = ?MODULE:entries(Ledger),
    case maps:is_key(Address, Entries) of
        false ->
            Entry = blockchain_ledger_entry_v1:new(1, Amount),
            Ledger#ledger_v1{entries=maps:put(Address, Entry, Entries)};
        true ->
            Entry = ?MODULE:find_entry(Address, Entries),
            NewEntry = blockchain_ledger_entry_v1:new(
                blockchain_ledger_entry_v1:nonce(Entry),
                blockchain_ledger_entry_v1:balance(Entry) + Amount
            ),
            Ledger#ledger_v1{entries=maps:update(Address, NewEntry, Entries)}
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec debit_account(libp2p_crypto:address(), integer(), integer(), ledger()) -> ledger() | {error, any()}.
debit_account(Address, Amount, Nonce, Ledger) ->
    Entries0 = ?MODULE:entries(Ledger),
    Entry = ?MODULE:find_entry(Address, Entries0),
    case Nonce =:= blockchain_ledger_entry_v1:nonce(Entry) + 1 of
        true ->
            case (blockchain_ledger_entry_v1:balance(Entry) - Amount) >= 0 of
                true ->
                    Entries1 = maps:update(
                        Address,
                        blockchain_ledger_entry_v1:new(
                            Nonce,
                            (blockchain_ledger_entry_v1:balance(Entry) - Amount)
                        ),
                        Entries0
                    ),
                    Ledger#ledger_v1{entries=Entries1};
                false ->
                    {error, {insufficient_balance, Amount, blockchain_ledger_entry_v1:balance(Entry)}}
            end;
        false ->
            {error, {bad_nonce, Nonce, blockchain_ledger_entry_v1:nonce(Entry)}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec debit_fee(Address :: libp2p_crypto:address(), Fee :: integer(), Ledger :: ledger()) -> ledger() | {error, any()}.
debit_fee(Address, Fee, Ledger) ->
    Entries0 = ?MODULE:entries(Ledger),
    Entry = ?MODULE:find_entry(Address, Entries0),
    case (blockchain_ledger_entry_v1:balance(Entry) - Fee) >= 0 of
        true ->
            %% NOTE: There is no nonce required when debiting a fee, I think
            Entries1 = maps:update(
                Address,
                blockchain_ledger_entry_v1:new(
                    blockchain_ledger_entry_v1:nonce(Entry),
                    (blockchain_ledger_entry_v1:balance(Entry) - Fee)
                ),
                Entries0
            ),
            Ledger#ledger_v1{entries=Entries1};
        false ->
            {error, {insufficient_balance, Fee, blockchain_ledger_entry_v1:balance(Entry)}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec find_htlc(libp2p_crypto:address(), htlcs()) ->blockchain_ledger_htlc_v1:htlc().
find_htlc(Address, HTLCS) ->
    maps:get(Address, HTLCS , blockchain_ledger_htlc_v1:new()).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
add_htlc(Address, Payer, Payee, Amount, Hashlock, Timelock, Ledger) ->
    HTLCS = ?MODULE:htlcs(Ledger),
    case maps:is_key(Address, HTLCS) of
        false ->
            NewHTLC = blockchain_ledger_htlc_v1:new(Payer, Payee, Amount, Hashlock, Timelock),
            Ledger#ledger_v1{htlcs=maps:put(Address, NewHTLC, HTLCS)};
        true ->
            {error, address_already_exists}
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
redeem_htlc(Address, Payee, Ledger) ->
    HTLC = ?MODULE:find_htlc(Address, htlcs(Ledger)),
    Amount = blockchain_ledger_htlc_v1:balance(HTLC),
    Ledger1 = ?MODULE:credit_account(Payee, Amount, Ledger),
    NewHTLCS = maps:remove(Address, ?MODULE:htlcs(Ledger1)),
    Ledger1#ledger_v1{htlcs=NewHTLCS}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec serialize(ledger()) -> binary().
serialize(Ledger) ->
    erlang:term_to_binary(Ledger).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec deserialize(binary()) -> ledger().
deserialize(Bin) ->
    erlang:binary_to_term(Bin).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

find_entry_test() ->
    Entry = blockchain_ledger_entry_v1:new(1, 1),
    Ledger = #ledger_v1{entries=#{test => Entry}},
    ?assertEqual(Entry, find_entry(test, entries(Ledger))),
    ?assertEqual(blockchain_ledger_entry_v1:new(), find_entry(test2, entries(Ledger))).

find_gateway_info_test() ->
    Info = blockchain_ledger_gateway_v1:new(<<>>, undefined),
    Ledger = #ledger_v1{active_gateways=#{address => Info}},
    ?assertEqual(Info, find_gateway_info(address, Ledger)),
    ?assertEqual(undefined, find_gateway_info(test, Ledger)).

consensus_members_1_test() ->
    Ledger0 = #ledger_v1{consensus_members=[1, 2, 3]},
    Ledger1 = #ledger_v1{},
    ?assertEqual([1, 2, 3], consensus_members(Ledger0)),
    ?assertEqual([], consensus_members(Ledger1)).

consensus_members_2_test() ->
    Ledger0 = #ledger_v1{consensus_members=[]},
    Ledger1 = consensus_members([1, 2, 3], Ledger0),
    ?assertEqual([1, 2, 3], consensus_members(Ledger1)).

active_gateways_test() ->
    Ledger = #ledger_v1{active_gateways = #{address => info}},
    ?assertEqual(#{address => info}, active_gateways(Ledger)),
    ?assertEqual(#{}, active_gateways(#ledger_v1{})).

add_gateway_test() ->
    Ledger0 = #ledger_v1{active_gateways=#{}},
    Ledger1 = add_gateway(<<"owner_address">>, gw_address, Ledger0),
    ?assertEqual(
        blockchain_ledger_gateway_v1:new(<<"owner_address">>, undefined),
        find_gateway_info(gw_address, Ledger1)
    ),
    ?assertEqual({error,gateway_already_active}, add_gateway(owner_address, gw_address, Ledger1)).

add_gateway_location_test() ->
    Ledger0 = #ledger_v1{active_gateways=#{}},
    Ledger1 = add_gateway(<<"owner_address">>, gw_address, Ledger0),
    Ledger2 = add_gateway_location(gw_address, 1, 1, Ledger1),
    GW0 = blockchain_ledger_gateway_v1:new(<<"owner_address">>, 1),
    GW1 =  blockchain_ledger_gateway_v1:nonce(1, GW0),
    ?assertEqual(
       GW1,
       find_gateway_info(gw_address, Ledger2)
      ),
    ?assertEqual(
       {error,no_active_gateway},
       add_gateway_location(gw_address, 1, 1, Ledger0)
      ).

credit_account_test() ->
    Ledger0 = #ledger_v1{entries=#{address => blockchain_ledger_entry_v1:new()}},
    Ledger1 = credit_account(address, 1000, Ledger0),
    Entry = find_entry(address, entries(Ledger1)),
    ?assertEqual(1000, blockchain_ledger_entry_v1:balance(Entry)).

debit_account_test() ->
    Ledger0 = #ledger_v1{entries=#{address => blockchain_ledger_entry_v1:new()}},
    Ledger1 = credit_account(address, 1000, Ledger0),
    ?assertEqual({error, {bad_nonce, 0, 0}}, debit_account(address, 1000, 0, Ledger1)),
    ?assertEqual({error, {bad_nonce, 12, 0}}, debit_account(address, 1000, 12, Ledger1)),
    ?assertEqual({error, {insufficient_balance, 9999, 1000}}, debit_account(address, 9999, 1, Ledger1)),
    Ledger2 = debit_account(address, 500, 1, Ledger1),
    Entry = find_entry(address, entries(Ledger2)),
    ?assertEqual(500, blockchain_ledger_entry_v1:balance(Entry)),
    ?assertEqual(1, blockchain_ledger_entry_v1:nonce(Entry)).

debit_fee_test() ->
    Ledger0 = #ledger_v1{entries=#{address => blockchain_ledger_entry_v1:new()}},
    Ledger1 = credit_account(address, 1000, Ledger0),
    ?assertEqual({error, {insufficient_balance, 9999, 1000}}, debit_fee(address, 9999, Ledger1)),
    Ledger2 = debit_fee(address, 500, Ledger1),
    Entry = find_entry(address, entries(Ledger2)),
    ?assertEqual(500, blockchain_ledger_entry_v1:balance(Entry)),
    ?assertEqual(0, blockchain_ledger_entry_v1:nonce(Entry)).

serialize_deserialize_test() ->
    Ledger0 = #ledger_v1{entries=#{address => blockchain_ledger_entry_v1:new()}},
    Ledger1 = credit_account(address, 1000, Ledger0),
    ?assertEqual(Ledger1, deserialize(serialize(Ledger1))).

-endif.
