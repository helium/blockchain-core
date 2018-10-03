%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Ledger ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_ledger).

-export([
    empty_entry/0
    ,balance/1
    ,payment_nonce/1
    ,assert_location_nonce/1
    ,new_entry/2
    ,new_entry/4
    ,find_entry/2
    ,find_gateway_info/2
    ,consensus_members/1, consensus_members/2
    ,active_gateways/1
    ,add_gateway/3
    ,add_gateway_location/4
    ,gateway_location/1
    ,gateway_owner/1
    ,credit_account/3
    ,debit_account/4
    ,save/2, load/1
    ,serialize/2
    ,deserialize/2
]).

-include("blockchain.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(entry, {
    nonce = 0 :: non_neg_integer()
    ,balance = 0 :: non_neg_integer()
    ,hashlock = <<>> :: binary()
    ,timelock = 0 :: integer()
}).

-record(gw_info, {
    owner_address :: libp2p_crypto:address()
    ,location :: undefined | pos_integer()
    ,nonce = 0 :: non_neg_integer()
}).

-type ledger() :: #{
    libp2p_crypto:address() => entry()
    ,consensus_members => [libp2p_crypto:address()]
    ,active_gateways => active_gateways()
}.
-type entry() :: #entry{}.
-type gw_info() :: #gw_info{}.
-type active_gateways() :: #{libp2p_crypto:address() => gw_info()}.

-export_type([ledger/0, entry/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec empty_entry() -> entry().
empty_entry() ->
    #entry{}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec balance(entry()) -> non_neg_integer().
balance(Entry) when Entry /= undefined ->
    Entry#entry.balance.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec payment_nonce(entry()) -> non_neg_integer().
payment_nonce(Entry) when Entry /= undefined ->
    Entry#entry.nonce.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec assert_location_nonce(gw_info()) -> non_neg_integer().
assert_location_nonce(GwInfo) when GwInfo /= undefined ->
    GwInfo#gw_info.nonce.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new_entry(non_neg_integer(), non_neg_integer()) -> entry().
new_entry(Nonce, Balance) when Nonce /= undefined andalso Balance /= undefined ->
    #entry{nonce=Nonce, balance=Balance}.

-spec new_entry(non_neg_integer(), non_neg_integer(), binary(), integer()) -> entry().
new_entry(Nonce, Balance, Hashlock, Timelock) when Nonce /= undefined andalso Balance /= undefined ->
    #entry{nonce=Nonce, balance=Balance, hashlock=Hashlock, timelock=Timelock}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec find_entry(libp2p_crypto:address(), ledger()) -> entry().
find_entry(Address, Ledger) ->
    maps:get(Address, Ledger, blockchain_ledger:empty_entry()).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec find_gateway_info(libp2p_crypto:address(), ledger()) -> undefined | gw_info().
find_gateway_info(GatewayAddress, Ledger) ->
    ActiveGateways = ?MODULE:active_gateways(Ledger),
    maps:get(GatewayAddress, ActiveGateways, undefined).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec consensus_members(ledger()) -> [libp2p_crypto:address()].
consensus_members(Ledger) ->
    maps:get(consensus_members, Ledger, []).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec consensus_members([libp2p_crypto:address()], ledger()) -> ledger().
consensus_members(Members, Ledger) ->
    maps:put(consensus_members, Members, Ledger).
%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec active_gateways(ledger()) -> active_gateways().
active_gateways(Ledger) ->
    maps:get(active_gateways, Ledger, #{}).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec add_gateway(libp2p_crypto:address(), libp2p_crypto:address(), ledger()) -> false | ledger().
add_gateway(OwnerAddr, GatewayAddress, Ledger) ->
    ActiveGateways = ?MODULE:active_gateways(Ledger),
    case maps:is_key(GatewayAddress, ActiveGateways) of
        true ->
            %% GW already active
            false;
        false ->
            GwInfo = #gw_info{owner_address=OwnerAddr, location=undefined},
            maps:put(active_gateways, maps:put(GatewayAddress, GwInfo, ActiveGateways), Ledger)
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec add_gateway_location(libp2p_crypto:address(), non_neg_integer(), non_neg_integer(), ledger()) -> false | ledger().
add_gateway_location(GatewayAddress, Location, Nonce, Ledger) ->
    ActiveGateways = ?MODULE:active_gateways(Ledger),
    case maps:is_key(GatewayAddress, ActiveGateways) of
        false ->
            false;
        true ->
            case maps:get(GatewayAddress, ActiveGateways, undefined) of
                undefined ->
                    %% there is no GwInfo for this gateway, assert_location sould not be allowed
                    false;
                GwInfo ->
                    NewGwInfo =
                        case ?MODULE:gateway_location(GwInfo) of
                            undefined ->
                                GwInfo#gw_info{location=Location, nonce=Nonce};
                            _Loc ->
                                %%XXX: this gw already had a location asserted, do something about it here
                                GwInfo#gw_info{location=Location, nonce=Nonce}
                        end,
                    maps:put(active_gateways, maps:put(GatewayAddress, NewGwInfo, ActiveGateways), Ledger)
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec gateway_location(undefined | gw_info()) -> undefined | pos_integer().
gateway_location(undefined) ->
    undefined;
gateway_location(GwInfo) ->
    GwInfo#gw_info.location.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec gateway_owner(undefined | gw_info()) -> libp2p_crypto:address().
gateway_owner(undefined) ->
    undefined;
gateway_owner(GwInfo) ->
    GwInfo#gw_info.owner_address.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec credit_account(libp2p_crypto:address(), integer(), ledger()) -> ledger().
credit_account(Address, Amount, Ledger) ->
    Entry = ?MODULE:find_entry(Address, Ledger),
    NewEntry = ?MODULE:new_entry(?MODULE:payment_nonce(Entry), ?MODULE:balance(Entry) + Amount),
    maps:put(Address, NewEntry, Ledger).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec debit_account(libp2p_crypto:address(), integer(), integer(), ledger()) -> ledger() | {error, any()}.
debit_account(Address, Amount, Nonce, Ledger) ->
    Entry = ?MODULE:find_entry(Address, Ledger),
    case Nonce == ?MODULE:payment_nonce(Entry) + 1 of
        true ->
            case (?MODULE:balance(Entry) - Amount) >= 0 of
                true ->
                    maps:put(Address,
                             %% update the ledger entry nonce here with the transaction nonce
                             ?MODULE:new_entry(Nonce, ?MODULE:balance(Entry) - Amount),
                             Ledger);
                false ->
                    {error, {insufficient_balance, Amount, ?MODULE:balance(Entry)}}
            end;
        false ->
            {error, {bad_nonce, Nonce, ?MODULE:payment_nonce(Entry)}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
add_htlc(Address, Hashlock, Timelock, Ledger) ->
    Entry = ?MODULE:find_entry(Address, Ledger),
    NewEntry = ?MODULE:new_entry(?MODULE:payment_nonce(Entry), ?MODULE:balance(Entry), Hashlock, Timelock),
    maps:put(Address, NewEntry, Ledger).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec save(ledger(), string()) -> ok | {error, any()}.
save(Ledger, BaseDir) ->
    BinLedger = ?MODULE:serialize(blockchain_util:serial_version(BaseDir), Ledger),
    File = filename:join(BaseDir, ?LEDGER_FILE),
    blockchain_util:atomic_save(File, BinLedger).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec load(file:filename_all()) -> {ok, ledger()} | {error, any()}.
load(BaseDir) ->
    File = filename:join(BaseDir, ?LEDGER_FILE),
    case file:read_file(File) of
        {error, _Reason}=Error ->
            Error;
        {ok, Binary} ->
            {ok, ?MODULE:deserialize(blockchain_util:serial_version(BaseDir), Binary)}
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec serialize(blockchain_util:serial_version(), ledger()) -> binary().
serialize(_Version, Ledger) ->
    erlang:term_to_binary(Ledger).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec deserialize(blockchain_util:serial_version(), binary()) -> ledger().
deserialize(_Version, Bin) ->
    erlang:binary_to_term(Bin).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

empty_entry_test() ->
    ?assertEqual(#entry{}, empty_entry()).

balance_test() ->
    Entry = new_entry(1, 1),
    ?assertEqual(1, balance(Entry)).

payment_nonce_test() ->
    Entry = new_entry(1, 1),
    ?assertEqual(1, payment_nonce(Entry)).

assert_location_nonce_test() ->
    ?assertEqual(1, assert_location_nonce(#gw_info{nonce=1})).

new_entry_test() ->
    Entry = new_entry(2, 1),
    ?assertEqual(1, balance(Entry)),
    ?assertEqual(2, payment_nonce(Entry)).

find_entry_test() ->
    Entry = new_entry(1, 1),
    Ledger = #{test => Entry},
    ?assertEqual(Entry, find_entry(test, Ledger)),
    ?assertEqual(empty_entry(), find_entry(test2, Ledger)).

find_gateway_info_test() ->
    Info = #gw_info{},
    Ledger = #{active_gateways => #{address => Info}},
    ?assertEqual(Info, find_gateway_info(address, Ledger)),
    ?assertEqual(undefined, find_gateway_info(test, Ledger)).

consensus_members_1_test() ->
    Ledger0 = #{consensus_members => [1, 2, 3]},
    Ledger1 = #{},
    ?assertEqual([1, 2, 3], consensus_members(Ledger0)),
    ?assertEqual([], consensus_members(Ledger1)).

consensus_members_2_test() ->
    Ledger0 = #{consensus_members => []},
    Ledger1 = consensus_members([1, 2, 3], Ledger0),
    ?assertEqual([1, 2, 3], consensus_members(Ledger1)).

active_gateways_test() ->
    Ledger = #{active_gateways => #{address => info}},
    ?assertEqual(#{address => info}, active_gateways(Ledger)),
    ?assertEqual(#{}, active_gateways(#{})).

add_gateway_test() ->
    Ledger0 = #{active_gateways => #{}},
    Ledger1 = add_gateway(owner_address, gw_address, Ledger0),
    ?assertEqual(
        #gw_info{owner_address=owner_address, location=undefined}
        ,find_gateway_info(gw_address, Ledger1)
    ),
    ?assertEqual(false, add_gateway(owner_address, gw_address, Ledger1)).

add_gateway_location_test() ->
    Ledger0 = #{active_gateways => #{}},
    Ledger1 = add_gateway(owner_address, gw_address, Ledger0),
    Ledger2 = add_gateway_location(gw_address, 1, 1, Ledger1),
    ?assertEqual(
        #gw_info{owner_address=owner_address, location=1, nonce=1}
        ,find_gateway_info(gw_address, Ledger2)
    ),
    ?assertEqual(
        false
        ,add_gateway_location(gw_address, 1, 1, Ledger0)
    ).

gateway_location_test() ->
    ?assertEqual(1, gateway_location(#gw_info{location=1})),
    ?assertEqual(undefined, gateway_location(undefined)).

gateway_owner_test() ->
    ?assertEqual(addr, gateway_owner(#gw_info{owner_address=addr})),
    ?assertEqual(undefined, gateway_owner(undefined)).

credit_account_test() ->
    Ledger0 = #{},
    Ledger1 = credit_account(address, 1000, Ledger0),
    Entry = find_entry(address, Ledger1),
    ?assertEqual(1000, balance(Entry)).

debit_account_test() ->
    Ledger0 = #{},
    Ledger1 = credit_account(address, 1000, Ledger0),
    ?assertEqual({error, {bad_nonce, 0, 0}}, debit_account(address, 1000, 0, Ledger1)),
    ?assertEqual({error, {bad_nonce, 12, 0}}, debit_account(address, 1000, 12, Ledger1)),
    ?assertEqual({error, {insufficient_balance, 9999, 1000}}, debit_account(address, 9999, 1, Ledger1)),
    Ledger2 = debit_account(address, 500, 1, Ledger1),
    Entry = find_entry(address, Ledger2),
    ?assertEqual(500, balance(Entry)),
    ?assertEqual(1, payment_nonce(Entry)).

save_load_test() ->
    BaseDir = "data/test",
    Ledger0 = #{},
    Ledger1 = credit_account(address, 1000, Ledger0),
    ?assertEqual(ok, save(Ledger1, BaseDir)),
    ?assertEqual({ok, Ledger1}, load(BaseDir)),
    ?assertEqual({error, enoent}, load("data/test2")),
    ok.

serialize_deserialize_test() ->
    Ledger0 = #{},
    Ledger1 = credit_account(address, 1000, Ledger0),
    ?assertEqual(Ledger1, deserialize(v1, serialize(v1, Ledger1))).

-endif.
