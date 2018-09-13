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
    ,find_entry/2
    ,find_gateway_info/2
    ,consensus_members/1
    ,add_consensus_members/2
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

-record(entry, {
    nonce = 0 :: non_neg_integer()
    ,balance = 0 :: non_neg_integer()
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
-spec add_consensus_members([libp2p_crypto:address()], ledger()) -> ledger().
add_consensus_members(Members, Ledger) ->
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
-spec save(ledger(), string()) -> ok | {error, any()}.
save(Ledger, BaseDir) ->
    BinLedger = serialize(blockchain_util:serial_version(BaseDir), Ledger),
    File = filename:join(BaseDir, ?LEDGER_FILE),
    blockchain_util:atomic_save(File, BinLedger).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec load(string()) -> ledger() | undefined.
load(BaseDir) ->
    File = filename:join(BaseDir, ?LEDGER_FILE),
    case file:read_file(File) of
        {error, _Reason} ->
            undefined;
        {ok, Binary} ->
            deserialize(blockchain_util:serial_version(BaseDir), Binary)
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
