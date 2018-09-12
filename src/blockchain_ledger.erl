%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Ledger ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_ledger).

-export([
    empty_entry/0
    ,balance/1
    ,nonce/1
    ,new_entry/2
    ,find_entry/2
    ,consensus_members/1
    ,add_consensus_members/2
    ,active_gateways/1
    ,add_gateway/2
    ,add_gateway_location/3
    ,credit_account/3
    ,debit_account/4
    ,save/2, load/1
    ,serialize/1
    ,deserialize/1
]).

-record(entry, {
    nonce = 0 :: non_neg_integer()
    ,balance = 0 :: non_neg_integer()
}).

-record(gw_info, {
    location :: undefined | non_neg_integer()
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
-spec nonce(entry()) -> non_neg_integer().
nonce(Entry) when Entry /= undefined ->
    Entry#entry.nonce.

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
-spec add_gateway(libp2p_crypto:address(), ledger()) -> ledger().
add_gateway(Address, Ledger) ->
    ActiveGateways = active_gateways(Ledger),
    case maps:is_key(Address, ActiveGateways) of
        true ->
            %% GW already active
            Ledger;
        false ->
            maps:put(active_gateways, maps:put(Address, undefined, ActiveGateways), Ledger)
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec add_gateway_location(libp2p_crypto:address(), non_neg_integer(), ledger()) -> false | ledger().
add_gateway_location(Address, Location, Ledger) ->
    ActiveGateways = ?MODULE:active_gateways(Ledger),
    case maps:is_key(Address, ActiveGateways) of
        false ->
            false;
        true ->
            maps:put(active_gateways, maps:update(Address, Location, ActiveGateways), Ledger)
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec credit_account(libp2p_crypto:address(), integer(), ledger()) -> ledger().
credit_account(Address, Amount, Ledger) ->
    Entry = ?MODULE:find_entry(Address, Ledger),
    NewEntry = ?MODULE:new_entry(?MODULE:nonce(Entry), ?MODULE:balance(Entry) + Amount),
    maps:put(Address, NewEntry, Ledger).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec debit_account(libp2p_crypto:address(), integer(), integer(), ledger()) -> ledger() | {error, any()}.
debit_account(Address, Amount, Nonce, Ledger) ->
    Entry = ?MODULE:find_entry(Address, Ledger),
    case Nonce == ?MODULE:nonce(Entry) + 1 of
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
            {error, {bad_nonce, Nonce, ?MODULE:nonce(Entry)}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec save(ledger(), string()) -> ok | {error, any()}.
save(Ledger, BaseDir) ->
    BinLedger = ?MODULE:serialize(Ledger),
    File = filename:join(BaseDir, "ledger"),
    blockchain_util:atomic_save(File, BinLedger).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec load(string()) -> ledger() | undefined.
load(BaseDir) ->
    File = filename:join(BaseDir, "ledger"),
    case file:read_file(File) of
        {error, _Reason} ->
            undefined;
        {ok, Binary} ->
            ?MODULE:deserialize(Binary)
    end.

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
