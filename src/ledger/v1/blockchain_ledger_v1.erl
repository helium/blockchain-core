%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Ledger ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_ledger_v1).

-export([
    new/1,
    mode/1, mode/2,
    dir/1,

    new_context/1, delete_context/1, commit_context/1, context_cache/1,

    current_height/1, increment_height/2,
    transaction_fee/1, update_transaction_fee/1,
    consensus_members/1, consensus_members/2,
    active_gateways/1,
    entries/1,
    htlcs/1,

    find_gateway_info/2,
    add_gateway/3, add_gateway/6,
    add_gateway_location/4,

    find_poc/2,
    request_poc/5,
    delete_poc/3, delete_pocs/2,

    find_entry/2,
    credit_account/3,
    debit_account/4,
    debit_fee/3,
    check_balance/3,

    find_htlc/2,
    add_htlc/7,
    redeem_htlc/3,

    get_oui_counter/1, increment_oui_counter/1,
    find_ouis/2, add_oui/3,
    find_routing/2,  add_routing/5,

    clean/1, close/1
]).

-include("blockchain.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(ledger_v1, {
    dir :: file:filename_all(),
    db :: rocksdb:db_handle(),
    mode = active :: active | delayed,
    active :: sub_ledger(),
    delayed :: sub_ledger()
}).

-record(sub_ledger_v1, {
    default :: rocksdb:cf_handle(),
    active_gateways :: rocksdb:cf_handle(),
    entries :: rocksdb:cf_handle(),
    htlcs :: rocksdb:cf_handle(),
    pocs :: rocksdb:cf_handle(),
    routing :: rocksdb:cf_handle(),
    context :: undefined | rocksdb:batch_handle(),
    cache :: undefined | ets:tid()
}).

-define(DB_FILE, "ledger.db").
-define(CURRENT_HEIGHT, <<"current_height">>).
-define(TRANSACTION_FEE, <<"transaction_fee">>).
-define(CONSENSUS_MEMBERS, <<"consensus_members">>).
-define(OUI_COUNTER, <<"oui_counter">>).

-type ledger() :: #ledger_v1{}.
-type sub_ledger() :: #sub_ledger_v1{}.
-type entries() :: #{libp2p_crypto:pubkey_bin() => blockchain_ledger_entry_v1:entry()}.
-type active_gateways() :: #{libp2p_crypto:pubkey_bin() => blockchain_ledger_gateway_v1:gateway()}.
-type htlcs() :: #{libp2p_crypto:pubkey_bin() => blockchain_ledger_htlc_v1:htlc()}.

-export_type([ledger/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(file:filename_all()) -> ledger().
new(Dir) ->
    {ok, DB, CFs} = open_db(Dir),
    [DefaultCF, AGwsCF, EntriesCF, HTLCsCF, PoCsCF, RoutingCf,
     DelayedDefaultCF, DelayedAGwsCF, DelayedEntriesCF, DelayedHTLCsCF, DelayedPoCsCF, DelayedRoutingCf] = CFs,
    #ledger_v1{
        dir=Dir,
        db=DB,
        mode=active,
        active= #sub_ledger_v1{
            default=DefaultCF,
            active_gateways=AGwsCF,
            entries=EntriesCF,
            htlcs=HTLCsCF,
            pocs=PoCsCF,
            routing=RoutingCf
        },
        delayed= #sub_ledger_v1{
            default=DelayedDefaultCF,
            active_gateways=DelayedAGwsCF,
            entries=DelayedEntriesCF,
            htlcs=DelayedHTLCsCF,
            pocs=DelayedPoCsCF,
            routing=DelayedRoutingCf
        }
    }.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec mode(ledger()) -> active | delayed.
mode(Ledger) ->
    Ledger#ledger_v1.mode.

-spec mode(active | delayed, ledger()) -> ledger().
mode(Mode, Ledger) ->
    Ledger#ledger_v1{mode=Mode}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec dir(ledger()) -> file:filename_all().
dir(Ledger) ->
    Ledger#ledger_v1.dir.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new_context(ledger()) -> ledger().
new_context(Ledger) ->
    %% accumulate DB operations in a rocksdb batch
    {ok, Context} = rocksdb:batch(),
    %% accumulate ledger changes in a read-through ETS cache
    Cache = ets:new(txn_cache, [set, private, {keypos, 1}]),
    context_cache({Context, Cache}, Ledger).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec delete_context(ledger()) -> ledger().
delete_context(Ledger) ->
    case context_cache(Ledger) of
        {undefined, undefined} ->
            Ledger;
        {undefined, Cache} ->
            ets:delete(Cache),
            context_cache({undefined, undefined}, Ledger);
        {Context, undefined} ->
            rocksdb:batch_clear(Context),
            context_cache({undefined, undefined}, Ledger);
        {Context, Cache} ->
            rocksdb:batch_clear(Context),
            ets:delete(Cache),
            context_cache({undefined, undefined}, Ledger)
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec commit_context(ledger()) -> ok.
commit_context(#ledger_v1{db=DB}=Ledger) ->
    {Context, _Cache} = context_cache(Ledger),
    ok = rocksdb:write_batch(DB, Context, [{sync, true}]),
    delete_context(Ledger),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec current_height(ledger()) -> {ok, non_neg_integer()} | {error, any()}.
current_height(Ledger) ->
    DefaultCF = default_cf(Ledger),
    case cache_get(Ledger, DefaultCF, ?CURRENT_HEIGHT, []) of
        {ok, <<Height:64/integer-unsigned-big>>} ->
            {ok, Height};
        not_found ->
            {ok, 0};
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec increment_height(blockchain_block:block(), ledger()) -> ok | {error, any()}.
increment_height(Block, Ledger) ->
    DefaultCF = default_cf(Ledger),
    BlockHeight = blockchain_block:height(Block),
    case current_height(Ledger) of
        {error, _} ->
            cache_put(Ledger, DefaultCF, ?CURRENT_HEIGHT, <<1:64/integer-unsigned-big>>);
        {ok, Height0} ->
            Height1 = erlang:max(BlockHeight, Height0),
            cache_put(Ledger, DefaultCF, ?CURRENT_HEIGHT, <<Height1:64/integer-unsigned-big>>)
    end.


%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec transaction_fee(ledger()) -> {ok, pos_integer()} | {error, any()}.
transaction_fee(Ledger) ->
    DefaultCF = default_cf(Ledger),
    case cache_get(Ledger, DefaultCF, ?TRANSACTION_FEE, []) of
        {ok, <<Height:64/integer-unsigned-big>>} ->
            {ok, Height};
        not_found ->
            {error, not_found};
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec update_transaction_fee(ledger()) -> ok.
update_transaction_fee(Ledger) ->
    %% TODO - this should calculate a new transaction fee for the network
    %% TODO - based on the average of usage fees
    DefaultCF = default_cf(Ledger),
    case ?MODULE:current_height(Ledger) of
        {error, _} ->
            ok;
        {ok, Fee0} ->
            Fee1 = Fee0 div 1000,
            cache_put(Ledger, DefaultCF, ?TRANSACTION_FEE, <<Fee1:64/integer-unsigned-big>>)
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec consensus_members(ledger()) -> {ok, [libp2p_crypto:pubkey_bin()]} | {error, any()}.
consensus_members(Ledger) ->
    DefaultCF = default_cf(Ledger),
    case cache_get(Ledger, DefaultCF, ?CONSENSUS_MEMBERS, []) of
        {ok, Bin} ->
            {ok, erlang:binary_to_term(Bin)};
        not_found ->
            {error, not_found};
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec consensus_members([libp2p_crypto:pubkey_bin()], ledger()) ->  ok | {error, any()}.
consensus_members(Members, Ledger) ->
    Bin = erlang:term_to_binary(Members),
    DefaultCF = default_cf(Ledger),
    cache_put(Ledger, DefaultCF, ?CONSENSUS_MEMBERS, Bin).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec active_gateways(ledger()) -> active_gateways().
active_gateways(#ledger_v1{db=DB}=Ledger) ->
    AGwsCF = active_gateways_cf(Ledger),
    rocksdb:fold(
        DB,
        AGwsCF,
        fun({Address, Binary}, Acc) ->
            Gw = blockchain_ledger_gateway_v1:deserialize(Binary),
            maps:put(Address, Gw, Acc)
        end,
        #{},
        []
    ).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec entries(ledger()) -> entries().
entries(#ledger_v1{db=DB}=Ledger) ->
    EntriesCF = entries_cf(Ledger),
    rocksdb:fold(
        DB,
        EntriesCF,
        fun({Address, Binary}, Acc) ->
            Entry = blockchain_ledger_entry_v1:deserialize(Binary),
            maps:put(Address, Entry, Acc)
        end,
        #{},
        []
    ).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec htlcs(ledger()) -> htlcs().
htlcs(#ledger_v1{db=DB}=Ledger) ->
    HTLCsCF = htlcs_cf(Ledger),
    rocksdb:fold(
        DB,
        HTLCsCF,
        fun({Address, Binary}, Acc) ->
            Entry = blockchain_ledger_htlc_v1:deserialize(Binary),
            maps:put(Address, Entry, Acc)
        end,
        #{},
        []
    ).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec find_gateway_info(libp2p_crypto:pubkey_bin(), ledger()) -> {ok, blockchain_ledger_gateway_v1:gateway()}
                                                              | {error, any()}.
find_gateway_info(Address, Ledger) ->
    AGwsCF = active_gateways_cf(Ledger),
    case cache_get(Ledger, AGwsCF, Address, []) of
        {ok, BinGw} ->
            {ok, blockchain_ledger_gateway_v1:deserialize(BinGw)};
        not_found ->
            {error, not_found};
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec add_gateway(libp2p_crypto:pubkey_bin(), libp2p_crypto:pubkey_bin(), ledger()) -> ok | {error, gateway_already_active}.
add_gateway(OwnerAddr, GatewayAddress, Ledger) ->
    AGwsCF = active_gateways_cf(Ledger),
    case ?MODULE:find_gateway_info(GatewayAddress, Ledger) of
        {ok, _} ->
            {error, gateway_already_active};
        _ ->
            Gateway = blockchain_ledger_gateway_v1:new(OwnerAddr, undefined),
            Bin = blockchain_ledger_gateway_v1:serialize(Gateway),
            cache_put(Ledger, AGwsCF, GatewayAddress, Bin)
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
%% NOTE: This should only be allowed when adding a gateway which was
%% added in an old blockchain and is being added via a special
%% genesis block transaction to a new chain.
-spec add_gateway(OwnerAddress :: libp2p_crypto:pubkey_bin(),
                  GatewayAddress :: libp2p_crypto:pubkey_bin(),
                  Location :: undefined | pos_integer(),
                  Nonce :: non_neg_integer(),
                  Score :: float(),
                  Ledger :: ledger()) -> ok | {error, gateway_already_active}.
add_gateway(OwnerAddr,
            GatewayAddress,
            Location,
            Nonce,
            Score,
            Ledger) ->
    case ?MODULE:find_gateway_info(GatewayAddress, Ledger) of
        {ok, _} ->
            {error, gateway_already_active};
        _ ->
            Gateway = blockchain_ledger_gateway_v1:new(OwnerAddr,
                                                       Location,
                                                       Nonce,
                                                       Score),
            Bin = blockchain_ledger_gateway_v1:serialize(Gateway),
            AGwsCF = active_gateways_cf(Ledger),
            cache_put(Ledger, AGwsCF, GatewayAddress, Bin)
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec add_gateway_location(libp2p_crypto:pubkey_bin(), non_neg_integer(), non_neg_integer(), ledger()) -> ok | {error, no_active_gateway}.
add_gateway_location(GatewayAddress, Location, Nonce, Ledger) ->
    case ?MODULE:find_gateway_info(GatewayAddress, Ledger) of
        {error, _} ->
            {error, no_active_gateway};
        {ok, Gw} ->
            NewGw = case blockchain_ledger_gateway_v1:location(Gw) of
                undefined ->
                    Gw1 = blockchain_ledger_gateway_v1:location(Location, Gw),
                    blockchain_ledger_gateway_v1:nonce(Nonce, Gw1);
                _Loc ->
                    %%XXX: this gw already had a location asserted, do something about it here
                    Gw1 = blockchain_ledger_gateway_v1:location(Location, Gw),
                    blockchain_ledger_gateway_v1:nonce(Nonce, Gw1)
            end,
            Bin = blockchain_ledger_gateway_v1:serialize(NewGw),
            AGwsCF = active_gateways_cf(Ledger),
            cache_put(Ledger, AGwsCF, GatewayAddress, Bin)
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec find_poc(binary(), ledger()) -> {ok, blockchain_ledger_poc_v1:pocs()} | {error, any()}.
find_poc(OnionKeyHash, Ledger) ->
    PoCsCF = pocs_cf(Ledger),
    case cache_get(Ledger, PoCsCF, OnionKeyHash, []) of
        {ok, BinPoCs} ->
            PoCs = erlang:binary_to_term(BinPoCs),
            {ok, lists:map(fun blockchain_ledger_poc_v1:deserialize/1, PoCs)};
        not_found ->
            {error, not_found};
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec request_poc(OnionKeyHash :: binary(),
                  SecretHash :: binary(),
                  Challenger :: libp2p_crypto:pubkey_bin(),
                  Block :: blockchain_block:block(),
                  Ledger :: ledger()) -> ok | {error, any()}.
request_poc(OnionKeyHash, SecretHash, Challenger, Block, Ledger) ->
    case ?MODULE:find_gateway_info(Challenger, Ledger) of
        {error, _} ->
            {error, no_active_gateway};
        {ok, Gw0} ->
            case ?MODULE:find_poc(OnionKeyHash, Ledger) of
                {error, not_found} ->
                    request_poc_(OnionKeyHash, SecretHash, Challenger, Block, Ledger, Gw0, []);
                {error, _} ->
                    {error, fail_getting_poc};
                {ok, PoCs} ->
                    request_poc_(OnionKeyHash, SecretHash, Challenger, Block, Ledger, Gw0, PoCs)
            end
    end.

request_poc_(OnionKeyHash, SecretHash, Challenger, Block, Ledger, Gw0, PoCs) ->
    case blockchain_ledger_gateway_v1:last_poc_onion_key_hash(Gw0) of
        undefined ->
            Height = blockchain_block:height(Block),
            Gw1 = blockchain_ledger_gateway_v1:last_poc_challenge(Height, Gw0),
            Gw2 = blockchain_ledger_gateway_v1:last_poc_onion_key_hash(OnionKeyHash, Gw1),
            GwBin = blockchain_ledger_gateway_v1:serialize(Gw2),
            AGwsCF = active_gateways_cf(Ledger),
            ok = cache_put(Ledger, AGwsCF, Challenger, GwBin),

            PoC = blockchain_ledger_poc_v1:new(SecretHash, OnionKeyHash, Challenger),
            PoCBin = blockchain_ledger_poc_v1:serialize(PoC),
            BinPoCs = erlang:term_to_binary([PoCBin|lists:map(fun blockchain_ledger_poc_v1:serialize/1, PoCs)]),
            PoCsCF = pocs_cf(Ledger),
            cache_put(Ledger, PoCsCF, OnionKeyHash, BinPoCs);
        LastOnionKeyHash  ->  
            case delete_poc(LastOnionKeyHash, Challenger, Ledger) of
                {error, _}=Error ->
                    Error;
                ok ->
                    Height = blockchain_block:height(Block),
                    Gw1 = blockchain_ledger_gateway_v1:last_poc_challenge(Height, Gw0),
                    Gw2 = blockchain_ledger_gateway_v1:last_poc_onion_key_hash(OnionKeyHash, Gw1),
                    GwBin = blockchain_ledger_gateway_v1:serialize(Gw2),
                    AGwsCF = active_gateways_cf(Ledger),
                    ok = cache_put(Ledger, AGwsCF, Challenger, GwBin),

                    PoC = blockchain_ledger_poc_v1:new(SecretHash, OnionKeyHash, Challenger),
                    PoCBin = blockchain_ledger_poc_v1:serialize(PoC),
                    BinPoCs = erlang:term_to_binary([PoCBin|lists:map(fun blockchain_ledger_poc_v1:serialize/1, PoCs)]),
                    PoCsCF = pocs_cf(Ledger),
                    cache_put(Ledger, PoCsCF, OnionKeyHash, BinPoCs)
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec delete_poc(binary(), libp2p_crypto:pubkey_bin(), ledger()) -> ok | {error, any()}.
delete_poc(OnionKeyHash, Challenger, Ledger) ->
    case ?MODULE:find_poc(OnionKeyHash, Ledger) of
        {error, not_found} ->
            ok;
        {error, _}=Error ->
            Error;
        {ok, PoCs} ->
            FilteredPoCs = lists:filter(
                fun(PoC) ->
                    blockchain_ledger_poc_v1:challenger(PoC) =/= Challenger
                end,
                PoCs
            ),
            case FilteredPoCs of
                [] ->
                    ?MODULE:delete_pocs(OnionKeyHash, Ledger);
                _ ->
                    BinPoCs = erlang:term_to_binary(lists:map(fun blockchain_ledger_poc_v1:serialize/1, FilteredPoCs)),
                    PoCsCF = pocs_cf(Ledger),
                    cache_put(Ledger, PoCsCF, OnionKeyHash, BinPoCs)
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec delete_pocs(binary(), ledger()) -> ok | {error, any()}.
delete_pocs(OnionKeyHash, Ledger) ->
    PoCsCF = pocs_cf(Ledger),
    cache_delete(Ledger, PoCsCF, OnionKeyHash).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec find_entry(libp2p_crypto:pubkey_bin(), ledger()) -> {ok, blockchain_ledger_entry_v1:entry()}
                                                          | {error, any()}.
find_entry(Address, Ledger) ->
    EntriesCF = entries_cf(Ledger),
    case cache_get(Ledger, EntriesCF, Address, []) of
        {ok, BinEntry} ->
            {ok, blockchain_ledger_entry_v1:deserialize(BinEntry)};
        not_found ->
            {error, not_found};
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec credit_account(libp2p_crypto:pubkey_bin(), integer(), ledger()) -> ok | {error, any()}.
credit_account(Address, Amount, Ledger) ->
    EntriesCF = entries_cf(Ledger),
    case ?MODULE:find_entry(Address, Ledger) of
        {error, not_found} ->
            Entry = blockchain_ledger_entry_v1:new(0, Amount),
            Bin = blockchain_ledger_entry_v1:serialize(Entry),
            cache_put(Ledger, EntriesCF, Address, Bin);
        {ok, Entry} ->
            Entry1 = blockchain_ledger_entry_v1:new(
                blockchain_ledger_entry_v1:nonce(Entry),
                blockchain_ledger_entry_v1:balance(Entry) + Amount
            ),
            Bin = blockchain_ledger_entry_v1:serialize(Entry1),
            cache_put(Ledger, EntriesCF, Address, Bin);
        {error, _}=Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec debit_account(libp2p_crypto:pubkey_bin(), integer(), integer(), ledger()) -> ok | {error, any()}.
debit_account(Address, Amount, Nonce, Ledger) ->
    case ?MODULE:find_entry(Address, Ledger) of
        {error, _}=Error ->
            Error;
        {ok, Entry} ->
            case Nonce =:= blockchain_ledger_entry_v1:nonce(Entry) + 1 of
                true ->
                    Balance = blockchain_ledger_entry_v1:balance(Entry),
                    case (Balance - Amount) >= 0 of
                        true ->
                            Entry1 = blockchain_ledger_entry_v1:new(
                                Nonce,
                                (Balance - Amount)
                            ),
                            Bin = blockchain_ledger_entry_v1:serialize(Entry1),
                            EntriesCF = entries_cf(Ledger),
                            cache_put(Ledger, EntriesCF, Address, Bin);
                        false ->
                            {error, {insufficient_balance, Amount, Balance}}
                    end;
                false ->
                    {error, {bad_nonce, {payment, Nonce, blockchain_ledger_entry_v1:nonce(Entry)}}}
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec debit_fee(Address :: libp2p_crypto:pubkey_bin(), Fee :: non_neg_integer(), Ledger :: ledger()) -> ok | {error, any()}.
debit_fee(Address, Fee, Ledger) ->
    case ?MODULE:find_entry(Address, Ledger) of
        {error, _}=Error ->
            Error;
        {ok, Entry} ->
            Balance = blockchain_ledger_entry_v1:balance(Entry),
            case (Balance - Fee) >= 0 of
                true ->
                    Entry1 = blockchain_ledger_entry_v1:new(
                        blockchain_ledger_entry_v1:nonce(Entry),
                        (Balance - Fee)
                    ),
                    Bin = blockchain_ledger_entry_v1:serialize(Entry1),
                    EntriesCF = entries_cf(Ledger),
                    cache_put(Ledger, EntriesCF, Address, Bin);
                false ->
                    {error, {insufficient_balance, Fee, Balance}}
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec check_balance(Address :: libp2p_crypto:pubkey_bin(), Amount :: non_neg_integer(), Ledger :: ledger()) -> ok | {error, any()}.
check_balance(Address, Amount, Ledger) ->
    case ?MODULE:find_entry(Address, Ledger) of
        {error, _}=Error ->
            Error;
        {ok, Entry} ->
            Balance = blockchain_ledger_entry_v1:balance(Entry),
            case (Balance - Amount) >= 0 of
                false ->
                    {error, {insufficient_balance, Amount, Balance}};
                true ->
                    ok
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec find_ouis(binary(), ledger()) -> {ok, [non_neg_integer()]} | {error, any()}.
find_ouis(Owner, Ledger) ->
    RoutingCF = routing_cf(Ledger),
    case cache_get(Ledger, RoutingCF, Owner, []) of
        {ok, Bin} -> {ok, erlang:binary_to_term(Bin)};
        not_found -> {ok, []};
        Error -> Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec find_htlc(libp2p_crypto:pubkey_bin(), ledger()) -> {ok, blockchain_ledger_htlc_v1:htlc()}
                                                         | {error, any()}.
find_htlc(Address, Ledger) ->
    HTLCsCF = htlcs_cf(Ledger),
    case cache_get(Ledger, HTLCsCF, Address, []) of
        {ok, BinEntry} ->
            {ok, blockchain_ledger_htlc_v1:deserialize(BinEntry)};
        not_found ->
            {error, not_found};
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec add_htlc(libp2p_crypto:pubkey_bin(), libp2p_crypto:pubkey_bin(), libp2p_crypto:pubkey_bin(),
               non_neg_integer(),  binary(), non_neg_integer(), ledger()) -> ok | {error, any()}.
add_htlc(Address, Payer, Payee, Amount, Hashlock, Timelock, Ledger) ->
    HTLCsCF = htlcs_cf(Ledger),
    case ?MODULE:find_htlc(Address, Ledger) of
        {ok, _} ->
            {error, address_already_exists};
        {error, _} ->
            HTLC = blockchain_ledger_htlc_v1:new(Payer, Payee, Amount, Hashlock, Timelock),
            Bin = blockchain_ledger_htlc_v1:serialize(HTLC),
            cache_put(Ledger, HTLCsCF, Address, Bin)
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec redeem_htlc(libp2p_crypto:pubkey_bin(), libp2p_crypto:pubkey_bin(), ledger()) -> ok | {error, any()}.
redeem_htlc(Address, Payee, Ledger) ->
    case ?MODULE:find_htlc(Address, Ledger) of
        {error, _}=Error ->
            Error;
        {ok, HTLC} ->
            Amount = blockchain_ledger_htlc_v1:balance(HTLC),
            case ?MODULE:credit_account(Payee, Amount, Ledger) of
                {error, _}=Error -> Error;
                ok ->
                    HTLCsCF = htlcs_cf(Ledger),
                    cache_delete(Ledger, HTLCsCF, Address)
            end
    end.


%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec get_oui_counter(ledger()) -> {ok, non_neg_integer()} | {error, any()}.
get_oui_counter(Ledger) ->
    DefaultCF = default_cf(Ledger),
    case cache_get(Ledger, DefaultCF, ?OUI_COUNTER, []) of
        {ok, <<OUI:32/little-unsigned-integer>>} ->
            {ok, OUI};
        not_found ->
            {ok, 0};
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec increment_oui_counter(ledger()) -> {ok, non_neg_integer()} | {error, any()}.
increment_oui_counter(Ledger) ->
    case ?MODULE:get_oui_counter(Ledger) of
        {error, _}=Error ->
            Error;
        {ok, OUICounter} ->
            DefaultCF = default_cf(Ledger),
            OUI = OUICounter + 1,
            case cache_put(Ledger, DefaultCF, ?OUI_COUNTER, <<OUI:32/little-unsigned-integer>>) of
                {error, _}=Error -> Error;
                ok -> {ok, OUI}
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec add_oui(binary(), [binary()], ledger()) -> ok | {error, any()}.
add_oui(Owner, Addresses, Ledger) ->
    case ?MODULE:increment_oui_counter(Ledger) of
        {error, _}=Error ->
            Error;
        {ok, OUI} ->
            RoutingCF = routing_cf(Ledger),
            Routing = blockchain_ledger_routing_v1:new(OUI, Owner, Addresses, 0),
            Bin = blockchain_ledger_routing_v1:serialize(Routing),
            case ?MODULE:find_ouis(Owner, Ledger) of
                {error, _}=Error ->
                    Error;
                {ok, OUIs} ->
                    cache_put(Ledger, RoutingCF, <<OUI:32/little-unsigned-integer>>, Bin),
                    cache_put(Ledger, RoutingCF, Owner, erlang:term_to_binary([OUI|OUIs]))
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec find_routing(non_neg_integer(), ledger()) -> {ok, blockchain_ledger_routing_v1:routing()}
                                                   | {error, any()}.
find_routing(OUI, Ledger) ->
    RoutingCF = routing_cf(Ledger),
    case cache_get(Ledger, RoutingCF, <<OUI:32/little-unsigned-integer>>, []) of
        {ok, BinEntry} ->
            {ok, blockchain_ledger_routing_v1:deserialize(BinEntry)};
        not_found ->
            {error, not_found};
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec add_routing(binary(), non_neg_integer(), [binary()], non_neg_integer(), ledger()) -> ok | {error, any()}.
add_routing(Owner, OUI, Addresses, Nonce, Ledger) ->
    RoutingCF = routing_cf(Ledger),
    Routing = blockchain_ledger_routing_v1:new(OUI, Owner, Addresses, Nonce),
    Bin = blockchain_ledger_routing_v1:serialize(Routing),
    cache_put(Ledger, RoutingCF, <<OUI:32/little-unsigned-integer>>, Bin).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
clean(#ledger_v1{dir=Dir, db=DB}) ->
    DBDir = filename:join(Dir, ?DB_FILE),
    ok = rocksdb:close(DB),
    rocksdb:destroy(DBDir, []).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
close(#ledger_v1{db=DB}) ->
    rocksdb:close(DB).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec context_cache(ledger()) -> {undefined | rocksdb:batch_handle(), undefined | ets:tid()}.
context_cache(#ledger_v1{mode=active, active=#sub_ledger_v1{context=Context, cache=Cache}}) ->
    {Context, Cache};
context_cache(#ledger_v1{mode=delayed, delayed=#sub_ledger_v1{context=Context, cache=Cache}}) ->
    {Context, Cache}.

-spec context_cache({undefined | rocksdb:batch_handle(), undefined | ets:tid()}, ledger()) -> ledger().
context_cache({Context, Cache}, #ledger_v1{mode=active, active=Active}=Ledger) ->
    Ledger#ledger_v1{active=Active#sub_ledger_v1{context=Context, cache=Cache}};
context_cache({Context, Cache}, #ledger_v1{mode=delayed, delayed=Delayed}=Ledger) ->
    Ledger#ledger_v1{delayed=Delayed#sub_ledger_v1{context=Context, cache=Cache}}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec default_cf(ledger()) -> rocksdb:cf_handle().
default_cf(#ledger_v1{mode=active, active=#sub_ledger_v1{default=DefaultCF}}) ->
    DefaultCF;
default_cf(#ledger_v1{mode=delayed, delayed=#sub_ledger_v1{default=DefaultCF}}) ->
    DefaultCF.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec active_gateways_cf(ledger()) -> rocksdb:cf_handle().
active_gateways_cf(#ledger_v1{mode=active, active=#sub_ledger_v1{active_gateways=AGCF}}) ->
    AGCF;
active_gateways_cf(#ledger_v1{mode=delayed, delayed=#sub_ledger_v1{active_gateways=AGCF}}) ->
    AGCF.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec entries_cf(ledger()) -> rocksdb:cf_handle().
entries_cf(#ledger_v1{mode=active, active=#sub_ledger_v1{entries=EntriesCF}}) ->
    EntriesCF;
entries_cf(#ledger_v1{mode=delayed, delayed=#sub_ledger_v1{entries=EntriesCF}}) ->
    EntriesCF.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec htlcs_cf(ledger()) -> rocksdb:cf_handle().
htlcs_cf(#ledger_v1{mode=active, active=#sub_ledger_v1{htlcs=HTLCsCF}}) ->
    HTLCsCF;
htlcs_cf(#ledger_v1{mode=delayed, delayed=#sub_ledger_v1{htlcs=HTLCsCF}}) ->
    HTLCsCF.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec pocs_cf(ledger()) -> rocksdb:cf_handle().
pocs_cf(#ledger_v1{mode=active, active=#sub_ledger_v1{pocs=PoCsCF}}) ->
    PoCsCF;
pocs_cf(#ledger_v1{mode=delayed, delayed=#sub_ledger_v1{pocs=PoCsCF}}) ->
    PoCsCF.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec routing_cf(ledger()) -> rocksdb:cf_handle().
routing_cf(#ledger_v1{mode=active, active=#sub_ledger_v1{routing=RoutingCF}}) ->
    RoutingCF;
routing_cf(#ledger_v1{mode=delayed, delayed=#sub_ledger_v1{routing=RoutingCF}}) ->
    RoutingCF.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec cache_put(ledger(), rocksdb:cf_handle(), any(), any()) -> ok.
cache_put(Ledger, CF, Key, Value) ->
    {Context, Cache} = context_cache(Ledger),
    CFCache = case ets:lookup(Cache, CF) of
        [] -> #{};
        [{CF, X}] -> X
    end,
    ets:insert(Cache, {CF, maps:put(Key, Value, CFCache)}),
    rocksdb:batch_put(Context, CF, Key, Value).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec cache_get(ledger(), rocksdb:cf_handle(), any(), [any()]) -> {ok, any()} | {error, any()} | not_found.
cache_get(#ledger_v1{db=DB}=Ledger, CF, Key, Options) ->
    case context_cache(Ledger) of
        {_, undefined} ->
            rocksdb:get(DB, CF, Key, Options);
        {Context, Cache} ->
            case ets:lookup(Cache, CF) of
                [] ->
                    cache_get(context_cache({Context, undefined}, Ledger), CF, Key, Options);
                [{CF, CFCache}] ->
                    case maps:find(Key, CFCache) of
                        {ok, Value} ->
                            {ok, Value};
                        error ->
                            cache_get(context_cache({Context, undefined}, Ledger), CF, Key, Options)
                    end
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec cache_delete(ledger(), rocksdb:cf_handle(), any()) -> ok.
cache_delete(Ledger, CF, Key) ->
    {Context, Cache} = context_cache(Ledger),
    case ets:lookup(Cache, CF) of
        [] ->
            ok;
        [{CF, CFCache}] ->
            ets:insert(Cache, {CF, maps:remove(Key, CFCache)})
    end,
    rocksdb:batch_delete(Context, CF, Key).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec open_db(file:filename_all()) -> {ok, rocksdb:db_handle(), [rocksdb:cf_handle()]} | {error, any()}.
open_db(Dir) ->
    DBDir = filename:join(Dir, ?DB_FILE),
    ok = filelib:ensure_dir(DBDir),
    DBOptions = [{create_if_missing, true}],
    DefaultCFs = [
        "default", "active_gateways", "entries", "htlcs", "pocs", "routing",
        "delayed_default", "delayed_active_gateways", "delayed_entries",
        "delayed_htlcs", "delayed_pocs", "delayed_routing"
    ],
    ExistingCFs =
        case rocksdb:list_column_families(DBDir, DBOptions) of
            {ok, CFs0} ->
                CFs0;
            {error, _} ->
                ["default"]
        end,

    {ok, DB, OpenedCFs} = rocksdb:open_with_cf(DBDir, DBOptions,  [{CF, []} || CF <- ExistingCFs]),

    L1 = lists:zip(ExistingCFs, OpenedCFs),
    L2 = lists:map(
        fun(CF) ->
            {ok, CF1} = rocksdb:create_column_family(DB, CF, []),
            {CF, CF1}
        end,
        DefaultCFs -- ExistingCFs
    ),
    L3 = L1 ++ L2,
    {ok, DB, [proplists:get_value(X, L3) || X <- DefaultCFs]}.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

find_entry_test() ->
    BaseDir = test_utils:tmp_dir("find_entry_test"),
    Ledger = new(BaseDir),
    ?assertEqual({error, not_found}, find_entry(<<"test">>, Ledger)).

find_gateway_info_test() ->
    BaseDir = test_utils:tmp_dir("find_gateway_info_test"),
    Ledger = new(BaseDir),
    ?assertEqual({error, not_found}, find_gateway_info(<<"address">>, Ledger)).

mode_test() ->
    BaseDir = test_utils:tmp_dir("mode_test"),
    Ledger = new(BaseDir),
    ?assertEqual({error, not_found}, consensus_members(Ledger)),
    Ledger1 = new_context(Ledger),
    ok = consensus_members([1, 2, 3], Ledger1),
    ok = commit_context(Ledger1),
    ?assertEqual({ok, [1, 2, 3]}, consensus_members(Ledger)),
    Ledger2 = mode(delayed, Ledger1),
    Ledger3 = new_context(Ledger2),
    ?assertEqual({error, not_found}, consensus_members(Ledger3)).

consensus_members_1_test() ->
    BaseDir = test_utils:tmp_dir("consensus_members_1_test"),
    Ledger = new(BaseDir),
    ?assertEqual({error, not_found}, consensus_members(Ledger)).

consensus_members_2_test() ->
    BaseDir = test_utils:tmp_dir("consensus_members_2_test"),
    Ledger = new(BaseDir),
    Ledger1 = new_context(Ledger),
    ok = consensus_members([1, 2, 3], Ledger1),
    ok = commit_context(Ledger1),
    ?assertEqual({ok, [1, 2, 3]}, consensus_members(Ledger)).

active_gateways_test() ->
    BaseDir = test_utils:tmp_dir("active_gateways_test"),
    Ledger = new(BaseDir),
    ?assertEqual(#{}, active_gateways(Ledger)).

add_gateway_test() ->
    BaseDir = test_utils:tmp_dir("add_gateway_test"),
    Ledger = new(BaseDir),
    Ledger1 = new_context(Ledger),
    ok = add_gateway(<<"owner_address">>, <<"gw_address">>, Ledger1),
    ok = commit_context(Ledger1),
    ?assertMatch(
        {ok, _},
        find_gateway_info(<<"gw_address">>, Ledger)
    ),
    ?assertEqual({error, gateway_already_active}, add_gateway(<<"owner_address">>, <<"gw_address">>, Ledger)).

add_gateway_location_test() ->
    BaseDir = test_utils:tmp_dir("add_gateway_location_test"),
    Ledger = new(BaseDir),
    Ledger1 = new_context(Ledger),
    ok = add_gateway(<<"owner_address">>, <<"gw_address">>, Ledger1),
    ok = commit_context(Ledger1),
    Ledger2 = new_context(Ledger),
    ?assertEqual(
       ok,
       add_gateway_location(<<"gw_address">>, 1, 1, Ledger2)
    ).

credit_account_test() ->
    BaseDir = test_utils:tmp_dir("credit_account_test"),
    Ledger = new(BaseDir),
    Ledger1 = new_context(Ledger),
    ok = credit_account(<<"address">>, 1000, Ledger1),
    ok = commit_context(Ledger1),
    {ok, Entry} = find_entry(<<"address">>, Ledger),
    ?assertEqual(1000, blockchain_ledger_entry_v1:balance(Entry)).

debit_account_test() ->
    BaseDir = test_utils:tmp_dir("debit_account_test"),
    Ledger = new(BaseDir),
    Ledger1 = new_context(Ledger),
    ok = credit_account(<<"address">>, 1000, Ledger1),
    ok = commit_context(Ledger1),
    ?assertEqual({error, {bad_nonce, {payment, 0, 0}}}, debit_account(<<"address">>, 1000, 0, Ledger)),
    ?assertEqual({error, {bad_nonce, {payment, 12, 0}}}, debit_account(<<"address">>, 1000, 12, Ledger)),
    ?assertEqual({error, {insufficient_balance, 9999, 1000}}, debit_account(<<"address">>, 9999, 1, Ledger)),
    Ledger2 = new_context(Ledger),
    ok = debit_account(<<"address">>, 500, 1, Ledger2),
    ok = commit_context(Ledger2),
    {ok, Entry} = find_entry(<<"address">>, Ledger),
    ?assertEqual(500, blockchain_ledger_entry_v1:balance(Entry)),
    ?assertEqual(1, blockchain_ledger_entry_v1:nonce(Entry)).

debit_fee_test() ->
    BaseDir = test_utils:tmp_dir("debit_fee_test"),
    Ledger = new(BaseDir),
    Ledger1 = new_context(Ledger),
    ok = credit_account(<<"address">>, 1000, Ledger1),
    ok = commit_context(Ledger1),
    ?assertEqual({error, {insufficient_balance, 9999, 1000}}, debit_fee(<<"address">>, 9999, Ledger)),
    Ledger2 = new_context(Ledger),
    ok = debit_fee(<<"address">>, 500, Ledger2),
    ok = commit_context(Ledger2),
    {ok, Entry} = find_entry(<<"address">>, Ledger),
    ?assertEqual(500, blockchain_ledger_entry_v1:balance(Entry)),
    ?assertEqual(0, blockchain_ledger_entry_v1:nonce(Entry)).

poc_test() ->
    BaseDir = test_utils:tmp_dir("poc_test"),
    Ledger = new(BaseDir),

    Challenger0 = <<"challenger0">>,
    Challenger1 = <<"challenger1">>,

    OnionKeyHash0 = <<"onion_key_hash0">>,
    OnionKeyHash1 = <<"onion_key_hash1">>,

    OwnerAddr = <<"owner_address">>,
    Location = 123456789,
    Nonce = 1,
    Score = 0.1,
   
    SecretHash = <<"secret_hash">>,
    Block = blockchain_block_v1:new(#{
        prev_hash => <<"00000">>,
        height => 12,
        time => 0,
        hbbft_round => [],
        transactions => [],
        signatures => []
    }),

    ?assertEqual({error, not_found}, find_poc(OnionKeyHash0, Ledger)),

    commit(
        fun(L) ->
            ok = add_gateway(OwnerAddr, Challenger0, Location, Nonce, Score, L),
            ok = add_gateway(OwnerAddr, Challenger1, Location, Nonce, Score, L),
            ok = request_poc(OnionKeyHash0, SecretHash, Challenger0, Block, L)
        end,
        Ledger
    ),
    PoC0 = blockchain_ledger_poc_v1:new(SecretHash, OnionKeyHash0, Challenger0),
    ?assertEqual({ok, [PoC0]} ,find_poc(OnionKeyHash0, Ledger)),
    {ok, GwInfo0} = find_gateway_info(Challenger0, Ledger),
    ?assertEqual(12, blockchain_ledger_gateway_v1:last_poc_challenge(GwInfo0)),
    ?assertEqual(OnionKeyHash0, blockchain_ledger_gateway_v1:last_poc_onion_key_hash(GwInfo0)),

    commit(
        fun(L) ->
            ok = request_poc(OnionKeyHash0, SecretHash, Challenger1, Block, L)
        end,
        Ledger
    ),
    PoC1 = blockchain_ledger_poc_v1:new(SecretHash, OnionKeyHash0, Challenger1),
    ?assertEqual({ok, [PoC1, PoC0]}, find_poc(OnionKeyHash0, Ledger)),

    commit(
        fun(L) ->
            ok = delete_poc(OnionKeyHash0, Challenger0, L)
        end,
        Ledger
    ),
    ?assertEqual({ok, [PoC1]} ,find_poc(OnionKeyHash0, Ledger)),

    commit(
        fun(L) ->
            ok = delete_poc(OnionKeyHash0, Challenger1, L)
        end,
        Ledger
    ),
    ?assertEqual({error, not_found} ,find_poc(OnionKeyHash0, Ledger)),

    commit(
        fun(L) ->
            ok = request_poc(OnionKeyHash0, SecretHash, Challenger0, Block, L)
        end,
        Ledger
    ),
    ?assertEqual({ok, [PoC0]} ,find_poc(OnionKeyHash0, Ledger)),

    commit(
        fun(L) ->
            ok = request_poc(OnionKeyHash1, SecretHash, Challenger0, Block, L)
        end,
        Ledger
    ),
    ?assertEqual({error, not_found} ,find_poc(OnionKeyHash0, Ledger)),
    PoC2 = blockchain_ledger_poc_v1:new(SecretHash, OnionKeyHash1, Challenger0),
    ?assertEqual({ok, [PoC2]}, find_poc(OnionKeyHash1, Ledger)),
    {ok, GwInfo1} = find_gateway_info(Challenger0, Ledger),
    ?assertEqual(12, blockchain_ledger_gateway_v1:last_poc_challenge(GwInfo1)),
    ?assertEqual(OnionKeyHash1, blockchain_ledger_gateway_v1:last_poc_onion_key_hash(GwInfo1)),

    ok.

commit(Fun, Ledger0) ->
    Ledger1 = new_context(Ledger0),
    _ = Fun(Ledger1),
    commit_context(Ledger1).


routing_test() ->
    BaseDir = test_utils:tmp_dir("routing_test"),
    Ledger = new(BaseDir),
    Ledger1 = new_context(Ledger),
    ?assertEqual({error, not_found}, find_routing(1, Ledger1)),
    ?assertEqual({ok, 0}, get_oui_counter(Ledger1)),

    Ledger2 = new_context(Ledger),
    ok = add_oui(<<"owner">>, [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>], Ledger2),
    ok = commit_context(Ledger2),
    {ok, Routing0} = find_routing(1, Ledger),
    ?assertEqual(<<"owner">>, blockchain_ledger_routing_v1:owner(Routing0)),
    ?assertEqual(1, blockchain_ledger_routing_v1:oui(Routing0)),
    ?assertEqual([<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>], blockchain_ledger_routing_v1:addresses(Routing0)),
    ?assertEqual(0, blockchain_ledger_routing_v1:nonce(Routing0)),

    Ledger3 = new_context(Ledger),
    ok = add_oui(<<"owner2">>, [<<"/p2p/random">>], Ledger3),
    ok = commit_context(Ledger3),
    {ok, Routing1} = find_routing(2, Ledger),
    ?assertEqual(<<"owner2">>, blockchain_ledger_routing_v1:owner(Routing1)),
    ?assertEqual(2, blockchain_ledger_routing_v1:oui(Routing1)),
    ?assertEqual([<<"/p2p/random">>], blockchain_ledger_routing_v1:addresses(Routing1)),
    ?assertEqual(0, blockchain_ledger_routing_v1:nonce(Routing1)),

    Ledger4 = new_context(Ledger),
    ok = add_routing(<<"owner2">>, 2, [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>], 1, Ledger4),
    ok = commit_context(Ledger4),
    {ok, Routing2} = find_routing(2, Ledger),
    ?assertEqual(<<"owner2">>, blockchain_ledger_routing_v1:owner(Routing2)),
    ?assertEqual(2, blockchain_ledger_routing_v1:oui(Routing2)),
    ?assertEqual([<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>], blockchain_ledger_routing_v1:addresses(Routing2)),
    ?assertEqual(1, blockchain_ledger_routing_v1:nonce(Routing2)),

    ?assertEqual({ok, [1]}, blockchain_ledger_v1:find_ouis(<<"owner">>, Ledger)),
    ?assertEqual({ok, [2]}, blockchain_ledger_v1:find_ouis(<<"owner2">>, Ledger)),

    ok.

-endif.
