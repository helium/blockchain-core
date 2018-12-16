%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Ledger ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_ledger_v1).

-export([
    new/1,
    dir/1,

    new_context/1, delete_context/1, commit_context/1,

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

    clean/1, close/1
]).

-include("blockchain.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(ledger_v1, {
    dir :: file:filename_all(),
    db :: rocksdb:db_handle(),
    default :: rocksdb:cf_handle(),
    active_gateways :: rocksdb:cf_handle(),
    entries :: rocksdb:cf_handle(),
    htlcs :: rocksdb:cf_handle(),
    context :: undefined | rocksdb:batch_handle()
}).

-define(DB_FILE, "ledger.db").
-define(CURRENT_HEIGHT, <<"current_height">>).
-define(TRANSACTION_FEE, <<"transaction_fee">>).
-define(CONSENSUS_MEMBERS, <<"consensus_members">>).

-type ledger() :: #ledger_v1{}.
-type entries() :: #{libp2p_crypto:address() => blockchain_ledger_entry_v1:entry()}.
-type active_gateways() :: #{libp2p_crypto:address() => blockchain_ledger_gateway_v1:gateway()}.
-type htlcs() :: #{libp2p_crypto:address() => blockchain_ledger_htlc_v1:htlc()}.

-export_type([ledger/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(file:filename_all()) -> ledger().
new(Dir) ->
    {ok, DB, [DefaultCF, AGwsCF, EntriesCF, HTLCsCF]} = open_db(Dir),
    #ledger_v1{
        dir=Dir,
        db=DB,
        default=DefaultCF,
        active_gateways=AGwsCF,
        entries=EntriesCF,
        htlcs=HTLCsCF
    }.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec dir(ledger()) -> file:filename_all().
dir(Ledger) ->
    Ledger#ledger_v1.dir.

new_context(Ledger=#ledger_v1{context=undefined}) ->
    {ok, Batch} = rocksdb:batch(),
    Ledger#ledger_v1{context=Batch}.

delete_context(Ledger=#ledger_v1{context=Context}) ->
    %% we can just let the batch GC, but let's clear it just in case
    rocksdb:batch_clear(Context),
    Ledger#ledger_v1{context=undefined}.

commit_context(Ledger=#ledger_v1{db=DB, context=Context}) ->
    ok = rocksdb:write_batch(DB, Context, [{sync, true}]),
    delete_context(Ledger),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec current_height(ledger()) -> {ok, pos_integer()} | {error, any()}.
current_height(#ledger_v1{db=DB, default=DefaultCF}) ->
    case rocksdb:get(DB, DefaultCF, ?CURRENT_HEIGHT, []) of
        {ok, <<Height:64/integer-unsigned-big>>} ->
            {ok, Height};
        not_found ->
            {ok, 1};
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec increment_height(ledger()) -> ok | {error, any()}.
increment_height(#ledger_v1{context=Context, default=DefaultCF}=Ledger) ->
    case current_height(Ledger) of
        {error, _} ->
            rocksdb:batch_put(Context, DefaultCF, ?CURRENT_HEIGHT, <<1:64/integer-unsigned-big>>);
        {ok, Height0} ->
            Height1 = Height0 + 1,
            rocksdb:batch_put(Context, DefaultCF, ?CURRENT_HEIGHT, <<Height1:64/integer-unsigned-big>>)
    end.


%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec transaction_fee(ledger()) -> {ok, pos_integer()} | {error, any()}.
transaction_fee(#ledger_v1{db=DB, default=DefaultCF}) ->
    case rocksdb:get(DB, DefaultCF, ?TRANSACTION_FEE, []) of
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
update_transaction_fee(#ledger_v1{context=Context, default=DefaultCF}=Ledger) ->
    %% TODO - this should calculate a new transaction fee for the network
    %% TODO - based on the average of usage fees
    case ?MODULE:current_height(Ledger) of
        {error, _} ->
            ok;
        {ok, Fee0} ->
            Fee1 = Fee0 div 1000,
            rocksdb:batch_put(Context, DefaultCF, ?TRANSACTION_FEE, <<Fee1:64/integer-unsigned-big>>)
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec consensus_members(ledger()) -> {ok, [libp2p_crypto:address()]} | {error, any()}.
consensus_members(#ledger_v1{db=DB, default=DefaultCF}) ->
    case rocksdb:get(DB, DefaultCF, ?CONSENSUS_MEMBERS, []) of
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
-spec consensus_members([libp2p_crypto:address()], ledger()) ->  ok | {error, any()}.
consensus_members(Members, #ledger_v1{context=Context, default=DefaultCF}) ->
    Bin = erlang:term_to_binary(Members),
    rocksdb:batch_put(Context, DefaultCF, ?CONSENSUS_MEMBERS, Bin).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec active_gateways(ledger()) -> active_gateways().
active_gateways(#ledger_v1{db=DB, active_gateways=AGwsCF}) ->
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
entries(#ledger_v1{db=DB, entries=EntriesCF}) ->
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
htlcs(#ledger_v1{db=DB, htlcs=HTLCsCF}) ->
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
-spec find_gateway_info(libp2p_crypto:address(), ledger()) -> {ok, blockchain_ledger_gateway_v1:gateway()}
                                                              | {error, any()}.
find_gateway_info(Address, #ledger_v1{db=DB, active_gateways=AGwsCF}) ->
    case rocksdb:get(DB, AGwsCF, Address, []) of
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
-spec add_gateway(libp2p_crypto:address(), libp2p_crypto:address(), ledger()) -> ok | {error, gateway_already_active}.
add_gateway(OwnerAddr, GatewayAddress, #ledger_v1{context=Context, active_gateways=AGwsCF}=Ledger) ->
    case ?MODULE:find_gateway_info(GatewayAddress, Ledger) of
        {ok, _} ->
            {error, gateway_already_active};
        _ ->
            Gateway = blockchain_ledger_gateway_v1:new(OwnerAddr, undefined),
            Bin = blockchain_ledger_gateway_v1:serialize(Gateway),
            rocksdb:batch_put(Context, AGwsCF, GatewayAddress, Bin)
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
                  Ledger :: ledger()) -> ok | {error, gateway_already_active}.
add_gateway(OwnerAddr,
            GatewayAddress,
            Location,
            LastPocChallenge,
            Nonce,
            Score,
            #ledger_v1{context=Context, active_gateways=AGwsCF}=Ledger) ->
    case ?MODULE:find_gateway_info(GatewayAddress, Ledger) of
        {ok, _} ->
            {error, gateway_already_active};
        _ ->
            Gateway = blockchain_ledger_gateway_v1:new(OwnerAddr,
                                                       Location,
                                                       LastPocChallenge,
                                                       Nonce,
                                                       Score),
            Bin = blockchain_ledger_gateway_v1:serialize(Gateway),
            rocksdb:batch_put(Context, AGwsCF, GatewayAddress, Bin)
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec add_gateway_location(libp2p_crypto:address(), non_neg_integer(), non_neg_integer(), ledger()) -> ok | {error, no_active_gateway}.
add_gateway_location(GatewayAddress, Location, Nonce, #ledger_v1{context=Context, active_gateways=AGwsCF}=Ledger) ->
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
            rocksdb:batch_put(Context, AGwsCF, GatewayAddress, Bin)
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec request_poc(libp2p_crypto:address(), ledger()) -> ok | {error, any()}.
request_poc(GatewayAddress, #ledger_v1{context=Context, active_gateways=AGwsCF}=Ledger) ->
    case ?MODULE:find_gateway_info(GatewayAddress, Ledger) of
        {error, _} ->
            {error, no_active_gateway};
        {ok, Gw} ->
            case blockchain_ledger_gateway_v1:location(Gw) of
                undefined ->
                    {error, no_gateway_location};
                _Location ->
                    case ?MODULE:current_height(Ledger) of
                        {error, _}=Error ->
                            Error;
                        {ok, Height} ->
                            case blockchain_ledger_gateway_v1:last_poc_challenge(Gw) > (Height - 30) of
                                false ->
                                    {error, too_many_challenges};
                                true ->
                                    Gw1 = blockchain_ledger_gateway_v1:last_poc_challenge(Height, Gw),
                                    Bin = blockchain_ledger_gateway_v1:serialize(Gw1),
                                    rocksdb:batch_put(Context, AGwsCF, GatewayAddress, Bin)
                            end
                    end
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec find_entry(libp2p_crypto:address(), ledger()) -> {ok, blockchain_ledger_entry_v1:entry()}
                                                       | {error, any()}.
find_entry(Address, #ledger_v1{db=DB, entries=EntriesCF}) ->
    case rocksdb:get(DB, EntriesCF, Address, []) of
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
-spec credit_account(libp2p_crypto:address(), integer(), ledger()) -> ok | {error, any()}.
credit_account(Address, Amount, #ledger_v1{context=Context, entries=EntriesCF}=Ledger) ->
    case ?MODULE:find_entry(Address, Ledger) of
        {error, _} ->
            Entry = blockchain_ledger_entry_v1:new(0, Amount),
            Bin = blockchain_ledger_entry_v1:serialize(Entry),
            rocksdb:batch_put(Context, EntriesCF, Address, Bin);
        {ok, Entry} ->
            Entry1 = blockchain_ledger_entry_v1:new(
                blockchain_ledger_entry_v1:nonce(Entry),
                blockchain_ledger_entry_v1:balance(Entry) + Amount
            ),
            Bin = blockchain_ledger_entry_v1:serialize(Entry1),
            rocksdb:batch_put(Context, EntriesCF, Address, Bin)
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec debit_account(libp2p_crypto:address(), integer(), integer(), ledger()) -> ok | {error, any()}.
debit_account(Address, Amount, Nonce, #ledger_v1{context=Context, entries=EntriesCF}=Ledger) ->
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
                            rocksdb:batch_put(Context, EntriesCF, Address, Bin);
                        false ->
                            {error, {insufficient_balance, Amount, Balance}}
                    end;
                false ->
                    {error, {bad_nonce, Nonce, blockchain_ledger_entry_v1:nonce(Entry)}}
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec debit_fee(Address :: libp2p_crypto:address(), Fee :: integer(),  Ledger :: ledger()) -> ok | {error, any()}.
debit_fee(Address, Fee, #ledger_v1{context=Context, entries=EntriesCF}=Ledger) ->
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
                    rocksdb:batch_put(Context, EntriesCF, Address, Bin);
                false ->
                    {error, {insufficient_balance, Fee, Balance}}
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec find_htlc(libp2p_crypto:address(), ledger()) -> {ok, blockchain_ledger_htlc_v1:htlc()}
                                                      | {error, any()}.
find_htlc(Address, #ledger_v1{db=DB, htlcs=HTLCsCF}) ->
    case rocksdb:get(DB, HTLCsCF, Address, []) of
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
-spec add_htlc(libp2p_crypto:address(), libp2p_crypto:address(), libp2p_crypto:address(),
               non_neg_integer(),  binary(), non_neg_integer(), ledger()) -> ok | {error, any()}.
add_htlc(Address, Payer, Payee, Amount, Hashlock, Timelock, #ledger_v1{context=Context, htlcs=HTLCsCF}=Ledger) ->
    case ?MODULE:find_htlc(Address, Ledger) of
        {ok, _} ->
            {error, address_already_exists};
        {error, _} ->
            HTLC = blockchain_ledger_htlc_v1:new(Payer, Payee, Amount, Hashlock, Timelock),
            Bin = blockchain_ledger_htlc_v1:serialize(HTLC),
            rocksdb:batch_put(Context, HTLCsCF, Address, Bin)
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec redeem_htlc(libp2p_crypto:address(), libp2p_crypto:address(), ledger()) -> ok | {error, any()}.
redeem_htlc(Address, Payee, #ledger_v1{context=Context}=Ledger) ->
    case ?MODULE:find_htlc(Address,  #ledger_v1{htlcs=HTLCsCF}=Ledger) of
        {error, _}=Error ->
            Error;
        {ok, HTLC} ->
            Amount = blockchain_ledger_htlc_v1:balance(HTLC),
            case ?MODULE:credit_account(Payee, Amount, Ledger) of
                {error, _}=Error -> Error;
                ok -> rocksdb:batch_delete(Context, HTLCsCF, Address)
            end
    end.

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
-spec open_db(file:filename_all()) -> {ok, rocksdb:db_handle(), [rocksdb:cf_handle()]} | {error, any()}.
open_db(Dir) ->
    DBDir = filename:join(Dir, ?DB_FILE),
    ok = filelib:ensure_dir(DBDir),
    DBOptions = [{create_if_missing, true}],
    DefaultCFs = ["default", "active_gateways", "entries", "htlcs"],
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

consensus_members_1_test() ->
    BaseDir = test_utils:tmp_dir("consensus_members_1_test"),
    Ledger = new(BaseDir),
    ?assertEqual({error, not_found}, consensus_members(Ledger)).

consensus_members_2_test() ->
    BaseDir = test_utils:tmp_dir("consensus_members_2_test"),
    Ledger = new(BaseDir),
    Ledger1 = new_context(Ledger),
    ok = consensus_members([1, 2, 3], Ledger1),
    commit_context(Ledger1),
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
    commit_context(Ledger1),
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
    commit_context(Ledger1),
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
    commit_context(Ledger1),
    {ok, Entry} = find_entry(<<"address">>, Ledger),
    ?assertEqual(1000, blockchain_ledger_entry_v1:balance(Entry)).

debit_account_test() ->
    BaseDir = test_utils:tmp_dir("debit_account_test"),
    Ledger = new(BaseDir),
    Ledger1 = new_context(Ledger),
    ok = credit_account(<<"address">>, 1000, Ledger1),
    commit_context(Ledger1),
    ?assertEqual({error, {bad_nonce, 0, 0}}, debit_account(<<"address">>, 1000, 0, Ledger)),
    ?assertEqual({error, {bad_nonce, 12, 0}}, debit_account(<<"address">>, 1000, 12, Ledger)),
    ?assertEqual({error, {insufficient_balance, 9999, 1000}}, debit_account(<<"address">>, 9999, 1, Ledger)),
    Ledger2 = new_context(Ledger),
    ok = debit_account(<<"address">>, 500, 1, Ledger2),
    commit_context(Ledger2),
    {ok, Entry} = find_entry(<<"address">>, Ledger),
    ?assertEqual(500, blockchain_ledger_entry_v1:balance(Entry)),
    ?assertEqual(1, blockchain_ledger_entry_v1:nonce(Entry)).

debit_fee_test() ->
    BaseDir = test_utils:tmp_dir("debit_fee_test"),
    Ledger = new(BaseDir),
    Ledger1 = new_context(Ledger),
    ok = credit_account(<<"address">>, 1000, Ledger1),
    commit_context(Ledger1),
    ?assertEqual({error, {insufficient_balance, 9999, 1000}}, debit_fee(<<"address">>, 9999, Ledger)),
    Ledger2 = new_context(Ledger),
    ok = debit_fee(<<"address">>, 500, Ledger2),
    commit_context(Ledger2),
    {ok, Entry} = find_entry(<<"address">>, Ledger),
    ?assertEqual(500, blockchain_ledger_entry_v1:balance(Entry)),
    ?assertEqual(0, blockchain_ledger_entry_v1:nonce(Entry)).

-endif.
