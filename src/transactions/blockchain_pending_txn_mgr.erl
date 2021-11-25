-module(blockchain_pending_txn_mgr).

-include_lib("helium_proto/include/blockchain_txn_pb.hrl").

-export([
    init/1,
    load_chain/2,
    load_block/5,
    terminate/2
]).

%% API
-export([submit_txn/1, submit_txn/2, get_txn_status/1, get_txn/1]).

-define(DB_FILE, "pendning_transactions.db").
-define(TXN_STATUS_CLEARED, 0).
-define(TXN_STATUS_FAILED, 1).

-record(state, {
    db :: rocksdb:db_handle(),
    default :: rocksdb:cf_handle(),
    pending :: rocksdb:cf_handle(),
    status :: rocksdb:cf_handle()
}).


init(Args) ->
    Dir = filename:join(proplists:get_value(base_dir, Args, "data"), ?DB_FILE),
    case load_db(Dir) of
        {error, {db_open, "Corruption:" ++ _Reason}} ->
            lager:error("DB could not be opened corrupted ~p, cleaning up", [_Reason]),
            ok = bn_db:clean_db(Dir),
            init(Args);
        {ok, State} ->
            persistent_term:put(?MODULE, State),
            {ok, State}
    end.


load_chain(_Chain, State = #state{}) ->
    Submitted = submit_pending_txns(State),
    lager:info("Submitted ~p pending transactions", [Submitted]),
    {ok, State}.

load_block(_Hash, Block, _Sync, _Ledger, State = #state{db = DB, default = DefaultCF}) ->
    ok = bn_db:put_follower_height(DB, DefaultCF, blockchain_block:height(Block)),
    {ok, State}.

terminate(_Reason, #state{db = DB}) ->
    rocksdb:close(DB).

%%
%% API
%%
-spec submit_txn(blockchain_txn:txn()) -> {ok, TxnKey :: blockchain_txn_mgr:txn_key()} | {error, term()}.
submit_txn(Txn) ->
    {ok, State} = get_state(),
    Key = get_txn_key(),
    submit_txn(Txn, Key, State).

-spec submit_txn(blockchain_txn:txn(), blockchain_txn_mgr:txn_key()) -> {ok, TxnKey :: blockchain_txn_mgr:txn_key()} | {error, term()}.
submit_txn(Txn, TxnKey) ->
    {ok, State} = get_state(),
    submit_txn(Txn, TxnKey, State).

-type txn_status() :: pending | {failed, Reason :: binary()}.

-spec get_txn_status(TxnKey :: blockchain_txn_mgr:txn_key()) -> {ok, txn_status()} | {error, term()}.
get_txn_status(TxnKey) ->
    %% check if the txn is queued in txn mgr
    %% if it is then assume its pending
    %% if not the check rocksdb to see if it
    %% was previously confirmed or failed
    %% this saves any unnecessary and slower reads to rocksdb
    case blockchain_txn_mgr:txn_status(TxnKey) of
        {ok, PendingDetails} ->
            {ok, pending, PendingDetails};
        {error, _Reason} ->
            {ok, State} = get_state(),
            case get_txn_status(TxnKey, State) of
                {ok, {cleared, _Block}} = Resp ->
                    Resp;
                {ok, {failed, _Reason}} = Resp ->
                    Resp;
                {ok, pending} ->
                    %% if we hit here its odd
                    %% as the txn should have been found
                    %% previously in the call blockchain_txn_mgr:txn_status/1
                    %% default to pending with empty details
                    {ok, pending, maps:new()};
                {error, txn_not_found} = Resp ->
                    Resp

            end
    end.

-spec get_txn(blockchain_txn_mgr:txn_key()) -> {ok, blockchain_txn:txn()} | {error, term()}.
get_txn(TxnKey) ->
    {ok, State} = get_state(),
    get_txn(TxnKey, State).

%%
%% Internal
%%

-spec get_txn_key()-> blockchain_txn_mgr:txn_key().
get_txn_key()->
    %% define a unique value to use as the cache key for the received txn, for now its just a mono increasing timestamp.
    %% Timestamp is a poormans key but as txns are serialised via a single txn mgr per node, it satisfies the need here
    erlang:monotonic_time().

get_state() ->
    bn_db:get_state(?MODULE).

-spec get_txn_status(TxnKey ::  blockchain_txn_mgr:txn_key(), #state{}) ->
    {ok, {failed, Reason :: binary()} | {cleared, Block :: pos_integer()} | pending}
    | {error, not_found}.
get_txn_status(TxnKey, #state{db = DB, pending = PendingCF, status = StatusCF}) ->
    case rocksdb:get(DB, StatusCF, TxnKey, []) of
        {ok, <<(?TXN_STATUS_CLEARED):8, Block:64/integer-unsigned-little>>} ->
            {ok, {cleared, Block}};
        {ok, <<(?TXN_STATUS_FAILED):8, Reason/binary>>} ->
            {ok, {failed, Reason}};
        not_found ->
            case rocksdb:get(DB, PendingCF, TxnKey, []) of
                not_found ->
                    {error, no_pending_txn_found};
                {ok, _} ->
                    {ok, pending}
            end
    end.

-spec submit_pending_txns(#state{}) -> non_neg_integer().
submit_pending_txns(State = #state{db = DB, pending = PendingCF}) ->
    %% iterate over the transactions and submit each one of them
    {ok, Itr} = rocksdb:iterator(DB, PendingCF, []),
    Submitted = submit_pending_txns(Itr, rocksdb:iterator_move(Itr, first), State, 0),
    catch rocksdb:iterator_close(Itr),
    Submitted.

submit_pending_txns(_Itr, {error, _Error}, #state{}, Acc) ->
    Acc;
submit_pending_txns(Itr, {ok, Hash, BinTxn}, State = #state{}, Acc) ->
    try blockchain_txn:deserialize(BinTxn) of
        Txn ->
            blockchain_txn_mgr:submit(Txn, fun(Result) ->
                finalize_txn(Hash, Result, State)
            end)
    catch
        What:Why ->
            lager:warning("Error while fetching pending transaction: ~p: ~p ~p", [
                Hash,
                What,
                Why
            ])
    end,
    submit_pending_txns(Itr, rocksdb:iterator_move(Itr, next), State, Acc + 1).

finalize_txn(Key, Status, #state{db = DB, pending = PendingCF, status = StatusCF}) ->
    {ok, Batch} = rocksdb:batch(),
    %% Set cleared or failed status
    case Status of
        ok ->
            rocksdb:batch_put(
                Batch,
                StatusCF,
                Key,
                <<(?TXN_STATUS_CLEARED):8, 0:64/integer-unsigned-little>>
            );
        {error, Error} ->
            ErrorBin = list_to_binary(lists:flatten(io_lib:format("~p", [Error]))),
            rocksdb:batch_put(
                Batch,
                StatusCF,
                Key,
                <<(?TXN_STATUS_FAILED):8, ErrorBin/binary>>
            )
    end,
    %% Delete the transaction from the pending table
    rocksdb:batch_delete(Batch, PendingCF, Key),
    ok = rocksdb:write_batch(DB, Batch, [{sync, true}]).

submit_txn(Txn, TxnKey, State = #state{db = DB, pending = PendingCF}) ->
    {ok, Batch} = rocksdb:batch(),

    ok = rocksdb:batch_put(Batch, PendingCF, TxnKey, blockchain_txn:serialize(Txn)),
    ok = rocksdb:write_batch(DB, Batch, [{sync, true}]),
    blockchain_txn_mgr:submit(Txn, TxnKey, fun(Result) ->
        finalize_txn(TxnKey, Result, State)
    end),
    {ok, TxnKey}.

-spec get_txn(TxnKey ::  blockchain_txn_mgr:txn_key(), #state{}) -> {ok, blockchain_txn:txn()} | {error, term()}.
get_txn(TxnKey, #state{db = DB, pending = PendingCF}) ->
    case rocksdb:get(DB, PendingCF, TxnKey, []) of
        {ok, BinTxn} ->
            {ok, blockchain_txn:deserialize(BinTxn)};
        not_found ->
            {error, not_found};
        Error ->
            Error
    end.

-spec load_db(Dir :: file:filename_all()) -> {ok, #state{}} | {error, any()}.
load_db(Dir) ->
    case bn_db:open_db(Dir, ["default", "pending", "status"]) of
        {error, _Reason} = Error ->
            Error;
        {ok, DB, [DefaultCF, PendingCF, StatusCF]} ->
            State = #state{
                db = DB,
                default = DefaultCF,
                pending = PendingCF,
                status = StatusCF
            },
            compact_db(State),
            {ok, State}
    end.

compact_db(#state{db = DB, default = Default, pending = PendingCF, status = StatusCF}) ->
    rocksdb:compact_range(DB, Default, undefined, undefined, []),
    rocksdb:compact_range(DB, PendingCF, undefined, undefined, []),
    rocksdb:compact_range(DB, StatusCF, undefined, undefined, []),
    ok.