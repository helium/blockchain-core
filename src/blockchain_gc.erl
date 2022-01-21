-module(blockchain_gc).

-behaviour(gen_server).

-export([start_link/0]).

-export([init/1, handle_info/2, handle_cast/2, handle_call/3]).

-record(state, {
         bytes = 0
        }).

start_link() ->
    gen_server:start_link(?MODULE, [], []).

init([]) ->
    %% if we were started with the GC env var, try to keep
    %% the blockchain.db reasonably sized
    case os:getenv("BLOCKCHAIN_ROCKSDB_GC_BYTES") of
        false -> ignore;
        ValString ->
            try list_to_integer(ValString, 10) of
                _BytesToGC ->
                    blockchain_event:add_handler(self()),
                    {ok, #state{}}
            catch _:_ ->
                      ignore
            end
    end.

handle_info({blockchain_event, {add_block, Hash, _Sync, Ledger}}, State) ->
    %% use add block event to track how much we're writing to rocks, roughly
    {ok, Bin} = blockchain_ledger_v1:get_raw_block(Hash, Ledger),
    {noreply, maybe_gc(State#state{bytes=byte_size(Bin) + State#state.bytes})};
handle_info(_Other, State) ->
    {noreply, State}.

handle_cast(_Other, State) ->
    {noreply, State}.

handle_call(_Other, _From, State) ->
    {reply, ok, State}.


maybe_gc(State=#state{bytes=Bytes}) ->
    case application:get_env(blockchain, gc_byte_interval, 100 * 1024 * 1024) < Bytes of
        true ->
            %% run a GC, this blocks the process
            blockchain:rocksdb_gc(Bytes, blockchain_worker:blockchain()),
            State#state{bytes=0};
        false ->
            State
    end.
