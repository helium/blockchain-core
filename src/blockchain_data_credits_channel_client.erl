%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Data Credits Channel Client ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_data_credits_channel_client).

-behavior(gen_server).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------
-export([
    start_link/1
]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-include("blockchain.hrl").
-include("pb/blockchain_data_credits_handler_pb.hrl").

-define(SERVER, ?MODULE).

-record(state, {
    keys :: libp2p_crypto:key_map(),
    db :: rocksdb:db_handle(),
    cf :: rocksdb:cf_handle(),
    credits = 0 :: non_neg_integer()
}).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start_link(Args) ->
    gen_server:start_link(?SERVER, Args, []).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init([Keys, DB, CF]=Args) ->
    lager:info("~p init with ~p", [?SERVER, Args]),
    {ok, #state{
        keys=Keys,
        db=DB,
        cf=CF
    }}.

handle_call(_Msg, _From, State) ->
    lager:warning("rcvd unknown call msg: ~p from: ~p", [_Msg, _From]),
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    lager:warning("rcvd unknown cast msg: ~p", [_Msg]),
    {noreply, State}.

handle_info(_Msg, State) ->
    lager:warning("rcvd unknown info msg: ~p", [_Msg]),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------