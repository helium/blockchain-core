%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel Client ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channel_client).

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

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(SERVER, ?MODULE).

-record(state, {
    db :: rocksdb:db_handle() | undefined,
    swarm :: pid(),
    state_channels = #{} :: #{libp2p_crypto:pubkey_bin() => blockchain_state_channel:state_channel()}
}).

-type state() :: #state{}.

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start_link(Args) ->
    gen_server:start_link({local, ?SERVER}, ?SERVER, Args, []).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init([Swarm]=_Args) ->
    lager:info("~p init with ~p", [?SERVER, _Args]),
    {ok, DB} = blockchain_state_channel_db:get(),
    {ok, #state{db=DB, swarm=Swarm}}.

handle_call(_Msg, _From, State) ->
    lager:warning("rcvd unknown call msg: ~p from: ~p", [_Msg, _From]),
    {reply, ok, State}.

handle_cast({payment, Payment}, #state{state_channels=_SCS}=State) ->
    case validate_payment(Payment, State) of
        {error, _Reason} ->
            lager:warning("ignored unvalid payment ~p ~p", [_Reason, Payment]),
            {noreply, State};
        ok ->
            % Owner = blockchain_dcs_payment:payer(Payment),
            % SC = maps:get(Owner, SCS, undefined),
            {noreply, State}
    end;
handle_cast(_Msg, State) ->
    lager:warning("rcvd unknown cast msg: ~p", [_Msg]),
    {noreply, State}.

handle_info(_Msg, State) ->
    lager:warning("rcvd unknown info msg: ~p", [_Msg]),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason,  #state{db=DB}) ->
    ok = rocksdb:close(DB).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

-spec validate_payment(blockchain_dcs_payment:dcs_payment(), state()) -> ok | {error, any()}.
validate_payment(Payment, #state{state_channels=SCS}) ->
    Owner = blockchain_dcs_payment:payer(Payment),
    case maps:get(Owner, SCS, undefined) of
        undefined -> {error, unknown_payment};
        SC -> blockchain_state_channel:validate_payment(Payment, SC)
    end.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

-endif.