%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Stream Handler ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_handler).

-behavior(libp2p_framed_stream).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([
    server/4,
    client/2
]).

%% ------------------------------------------------------------------
%% libp2p_framed_stream Function Exports
%% ------------------------------------------------------------------
-export([
    init/3,
    handle_data/3
]).

-record(state, {
    group :: undefined | pid(),
    parent :: undefined | pid(),
    ref :: undefined | reference()
}).

client(Connection, Args) ->
    libp2p_framed_stream:client(?MODULE, Connection, Args).

server(Connection, Path, _TID, Args) ->
    libp2p_framed_stream:server(?MODULE, Connection, [Path | Args]).

%% ------------------------------------------------------------------
%% libp2p_framed_stream Function Definitions
%% ------------------------------------------------------------------
init(client, _Conn, [Parent, Ref]) ->
    {ok, #state{parent=Parent, ref=Ref}};
init(server, _Conn, [_Path, _Parent, Group]) ->
    %lager:info("txn handler accepted connection~n"),
    {ok, #state{group=Group}}.

handle_data(client, <<"ok">>, State=#state{parent=Parent, ref=Ref}) ->
    Parent ! {Ref, ok},
    {noreply, State};
handle_data(client, <<"error">>, State=#state{parent=Parent, ref=Ref}) ->
    Parent ! {Ref, error},
    {noreply, State};
handle_data(server, Data, State=#state{group=Group}) ->
    case binary_to_term(Data) of
        {TxnType, Txn} ->
            lager:info("Got ~p type transaction: ~p", [TxnType, Txn]),
            case libp2p_group_relcast:handle_input(Group, Txn) of
                ok ->
                    {noreply, State, <<"ok">>};
                _ ->
                    {noreply, State, <<"error">>}
            end;
        _ ->
            lager:notice("transaction_handler got unknown data"),
            {noreply, State, <<"error">>}
    end.
