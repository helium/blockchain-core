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
    txn_hash :: undefined | blockchain_txn:hash()
}).

client(Connection, Args) ->
    libp2p_framed_stream:client(?MODULE, Connection, Args).

server(Connection, Path, _TID, Args) ->
    libp2p_framed_stream:server(?MODULE, Connection, [Path | Args]).

%% ------------------------------------------------------------------
%% libp2p_framed_stream Function Definitions
%% ------------------------------------------------------------------
init(client, _Conn, [Parent, TxnHash]) ->
    {ok, #state{parent=Parent, txn_hash=TxnHash}};
init(server, _Conn, [_Path, _Parent, Group]) ->
    %lager:info("txn handler accepted connection~n"),
    {ok, #state{group=Group}}.

handle_data(client, <<"ok">>, State=#state{parent=Parent, txn_hash=TxnHash}) ->
    Parent ! {blockchain_txn_response, {ok, TxnHash}},
    {stop, normal, State};
handle_data(client, <<"error">>, State=#state{parent=Parent, txn_hash=TxnHash}) ->
    Parent ! {blockchain_txn_response, {error, TxnHash}},
    {stop, normal, State};
handle_data(server, Data, State=#state{group=Group}) ->
    try
        Txn = blockchain_txn:deserialize(Data),
        lager:debug("Got ~p type transaction: ~p", [blockchain_txn:type(Txn), Txn]),
        Ref = make_ref(),
        libp2p_group_relcast:handle_command(Group, {self(), Ref, Txn}),
        receive
            {Ref, ok} ->
                {stop, normal, State, <<"ok">>};
            {Ref, _} ->
                {stop, normal, State, <<"error">>}
        end
    catch _What:Why ->
            lager:notice("transaction_handler got bad data: ~p", [Why]),
            {stop, normal, State, <<"error">>}
    end.
