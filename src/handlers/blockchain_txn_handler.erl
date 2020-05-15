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
    callback :: undefined | function(),
    parent :: undefined | pid(),
    txn_hash :: undefined | blockchain_txn:hash()
}).

client(Connection, Args) ->
    libp2p_framed_stream:client(?MODULE, Connection, Args).

server(Connection, _Path, _TID, Args) ->
    %% NOTE: server/4 in the handler is never called.
    %% When spawning a server its handled only in libp2p_framed_stream
    libp2p_framed_stream:server(?MODULE, Connection, [_Path | Args]).

%% ------------------------------------------------------------------
%% libp2p_framed_stream Function Definitions
%% ------------------------------------------------------------------
init(client, _Conn, [Parent, TxnHash]) ->
    {ok, #state{parent=Parent, txn_hash=TxnHash}};
init(server, _Conn, [_Path, _Parent, Callback] = _Args) ->
    {ok, #state{callback = Callback}}.

handle_data(client, <<"ok">>, State=#state{parent=Parent, txn_hash=TxnHash}) ->
    Parent ! {blockchain_txn_response, {ok, TxnHash}},
    {stop, normal, State};
handle_data(client, <<"no_group">>, State=#state{parent=Parent, txn_hash=TxnHash}) ->
    Parent ! {blockchain_txn_response, {no_group, TxnHash}},
    {stop, normal, State};
handle_data(client, <<"error">>, State=#state{parent=Parent, txn_hash=TxnHash}) ->
    Parent ! {blockchain_txn_response, {error, TxnHash}},
    {stop, normal, State};
handle_data(server, Data, State=#state{callback = Callback}) ->
    try
        Txn = blockchain_txn:deserialize(Data),
        lager:debug("Got ~p type transaction: ~s", [blockchain_txn:type(Txn), blockchain_txn:print(Txn)]),
        case Callback(Txn) of
            ok ->
                {stop, normal, State, <<"ok">>};
            {error, no_group} ->
                {stop, normal, State, <<"no_group">>};
            _ ->
                {stop, normal, State, <<"error">>}
        end
    catch _What:Why ->
            lager:notice("transaction_handler got bad data: ~p", [Why]),
            {stop, normal, State, <<"error">>}
    end.
