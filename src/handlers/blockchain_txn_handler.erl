%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Stream Handler ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_handler).

-behavior(libp2p_framed_stream).

-include("blockchain.hrl").

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
    txn_hash :: undefined | blockchain_txn:hash(),
    path :: string() % a.k.a. protocol version
}).

client(Connection, Args) ->
    libp2p_framed_stream:client(?MODULE, Connection, Args).

server(Connection, Path, _TID, Args) ->
    %% NOTE: server/4 in the handler is never called.
    %% When spawning a server its handled only in libp2p_framed_stream
    %% TODO If never called, as per above, why do we need server/4?
    lager:warning("server with ~p", [Path]),
    libp2p_framed_stream:server(?MODULE, Connection, [Path | Args]).

%% ------------------------------------------------------------------
%% libp2p_framed_stream Function Definitions
%% ------------------------------------------------------------------
init(client, _Conn, [Path, Parent, TxnHash]) ->
    {ok, #state{parent=Parent, txn_hash=TxnHash, path=Path}};
init(server, _Conn, [_, Path, _, Callback] = _Args) ->
    {ok, #state{callback = Callback, path=Path}}.

handle_data(client, <<"ok">>, State=#state{parent=Parent, txn_hash=TxnHash}) ->
    Parent ! {blockchain_txn_response, {ok, TxnHash}},
    {stop, normal, State};
handle_data(client, <<"no_group">>, State=#state{parent=Parent, txn_hash=TxnHash}) ->
    Parent ! {blockchain_txn_response, {no_group, TxnHash}},
    {stop, normal, State};
handle_data(
    client,
    <<"error", _/binary>> = ErrorMsg,
    State=#state{
        path = Path,
        parent = Parent,
        txn_hash = TxnHash
    }
) ->
    TxnData = error_msg_to_txn_data(Path, ErrorMsg, TxnHash),
    Parent ! {blockchain_txn_response, {error, TxnData}},
    {stop, normal, State};
handle_data(server, Data, State=#state{path=Path, callback = Callback}) ->
    try
        Txn = blockchain_txn:deserialize(Data),
        lager:debug("Got ~p type transaction: ~s", [blockchain_txn:type(Txn), blockchain_txn:print(Txn)]),
        case Callback(Txn) of
            {ok, _Height} ->
                {stop, normal, State, <<"ok">>};
            {{error, no_group}, _Height} ->
                {stop, normal, State, <<"no_group">>};
            {{error, _}, Height} ->
                {stop, normal, State, error_msg(Path, Height)}
        end
    catch _What:Why ->
            lager:notice("transaction_handler got bad data: ~p", [Why]),
            {stop, normal, State, error_msg(Path, 0)}
    end.

-spec error_msg(string(), non_neg_integer()) -> binary().
error_msg(?TX_PROTOCOL_V1, _) ->
    <<"error">>;
error_msg(?TX_PROTOCOL_V2, Height) ->
    <<"error", Height/integer>>.

-spec error_msg_to_txn_data(string(), binary(), binary()) ->
    binary() | {binary(), non_neg_integer()}.
error_msg_to_txn_data(?TX_PROTOCOL_V1, <<"error">>, TxnHash) ->
    TxnHash;
error_msg_to_txn_data(?TX_PROTOCOL_V2, <<"error", Height/integer>>, TxnHash) ->
    {TxnHash, Height}.
