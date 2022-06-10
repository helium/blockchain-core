%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Stream Handler ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_handler).

-behavior(libp2p_framed_stream).

-include("blockchain.hrl").
-include_lib("helium_proto/include/blockchain_txn_handler_pb.hrl").

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([
    server/4,
    client/2,
    encode_request/4,
    decode_request/2
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

-spec encode_request(
    string(),
    blockchain_txn_mgr:txn_key(),
    blockchain_txn:txn(),
    blockchain_txn_mgr:txn_request_type()) -> binary().
encode_request(?TX_PROTOCOL_V1 = _Path, _TxnKey, Txn, _RequestType) ->
    blockchain_txn:serialize(Txn);
encode_request(?TX_PROTOCOL_V2 = _Path, _TxnKey, Txn, _RequestType) ->
    blockchain_txn:serialize(Txn);
encode_request(?TX_PROTOCOL_V3 = _Path, TxnKey, Txn, RequestType) ->
    Req = #blockchain_txn_request_v1_pb{
        type = RequestType,
        key = TxnKey,
        txn = Txn
    },
    blockchain_txn_handler_pb:encode_msg(Req).

-spec decode_request(
    string(),
    binary()) -> #blockchain_txn_request_v1_pb{}.
decode_request(?TX_PROTOCOL_V1 = _Path, Bin) ->
    Txn = blockchain_txn:deserialize(Bin),
    #blockchain_txn_request_v1_pb{type = submit, txn = Txn};
decode_request(?TX_PROTOCOL_V2 = _Path, Bin) ->
    Txn = blockchain_txn:deserialize(Bin),
    #blockchain_txn_request_v1_pb{type = submit, txn = Txn};
decode_request(?TX_PROTOCOL_V3 = _Path, Bin) ->
    blockchain_txn_handler_pb:decode_msg(blockchain_txn_request_v1_pb, Bin).

%% ------------------------------------------------------------------
%% libp2p_framed_stream Function Definitions
%% ------------------------------------------------------------------
init(client, _Conn, [Path, Parent, TxnHash]) ->
    {ok, #state{parent=Parent, txn_hash=TxnHash, path=Path}};
init(server, _Conn, [_, Path, _, Callback] = _Args) ->
    {ok, #state{callback = Callback, path=Path}}.

handle_data(client, ResponseBin, State=#state{path=Path, parent=Parent}) ->
    Response = decode_response(Path, ResponseBin),
    Parent ! {blockchain_txn_response, Response},
    {stop, normal, State};
handle_data(server, Data, State=#state{path=Path, callback = Callback}) ->
    try
        #blockchain_txn_request_v1_pb{type = ReqType, txn = Txn} = ?MODULE:decode_request(Path, Data),
        lager:debug("Got ~p type transaction: ~s", [blockchain_txn:type(Txn), blockchain_txn:print(Txn)]),
        case Callback(ReqType, Txn) of
            {{ok, QueuePos, QueueLen}, Height} ->
                case ReqType of
                    submit ->
                        {stop, normal, State,
                            encode_response(
                                Path,
                                txn_accepted,
                                undefined,
                                undefined,
                                QueuePos,
                                QueueLen,
                                Height)};
                    update ->
                        {stop, normal, State,
                            encode_response(
                                Path,
                                txn_updated,
                                undefined,
                                undefined,
                                QueuePos,
                                QueueLen,
                                Height)}
                end;
            {{error, no_group}, Height} ->
                {stop, normal, State,
                    encode_response(
                        Path,
                        txn_failed,
                        no_group,
                        undefined,
                        undefined,
                        undefined,
                        Height)};
            {{error, {Reason, _}}, Height} when is_atom(Reason)->
                {stop, normal, State,
                    encode_response(
                        Path,
                        txn_rejected,
                        Reason,
                        undefined,
                        undefined,
                        undefined,
                        Height)};
            {{error, Reason}, Height} when is_atom(Reason)->
                {stop, normal, State,
                    encode_response(
                        Path,
                        txn_rejected,
                        Reason,
                        undefined,
                        undefined,
                        undefined,
                        Height)}
        end
    catch _What:Why:Stack ->
        lager:notice("transaction_handler got bad data: ~p", [Why]),
        {stop, normal, State, encode_response(Path, txn_failed, Why, Stack, undefined, undefined, 0)}
    end.

%% marshall v1 response formats
-spec encode_response(
    Path :: string(),
    Status :: atom(),
    Details :: atom(),
    Trace :: term(),
    PosInQueue :: non_neg_integer(),
    QueueLen :: non_neg_integer(),
    Height :: pos_integer()
) -> term().
encode_response(?TX_PROTOCOL_V1, txn_accepted, _Details, _Trace, _PosInQueue, _QueueLen, _Height) ->
    <<"ok">>;
encode_response(?TX_PROTOCOL_V1, txn_failed, _Details = no_group, _Trace, _PosInQueue, _QueueLen, _Height) ->
    <<"no_group">>;
encode_response(?TX_PROTOCOL_V1, txn_failed, _Details, _Trace, _PosInQueue, _QueueLen, _Height) ->
    <<"error">>;
encode_response(?TX_PROTOCOL_V1, txn_rejected, _Details, _Trace, _PosInQueue, _QueueLen, _Height) ->
    <<"rejected">>;
%% marshall v2 response formats
encode_response(?TX_PROTOCOL_V2, txn_accepted, _Details, _Trace, _PosInQueue, _QueueLen, _Height)  ->
    <<"ok">>;
encode_response(?TX_PROTOCOL_V2, txn_failed, _Details = no_group, _Trace, _PosInQueue, _QueueLen, _Height)  ->
    <<"no_group">>;
encode_response(?TX_PROTOCOL_V2, txn_rejected, _Details, _Trace, _PosInQueue, _QueueLen, Height) ->
    <<"error", Height/integer>>;
%% marshall v3 response format
encode_response(?TX_PROTOCOL_V3, Resp, Details, Trace, QueuePos, QueueLen, Height)  ->
    Msg = #blockchain_txn_info_v1_pb{
        result = atom_to_binary(Resp, utf8),
        details = atom_to_binary(Details, utf8),
        trace = term_to_binary(Trace),
        height = Height,
        queue_pos = QueuePos,
        queue_len = QueueLen
    },
    blockchain_txn_handler_pb:encode_msg(Msg).

%% decode responses to V3 format
%% v1 -> v3
decode_response(?TX_PROTOCOL_V1, <<"ok">>) ->
    v1_to_v3(txn_accepted, undefined);
decode_response(?TX_PROTOCOL_V1, <<"no_group">>) ->
    v1_to_v3(txn_failed, <<"no_group">>);
decode_response(?TX_PROTOCOL_V1, <<"error">>) ->
    v1_to_v3(txn_rejected, <<"error">>);
%% v2 -> v3
decode_response(?TX_PROTOCOL_V2, <<"ok">>) ->
    v2_to_v3(txn_accepted, undefined, 0);
decode_response(?TX_PROTOCOL_V2, <<"no_group">>) ->
    v2_to_v3(txn_failed, <<"no_group">>, 0);
decode_response(?TX_PROTOCOL_V2, <<"error", Height/integer>>) ->
    v2_to_v3(txn_rejected, <<"error">>, Height);
%% v3 -> v3
decode_response(?TX_PROTOCOL_V3, Resp) when is_binary(Resp) ->
    decode_response(?TX_PROTOCOL_V3, blockchain_txn_handler_pb:decode_msg(Resp)).
%%decode_response(?TX_PROTOCOL_V3,
%%    #blockchain_txn_info_v1_pb{result = <<"txn_accepted">>, height=Height,
%%        queue_pos=QueuePos, queue_len=QueueLen}) ->
%%    {txn_accepted, {Height, QueuePos, QueueLen}};
%%decode_response(?TX_PROTOCOL_V3,
%%    #blockchain_txn_info_v1_pb{result = <<"txn_updated">>, height=Height,
%%        queue_pos=QueuePos, queue_len=QueueLen}) ->
%%    {txn_updated, {Height, QueuePos, QueueLen}};
%%decode_response(?TX_PROTOCOL_V3,
%%    #blockchain_txn_info_v1_pb{result = <<"txn_failed">>, details=FailReason}) ->
%%    {txn_failed, FailReason};
%%decode_response(?TX_PROTOCOL_V3,
%%    #blockchain_txn_info_v1_pb{result = <<"txn_rejected">>, details=RejectReason, height=Height}) ->
%%    {txn_rejected, {Height, RejectReason}}.

v1_to_v3(Status, Details) when is_atom(Status)->
    #blockchain_txn_info_v1_pb{
        result = atom_to_binary(Status, utf8),
        details = Details,
        height = 0,
        queue_pos = 0,
        queue_len = 0
    }.

v2_to_v3(Status, Details, Height) when is_atom(Status)->
    #blockchain_txn_info_v1_pb{
        result = atom_to_binary(Status, utf8),
        details = Details,
        height = Height,
        queue_pos = 0,
        queue_len = 0
    }.

