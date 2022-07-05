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
    send_request/5
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

send_request(Stream, ProtocolVersion, RequestType, TxnKey, Txn) ->
    case encode_request(ProtocolVersion, TxnKey, Txn, RequestType) of
        {error, req_not_supported} = Error ->
            lager:info("req_not_supported for type ~p and txn ~p", [RequestType, Txn]),
            Error;
        EncodedMsg ->
            libp2p_framed_stream:send(Stream, EncodedMsg)
    end.

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
        #blockchain_txn_request_v1_pb{type = ReqType, txn = Txn} = Req = decode_request(Path, Data),
        lager:debug("Got request ~p, transaction: ~s", [Req, blockchain_txn:print(Txn)]),
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
            %% if a submitted txn is already in buffer
            %% then ignore, dont send a response
            %% if a response is returned it will result
            %% in a duplicate acception in txn mgr
            {already_queued, _Height} ->
                {stop, normal, State};
            %% handle errors whereby we want to have txn retry dialing
            %% these should be errors which are not directly related
            %% to validations, such as unexpected errors or crashes
            %% for such we return txn_failed
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
            {{error, no_chain}, Height} ->
                {stop, normal, State,
                    encode_response(
                        Path,
                        txn_failed,
                        no_chain,
                        undefined,
                        undefined,
                        undefined,
                        Height)};
            {{error, {validation_failed, {FailReasonWhy, FailReasonStack}}}, Height} ->
                {stop, normal, State,
                    encode_response(
                        Path,
                        txn_failed,
                        format_crash_reason(FailReasonWhy),
                        FailReasonStack,
                        undefined,
                        undefined,
                        Height)};
            %% handle errors whereby we want to have the txn mgr
            %% treat as an explicit rejection of the submitted txn
            %% such as errors during validations
            %% these count towards the reject count after which
            %% it will reject the txn after the limit is breached
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
        lager:warning("transaction_handler got bad data: ~p, stack: ~p", [Why, Stack]),
        {stop, normal, State,
            encode_response(
                Path,
                txn_failed,
                format_crash_reason(Why),
                Stack,
                undefined,
                undefined,
                0)}
    end.

%% ------------------------------------------------------------------
%% internal functions
%% ------------------------------------------------------------------
-spec encode_request(
    string(),
    blockchain_txn_mgr:txn_key(),
    blockchain_txn:txn(),
    blockchain_txn_mgr:txn_request_type()) -> binary() | {error, req_not_supported}.
encode_request(?TX_PROTOCOL_V1 = _Path, _TxnKey, Txn, submit) ->
    blockchain_txn:serialize(Txn);
encode_request(?TX_PROTOCOL_V2 = _Path, _TxnKey, Txn, submit) ->
    blockchain_txn:serialize(Txn);
encode_request(?TX_PROTOCOL_V3 = _Path, TxnKey, Txn, RequestType)
    when RequestType == submit;
         RequestType == update ->
    Req = #blockchain_txn_request_v1_pb{
        type = RequestType,
        key = TxnKey,
        txn = blockchain_txn:wrap_txn(Txn)
    },
    blockchain_txn_handler_pb:encode_msg(Req);
encode_request(_Path, _TxnKey, _Txn, _RequestType) ->
    {error, req_not_supported}.

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
    #blockchain_txn_request_v1_pb{txn = WrappedTxn} =
        Msg = blockchain_txn_handler_pb:decode_msg(Bin, blockchain_txn_request_v1_pb),
    Msg#blockchain_txn_request_v1_pb{txn = blockchain_txn:unwrap_txn(WrappedTxn)}.

%% marshall v1 response formats
-spec encode_response(
    Path :: string(),
    Status :: atom(),
    Details :: atom(),
    Trace :: term(),
    PosInQueue :: non_neg_integer() | undefined,
    QueueLen :: non_neg_integer() | undefined,
    Height :: non_neg_integer()
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
encode_response(?TX_PROTOCOL_V2, txn_failed, Details, _Trace, _PosInQueue, _QueueLen, _Height)  ->
    Details;
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
        queue_len = QueueLen,
        txn_protocol_version = 3
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
decode_response(?TX_PROTOCOL_V1, <<"rejected">>) ->
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
    blockchain_txn_handler_pb:decode_msg(Resp, blockchain_txn_info_v1_pb).

v1_to_v3(Status, Details) when is_atom(Status)->
    #blockchain_txn_info_v1_pb{
        result = atom_to_binary(Status, utf8),
        details = Details,
        height = 0,
        queue_pos = 0,
        queue_len = 0,
        txn_protocol_version = 1
    }.

v2_to_v3(Status, Details, Height) when is_atom(Status)->
    #blockchain_txn_info_v1_pb{
        result = atom_to_binary(Status, utf8),
        details = Details,
        height = Height,
        queue_pos = 0,
        queue_len = 0,
        txn_protocol_version = 2
    }.

format_crash_reason(R) when is_atom(R) ->
    R;
format_crash_reason({R, _}) ->
    R.
