%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Sybc Stream Handler ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_snapshot_handler).

-behavior(libp2p_framed_stream).


-include_lib("kernel/include/file.hrl").
-include_lib("helium_proto/include/blockchain_snapshot_handler_pb.hrl").

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
    handle_data/3,
    handle_info/3
]).

-record(state,
        {
         chain :: blockchain:blochain(),
         hash :: any(),
         owner = undefined :: undefined | pid()
        }).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
client(Connection, Args) ->
    libp2p_framed_stream:client(?MODULE, Connection, Args).

server(Connection, Path, _TID, Args) ->
    libp2p_framed_stream:server(?MODULE, Connection, [Path | Args]).

%% ------------------------------------------------------------------
%% libp2p_framed_stream Function Definitions
%% ------------------------------------------------------------------
init(client, _Conn, [Hash, Height, Chain]) ->
    case blockchain_worker:sync_paused() of
        true ->
            {stop, normal};
        false ->
            Msg = #blockchain_snapshot_req_pb{height = Height, hash = Hash},
            {ok, #state{chain = Chain, hash = Hash},
             blockchain_snapshot_handler_pb:encode_msg(Msg)}
    end;
init(client, _Conn, [Hash, Height, Chain, Owner]) ->
    Msg = #blockchain_snapshot_req_pb{height = Height, hash = Hash},
    {ok, #state{chain = Chain, hash = Hash, owner = Owner},
     blockchain_snapshot_handler_pb:encode_msg(Msg)};
init(server, _Conn, [_Path, _, Chain]) ->
    {ok, #state{chain = Chain}}.

handle_data(client, Data, #state{chain = Chain, hash = Hash, owner = undefined} = State) ->
    #blockchain_snapshot_resp_pb{snapshot = BinSnap} =
        blockchain_snapshot_handler_pb:decode_msg(Data, blockchain_snapshot_resp_pb),
    case blockchain_ledger_snapshot_v1:deserialize(Hash, BinSnap) of
        {ok, Snapshot} ->
            Height = blockchain_ledger_snapshot_v1:height(Snapshot),

            case blockchain:have_snapshot(Height, Chain) orelse
                ok == blockchain:add_bin_snapshot(BinSnap, Height, Hash, Chain) of
                true ->
                    lager:info("retrieved and stored snapshot ~p, installing",
                               [Height]),
                    blockchain_worker:install_snapshot(Height, Hash, Snapshot, BinSnap);
                false ->
                    lager:info("could not install retrieved snapshot ~p", [Height]),
                    ok
            end;
        {error, Reason} ->
            lager:info("could not deserialize retrieved snapshot ~p: ~p",
                       [Reason]),
            ok
    end,
    {stop, normal, State};
handle_data(client, Data, #state{owner = Owner, hash = Hash} = State) ->
    #blockchain_snapshot_resp_pb{snapshot = BinSnap} =
        blockchain_snapshot_handler_pb:decode_msg(Data, blockchain_snapshot_resp_pb),
    case blockchain_ledger_snapshot_v1:deserialize(Hash, BinSnap) of
        {ok, Snapshot} ->
            Owner ! {ok, Snapshot};
        {error, _Reason} ->
            Owner ! {error, not_found}
    end,
    {stop, normal, State};
handle_data(server, Data, #state{chain = Chain} = State) ->
    case blockchain_snapshot_handler_pb:decode_msg(Data, blockchain_snapshot_req_pb) of
        #blockchain_snapshot_req_pb{height = _Height, hash = Hash} ->
            case blockchain:get_snapshot(Hash, Chain) of
                {ok, {file, FileName}} ->
                    lager:info("streaming snapshot from ~p", [FileName]),
                    {noreply, State, mk_file_stream_fun(FileName)};
                {ok, Snap} ->
                    lager:info("sending snapshot ~p", [Hash]),
                    Msg = #blockchain_snapshot_resp_pb{snapshot = Snap},
                    {noreply, State, blockchain_snapshot_handler_pb:encode_msg(Msg)};
                {error, _Reason} ->
                    lager:info("failed getting snapshot ~p : ~p", [Hash, _Reason]),
                    {stop, normal, State}
            end;
        _Other ->
            lager:info("unexpected snapshot message ~p", [_Other]),
            %% There was some sort of error, just die
            {stop, normal, State}
    end.

handle_info(_Type, _Msg, State) ->
    lager:info("unhandled message ~p ~p", [_Type, _Msg]),
    {noreply, State}.

%% ------------------------------------------------------------------
%% internal functions
%% ------------------------------------------------------------------

% convert integer to varint encoding for protocol buffer
varint(I, Bin) when I =< 127 -> <<Bin/binary, I>>;
varint(I, Bin) -> varint(I bsr 7, <<Bin/binary, (I band 127 bor 128)>>).


-define(FILE_STREAM_BLOCKSIZE, 4096).

% send file bytes in chunks
mk_file_stream_fun(File) when is_pid(File)->
    case file:read(File, ?FILE_STREAM_BLOCKSIZE) of
        {ok, Data} ->
            fun() ->
                    {mk_file_stream_fun(File), Data}
            end;
        eof ->
            file:close(File),
            fun() -> ok end
    end;
mk_file_stream_fun(FileName) ->
    {ok, #file_info{size = Bytes}} = file:read_file_info(FileName),
    {ok, File} = file:open(FileName, [read, binary]),
    PBSize = varint(Bytes, <<>>),
    %% 18 introduces a protocol buffers bytes type
    Msg0 = <<18, PBSize/binary>>,
    HdrSize = byte_size(Msg0),
    {ok, Data} = file:read(File, ?FILE_STREAM_BLOCKSIZE - HdrSize),
    Msg = <<Msg0/binary, Data/binary>>,
    {Bytes + HdrSize,
      fun() ->
                {mk_file_stream_fun(File), Msg}
      end
    }.
