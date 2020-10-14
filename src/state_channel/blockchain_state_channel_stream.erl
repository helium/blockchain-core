%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel Stream Handler ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_state_channel_stream).

-behavior(libp2p_stream).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([
    dial/3,
    send_packet/2,
    send_offer/2,
    send_purchase/2,
    send_response/2,
    send_banner/2,
    send_rejection/2,
    stop/1
]).

%% ------------------------------------------------------------------
%% libp2p_stream Function Exports
%% ------------------------------------------------------------------
-export([
    init/2,
    handle_packet/4,
    handle_info/3,
    protocol_id/0,
    handler/1
]).

-include("blockchain.hrl").
-include("blockchain_vars.hrl").

-record(state, {
    mod :: atom(),
    mod_opts :: map()
}).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
dial(_Swarm, Peer, #{ed25519_keypair := ClientKeyPair} = _Opts) ->
    %% TODO - add a general purpose helper function to build the handler list
    libp2p_stream_tcp:start_link(client, #{
        addr => Peer,
        handlers => [
            libp2p_stream_plaintext:handler(#{
                public_key => libp2p_keypair:public_key(ClientKeyPair),
                handlers => [
                    libp2p_stream_mplex:handler(#{
                        handlers => [?MODULE:handler(#{})]})
                ]
            })
        ]
    }).

-spec send_packet(pid(), blockchain_state_channel_packet_v1:packet()) -> ok.
send_packet(Pid, Packet) ->
    Pid ! {send_packet, Packet},
    ok.

-spec send_offer(pid(), blockchain_state_channel_packet_offer_v1:offer()) -> ok.
send_offer(Pid, Offer) ->
    lager:info("sending offer: ~p, pid: ~p", [Offer, Pid]),
    Pid ! {send_offer, Offer},
    ok.

-spec send_purchase(pid(), blockchain_state_channel_purchase_v1:purchase()) -> ok.
send_purchase(Pid, Purchase) ->
    lager:info("sending purchase: ~p, pid: ~p", [Purchase, Pid]),
    Pid ! {send_purchase, Purchase},
    ok.

-spec send_banner(pid(), blockchain_state_channel_banner_v1:banner()) -> ok.
send_banner(Pid, Banner) ->
    lager:info("sending banner: ~p, pid: ~p", [Banner, Pid]),
    Pid ! {send_banner, Banner},
    ok.

-spec send_rejection(pid(), blockchain_state_channel_rejection_v1:rejection()) -> ok.
send_rejection(Pid, Rejection) ->
    lager:info("sending rejection: ~p, pid: ~p", [Rejection, Pid]),
    Pid ! {send_rejection, Rejection},
    ok.

-spec send_response(pid(), blockchain_state_channel_response_v1:response()) -> ok.
send_response(Pid, Resp) ->
    Pid ! {send_response, Resp},
    ok.

-spec stop(pid()) -> ok.
stop(Pid) ->
    Pid ! stop,
    ok.


%% ------------------------------------------------------------------
%% libp2p_stream Function Definitions
%% ------------------------------------------------------------------

handler(Opts) ->
    {protocol_id(), {?MODULE, Opts}}.

protocol_id() ->
    <<?STATE_CHANNEL_PROTOCOL_V1>>.

init(Kind, Opts = #{handler_fn := HandlerFun}) ->
    init(Kind, maps:remove(handler_fn, Opts#{handlers => HandlerFun()}));
init(Kind, Opts = #{handlers := Handlers}) ->
    ModOpts = maps:get(mod_opts, Opts, #{}),
    NewOpts = Opts#{
        mod => libp2p_stream_multistream,
        mod_opts => maps:merge(ModOpts, #{handlers => Handlers})
    },
    init(Kind, maps:remove(handlers, NewOpts));

init(client = Kind, #{mod := Mod, mod_opts := ModOpts}) ->
    libp2p_stream_md:update({stack, {Mod, Kind}}),
    {ok, #state{mod = Mod, mod_opts = ModOpts}, [
        {active, once},
        {packet_spec, [varint]}
    ]};
init(server = Kind, #{mod := Mod, mod_opts := ModOpts}) ->
    libp2p_stream_md:update({stack, {Mod, Kind}}),
    %% TODO - see if the chain can be passed in via init options
    Blockchain = blockchain_worker:blockchain(),
    Ledger = blockchain:ledger(Blockchain),
    case blockchain:config(?sc_version, Ledger) of
        {ok, N} when N > 1 ->
            case blockchain_state_channels_server:active_sc() of
                undefined ->
                    %% Send empty banner
                    SCBanner = blockchain_state_channel_banner_v1:new(),
                    lager:info("sc_stream, empty banner: ~p", [SCBanner]),
                    %lager:info("sc_stream, banner: ~p", [SCBanner]),
                    {ok, #state{mod = Mod, mod_opts = ModOpts}, [
                        {active, once},
                        {packet_spec, [varint]},
                        [{send, blockchain_state_channel_message_v1:encode(SCBanner)}]
                    ]};
                ActiveSC ->
                    %lager:info("sc_stream, active_sc: ~p", [ActiveSC]),
                    SCBanner = blockchain_state_channel_banner_v1:new(ActiveSC),
                    %lager:info("sc_stream, banner: ~p", [SCBanner]),
                    {ok, #state{mod = Mod, mod_opts = ModOpts}, [
                        {active, once},
                        {packet_spec, [varint]},
                        [{send, blockchain_state_channel_message_v1:encode(SCBanner)}]
                    ]}
            end;
        _ ->
            {ok, #state{mod = Mod, mod_opts = ModOpts}, [
                {active, once},
                {packet_spec, [varint]}
            ]}
    end.

handle_packet(client, _, Data, State) ->
    case blockchain_state_channel_message_v1:decode(Data) of
        {banner, Banner} ->
            lager:info("sc_stream client got banner: ~p", [Banner]),
            blockchain_state_channels_client:banner(Banner, self());
        {purchase, Purchase} ->
            lager:info("sc_stream client got purchase: ~p", [Purchase]),
            blockchain_state_channels_client:purchase(Purchase, self());
        {reject, Rejection} ->
            lager:info("sc_stream client got rejection: ~p", [Rejection]),
            blockchain_state_channels_client:reject(Rejection, self());
        {response, Resp} ->
            lager:debug("sc_stream client got response: ~p", [Resp]),
            blockchain_state_channels_client:response(Resp)
    end,
    {noreply, State, [{active, once}]};
handle_packet(server, _, Data, State) ->
    case blockchain_state_channel_message_v1:decode(Data) of
        {offer, Offer} ->
            lager:info("sc_stream server got offer: ~p", [Offer]),
            blockchain_state_channels_server:offer(Offer, self());
        {packet, Packet} ->
            lager:debug("sc_stream server got packet: ~p", [Packet]),
            blockchain_state_channels_server:packet(Packet, self())
    end,
    {noreply, State, [{active, once}]}.

handle_info(client, {send_offer, Offer}, State) ->
    Data = blockchain_state_channel_message_v1:encode(Offer),
    {noreply, State, [{send, Data}]};
handle_info(client, {send_packet, Packet}, State) ->
    lager:debug("sc_stream client sending packet: ~p", [Packet]),
    Data = blockchain_state_channel_message_v1:encode(Packet),
    {noreply, State, [{send, Data}]};
handle_info(client, stop, State) ->
    lager:debug("sc_stream server stopping...", []),
    {stop, State, []};
handle_info(server, {send_banner, Banner}, State) ->
    Data = blockchain_state_channel_message_v1:encode(Banner),
    {noreply, State, [{send, Data}]};
handle_info(server, {send_rejection, Rejection}, State) ->
    Data = blockchain_state_channel_message_v1:encode(Rejection),
    {noreply, State, [{send, Data}]};
handle_info(server, {send_purchase, Purchase}, State) ->
    Data = blockchain_state_channel_message_v1:encode(Purchase),
    {noreply, State, [{send, Data}]};
handle_info(server, {send_response, Resp}, State) ->
    lager:debug("sc_stream server sending resp: ~p", [Resp]),
    Data = blockchain_state_channel_message_v1:encode(Resp),
    {noreply, State, [{send, Data}]};
handle_info(server, stop, State) ->
    lager:debug("sc_stream server stopping...", []),
    {stop, State, []};
handle_info(_Type, _Msg, State) ->
    lager:warning("~p got unhandled msg: ~p", [_Type, _Msg]),
    {noreply, State,[]}.
