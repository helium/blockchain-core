%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain CLI Peer ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_cli_peer).

-behavior(clique_handler).

-export([register_cli/0]).

register_cli() ->
    register_all_usage(),
    register_all_cmds().

register_all_usage() ->
    lists:foreach(fun(Args) ->
                          apply(clique, register_usage, Args)
                  end,
                 [
                  peer_listen_usage(),
                  peer_session_usage(),
                  peer_addr_usage(),
                  peer_connect_usage(),
                  peer_disconnect_usage(),
                  peer_ping_usage(),
                  peer_book_usage(),
                  peer_gossip_peers_usage(),
                  peer_gossip_peers_usage(),
                  peer_usage()
                 ]).

register_all_cmds() ->
    lists:foreach(fun(Cmds) ->
                          [apply(clique, register_command, Cmd) || Cmd <- Cmds]
                  end,
                 [
                  peer_listen_cmd(),
                  peer_session_cmd(),
                  peer_addr_cmd(),
                  peer_connect_cmd(),
                  peer_disconnect_cmd(),
                  peer_ping_cmd(),
                  peer_book_cmd(),
                  peer_gossip_peers_cmd(),
                  peer_gossip_peers_cmd(),
                  peer_cmd()
                 ]).
%%
%% peer
%%

peer_usage() ->
    [["peer"],
     ["blockchain peer commands\n\n",
      "  peer listen            - Display the addresses this node is listening on.\n",
      "  peer session           - Display the nodes this node is connected to.\n",
      "  peer ping              - Ping the peer over an established or new session.\n",
      "  peer connect           - Connnect this node to another node.\n",
      "  peer disconnect        - Disconnect from a connected peer.\n"
      "  peer addr              - Display the p2p address of this node.\n"
      "  peer book              - Display informatiom from the peerbook of this node.\n"
      "  peer gossip_peers      - Display gossip peers of this node.\n"
     ]
    ].

peer_cmd() ->
    [
     [["peer"], [], [], fun(_, _, _) -> usage end]
    ].

%%
%% peer listen_addr
%%

peer_session_cmd() ->
    [
     [["peer", "session"], [], [], fun peer_session/3]
    ].

peer_session_usage() ->
    [["peer", "session"],
     ["blockchain peer session\n\n",
      "  Display the peers this node is connected to.\n"
     ]
    ].

peer_session(_CmdBase, [], []) ->
    Swarm = blockchain_swarm:swarm(),
    [format_peer_sessions(Swarm)].

format_peer_sessions(Swarm) ->
    SessionInfos = libp2p_swarm:sessions(Swarm),
    R = lists:filtermap(fun({A, S}) ->
                                case multiaddr:protocols(A) of
                                    [{"p2p", B58}] -> {true, {A, libp2p_session:addr_info(S), B58}};
                                    _ -> false
                                end
                        end, SessionInfos),

    FormatEntry = fun({MA, {SockAddr, PeerAddr}, B58}) ->
                          AName = erl_angry_purple_tiger:animal_name(B58),
                          [
                           {"local", SockAddr},
                           {"remote", PeerAddr},
                           {"p2p", MA},
                           {"name", AName}
                          ]
                  end,
    clique_status:table(lists:map(FormatEntry, R)).


%%
%% peer listen
%%

peer_listen_cmd() ->
    [
     [["peer", "listen"], [], [], fun peer_listen/3]
    ].

peer_listen_usage() ->
    [["peer", "listen"],
     ["peer listen\n\n",
      "  Display the addresses this node listens on.\n"
     ]
    ].

peer_listen(_CmdBase, [], []) ->
    Swarm = blockchain_swarm:swarm(),
    ListenAddrs = libp2p_swarm:listen_addrs(Swarm),
    %% Format result
    [format_listen_addrs(Swarm, ListenAddrs)].

format_listen_addrs(Swarm, Addrs) ->
    SortedAddrs = libp2p_transport:sort_addrs(libp2p_swarm:tid(Swarm), Addrs),
    clique_status:table([[{"listen_addrs (prioritized)", A}] || A <- SortedAddrs]).


%%
%% peer addr
%%

peer_addr_cmd() ->
    [
     [["peer", "addr"], [], [], fun peer_addr/3]
    ].

peer_addr_usage() ->
    [["peer", "addr"],
     ["peer addr\n\n",
      "  Display the p2p addresses of this node.\n"
     ]
    ].

peer_addr(_CmdBase, [], []) ->
    Text = clique_status:text(libp2p_crypto:pubkey_bin_to_p2p(blockchain_swarm:pubkey_bin())),
    [Text].


%%
%% peer connect
%%

peer_connect_cmd() ->
    [
     [["peer", "connect", '*'], [], [], fun peer_connect/3]
    ].

peer_connect_usage() ->
    [["peer", "connect"],
     ["peer connect <p2p>\n\n",
      "  Connects to the node at the given <p2p> address.\n\n"
     ]
    ].

peer_connect(["peer", "connect", Addr], [], []) ->
    Swarm = blockchain_swarm:swarm(),
    TrimmedAddr = string:trim(Addr),
    case libp2p_swarm:connect(Swarm, TrimmedAddr) of
        {ok, _} ->
            Text = io_lib:format("Connected to ~p successfully~n", [TrimmedAddr]),
            [clique_status:text(Text)];
        {error, Reason} ->
            Text = io_lib:format("Failed to connect to ~p: ~p~n", [TrimmedAddr, Reason]),
            [clique_status:alert([clique_status:text(Text)])]
    end;
peer_connect([], [], []) ->
    usage.

%%
%% peer disconnect
%%

peer_disconnect_cmd() ->
    [
     [["peer", "disconnect", '*'], [], [], fun peer_disconnect/3]
    ].

peer_disconnect_usage() ->
    [["peer", "disconnect"],
     ["peer disconnect <Addr> \n\n",
      "  Disconnect this node from a given <p2p> addr.\n\n"
     ]
    ].

peer_disconnect(["peer", "disconnect", _Addr], [], []) ->
    %% TODO: unimplemented
    [clique_status:text("ok")];
peer_disconnect([], [], []) ->
    usage.

%%
%% peer ping
%%

peer_ping_cmd() ->
    [
     [["peer", "ping", '*'], [], [], fun peer_ping/3]
    ].

peer_ping_usage() ->
    [["peer", "ping"],
     ["peer ping <Addr> \n\n",
      "  Ping the node at the given <p2p> addr.\n\n"
     ]
    ].

peer_ping(["peer", "ping", Addr], [], []) ->
    Swarm = blockchain_swarm:swarm(),
    TrimmedAddr = string:trim(Addr),
    case libp2p_swarm:connect(Swarm, TrimmedAddr) of
        {ok, Session} ->
            case libp2p_session:ping(Session) of
                {ok, RTT} ->
                    Text = io_lib:format("Pinged ~p successfully with roundtrip time: ~p ms~n",
                                         [TrimmedAddr, RTT]),
                    [clique_status:text(Text)];
                {error, Reason} ->
                    Text = io_lib:format("Failed to ping ~p: ~p~n", [TrimmedAddr, Reason]),
                    [clique_status:alert([clique_status:text(Text)])]
            end;
        {error, Reason} ->
            Text = io_lib:format("Failed to connect to ~p: ~p~n", [TrimmedAddr, Reason]),
            [clique_status:alert([clique_status:text(Text)])]
    end;
peer_ping([], [], []) ->
    usage.

%%
%% peer peerbook
%%

peer_book_cmd() ->
    [
     [["peer", "book", '*'], [], [], fun peer_book/3],
     [["peer", "book"], [],
      [{self, [{shortname, "s"},
               {longname, "self"}]},
       {all, [{shortname, "a"},
               {longname, "all"}]}
      ], fun peer_book/3]
    ].

peer_book_usage() ->
    [["peer", "book"],
     ["peer book [<p2p> | -s | -a]\n\n",
      "  Displays peerbook entries for a given <p2p> address, with options\n"
      "  for display the entry for this node, or all entries.\n\n",
      "Options\n\n",
      "  -s, --self\n",
      "    Display the peerbook entry for this node.\n"
      "  -a, --all\n",
      "    Display all peerbook entries for this node.\n"
     ]
    ].

peer_book(["peer", "book", Addr], [], []) ->
    Swarm = blockchain_swarm:swarm(),
    PeerBook = libp2p_swarm:peerbook(Swarm),
    {ok, Peer} = libp2p_peerbook:get(PeerBook, libp2p_crypto:p2p_to_pubkey_bin(Addr)),
    [format_peers([Peer]),
     format_listen_addrs(Swarm, libp2p_peer:listen_addrs(Peer)),
     format_peer_connections(Peer)];
peer_book(_CmdBase, [], [{self, _}]) ->
    Swarm = blockchain_swarm:swarm(),
    PeerBook = libp2p_swarm:peerbook(Swarm),
    {ok, Peer} = libp2p_peerbook:get(PeerBook, blockchain_swarm:pubkey_bin()),
    [format_peers([Peer]),
     format_listen_addrs(Swarm, libp2p_peer:listen_addrs(Peer)),
     format_peer_sessions(Swarm)];
peer_book(_CmdBase, [], [{all, _}]) ->
    Swarm = blockchain_swarm:swarm(),
    Peerbook = libp2p_swarm:peerbook(Swarm),
    [format_peers(libp2p_peerbook:values(Peerbook))];
peer_book(_CmdBase, [], []) ->
    usage.

%%
%% peer gossip_peers
%%

peer_gossip_peers_cmd() ->
    [
     [["peer", "gossip_peers"], [], [], fun peer_gossip_peers/3]
    ].

peer_gossip_peers_usage() ->
    [["peer", "gossip_peers"],
     ["peer gossip_peers \n\n",
      "  Display gossip peers for this node.\n\n"
     ]
    ].

peer_gossip_peers(["peer", "gossip_peers"], [], []) ->
    %% TODO: tabularize this
    [clique_status:text(io_lib:format("~p", [blockchain_swarm:gossip_peers()]))];
peer_gossip_peers([], [], []) ->
    usage.


%%
%% internal functions
%%

format_peers(Peers) ->
    FormatPeer =
        fun(Peer) ->
                ListenAddrs = libp2p_peer:listen_addrs(Peer),
                ConnectedTo = libp2p_peer:connected_peers(Peer),
                NatType = libp2p_peer:nat_type(Peer),
                Timestamp = libp2p_peer:timestamp(Peer),
                Bin = libp2p_peer:pubkey_bin(Peer),
                [{address, libp2p_crypto:pubkey_bin_to_p2p(Bin)},
                 {name, erl_angry_purple_tiger:animal_name(libp2p_crypto:bin_to_b58(Bin))},
                 {listen_addrs, io_lib:format("~p", [length(ListenAddrs)])},
                 {connections, io_lib:format("~p", [length(ConnectedTo)])},
                 {nat, io_lib:format("~s", [NatType])},
                 {last_updated, io_lib:format("~ps", [(erlang:system_time(millisecond) - Timestamp) / 1000])}
                ]
        end,
    clique_status:table(lists:map(FormatPeer, Peers)).

format_peer_connections(Peer) ->
    Connections = [[{connections, libp2p_crypto:pubkey_bin_to_p2p(P)}]
                   || P <- libp2p_peer:connected_peers(Peer)],
    clique_status:table(Connections).
