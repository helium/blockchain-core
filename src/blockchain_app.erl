%%%-------------------------------------------------------------------
%% @doc blockchain public API
%% @end
%%%-------------------------------------------------------------------

-module(blockchain_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%%====================================================================
%% API
%%====================================================================

start(_StartType, _StartArgs) ->
    Key =
        case application:get_env(blockchain, key) of
            undefined ->
                #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
                {PubKey, libp2p_crypto:mk_sig_fun(PrivKey), libp2p_crypto:mk_ecdh_fun(PrivKey)};
            {ok, K} ->
                K
        end,
    BaseDir = application:get_env(blockchain, base_dir, "data"),
    Port = application:get_env(blockchain, port, 0),
    SeedNodes = application:get_env(blockchain, seed_nodes, []),
    SeedNodeDNS = application:get_env(blockchain, seed_node_dns, []),
    % look up the DNS record and add any resulting addresses to the SeedNodes
    % no need to do any checks here as any bad combination results in an empty list
    SeedAddresses = string:tokens(lists:flatten([string:prefix(X, "blockchain-seed-nodes=") || [X] <- inet_res:lookup(SeedNodeDNS, in, txt), string:prefix(X, "blockchain-seed-nodes=") /= nomatch]), ","),
    Args = [
        {base_dir, BaseDir},
        {seed_nodes, SeedNodes ++ SeedAddresses},
        {key, Key},
        {port, Port}
    ],

    case blockchain_sup:start_link(Args) of
        {ok, Pid} ->
            blockchain_cli_registry:register_cli(),
            {ok, Pid};
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
stop(_State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================
