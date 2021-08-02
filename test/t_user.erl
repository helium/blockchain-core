-module(t_user).

-export_type([
    t/0
]).

-export([
    new/0,
    n_new/1,
    addr/1,
    sig_fun/1,
    sign/2,       % access and apply sig_fun
    key_triple/1, % passed into blockchain app start options
    key_pair/1    % passed into ct config as {master_key, Pair}. TODO may not be needed
]).

-include_lib("eunit/include/eunit.hrl").

-record(?MODULE, {
    address  :: libp2p_crypto:pubkey_bin(),
    priv     :: libp2p_crypto:privkey(),
    pub      :: libp2p_crypto:pubkey(),
    sig_fun  :: libp2p_crypto:sig_fun(),
    ecdh_fun :: libp2p_crypto:ecdh_fun()
}).

-opaque t() ::
    #?MODULE{}.

-type key_type() ::
    %% TODO Export key_type/0 from libp2p_crypto
    ecc_compact | ed25519.

%% API ========================================================================
-spec n_new(pos_integer()) -> [t()].
n_new(N) ->
    [new() || _ <- lists:duplicate(N, {})].

-spec new() -> t().
new() ->
    new(ecc_compact).

-spec new(key_type()) -> t().
new(KeyType) ->
    #{public := Pub, secret := Priv} = libp2p_crypto:generate_keys(KeyType),
    Addr = libp2p_crypto:pubkey_to_bin(Pub),
    SigFun = libp2p_crypto:mk_sig_fun(Priv),
    ECDHFun = libp2p_crypto:mk_ecdh_fun(Priv),
    ?assert(is_binary(Addr)),
    ?assert(is_function(SigFun)),
    ?assert(is_function(ECDHFun)),
    #?MODULE{
        address  = Addr,
        priv     = Priv,
        pub      = Pub,
        ecdh_fun = ECDHFun,
        sig_fun  = SigFun
    }.

-spec key_triple(t()) ->
    {
        libp2p_crypto:pubkey(),
        libp2p_crypto:sig_fun(),
        libp2p_crypto:ecdh_fun()
    }.
key_triple(#?MODULE{pub=P, sig_fun=S, ecdh_fun=E}) ->
    {P, S, E}.

-spec key_pair(t()) ->
    {
        libp2p_crypto:privkey(),
        libp2p_crypto:pubkey()
    }.
key_pair(#?MODULE{priv=Priv, pub=Pub}) ->
    {Priv, Pub}.

-spec addr(t()) -> binary().
addr(#?MODULE{address = <<Addr/binary>>}) ->
    Addr.

-spec sig_fun(t()) -> libp2p_crypto:sig_fun().
sig_fun(#?MODULE{sig_fun = SigFun}) ->
    ?assert(is_function(SigFun)),
    SigFun.

-spec sign(binary(), t()) -> binary().
sign(<<Bin/binary>>, #?MODULE{sig_fun = SigFun}) ->
    ?assert(is_function(SigFun)),
    SigFun(Bin).

%% Private ====================================================================
