%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction OUI ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_oui).

-export([
    new/3
    ,hash/1
    ,oui/1
    ,fee/1
    ,owner/1
    ,signature/1
    ,sign/2
    ,is_valid/1
    ,is/1
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(txn_oui, {
    oui :: binary()
    ,fee :: non_neg_integer()
    ,owner :: libp2p_crypto:address()
    ,signature :: binary()
}).

-type txn_oui() :: #txn_oui{}.
-type hash() :: <<_:256>>. %% SHA256 digest
-export_type([txn_oui/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(binary(), non_neg_integer(), libp2p_crypto:address()) -> txn_oui().
new(OUI, Fee, Owner) ->
    #txn_oui{
        oui=OUI
        ,fee=Fee
        ,owner=Owner
        ,signature= <<>>
    }.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec hash(txn_oui()) -> hash().
hash(Txn) ->
    crypto:hash(sha256, erlang:term_to_binary(remove_signature(Txn))).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec oui(txn_oui()) -> binary().
oui(Txn) ->
    Txn#txn_oui.oui.
%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec fee(txn_oui()) -> non_neg_integer().
fee(Txn) ->
    Txn#txn_oui.fee.
%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec owner(txn_oui()) -> libp2p_crypto:address().
owner(Txn) ->
    Txn#txn_oui.owner.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec signature(txn_oui()) -> binary().
signature(Txn) ->
    Txn#txn_oui.signature.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec sign(txn_oui(), libp2p_crypto:sig_fun()) -> txn_oui().
sign(Txn, SigFun) ->
    Txn#txn_oui{signature=SigFun(erlang:term_to_binary(Txn))}.


%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_valid(txn_oui()) -> boolean().
is_valid(Txn=#txn_oui{owner=Owner, signature=Signature}) ->
    PubKey = libp2p_crypto:address_to_pubkey(Owner),
    libp2p_crypto:verify(erlang:term_to_binary(Txn#txn_oui{signature = <<>>}), Signature, PubKey).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is(blockchain_transactions:transaction()) -> boolean().
is(Txn) ->
    erlang:is_record(Txn, txn_oui).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec remove_signature(txn_oui()) -> txn_oui().
remove_signature(Txn) ->
    Txn#txn_oui{signature = <<>>}.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Tx = #txn_oui{
        oui= <<"0">>
        ,fee=1
        ,owner= <<"owner">>
        ,signature= <<>>
    },
    ?assertEqual(Tx, new(<<"0">>, 1, <<"owner">>)).

oui_test() ->
    Tx = new(<<"0">>, 1, <<"owner">>),
    ?assertEqual(<<"0">>, oui(Tx)).

fee_test() ->
    Tx = new(<<"0">>, 1, <<"owner">>),
    ?assertEqual(1, fee(Tx)).

owner_test() ->
    Tx = new(<<"0">>, 1, <<"owner">>),
    ?assertEqual(<<"owner">>, owner(Tx)).

signature_test() ->
    Tx = new(<<"0">>, 1, <<"owner">>),
    ?assertEqual(<<>>, signature(Tx)).

sign_test() ->
    {PrivKey, PubKey} = libp2p_crypto:generate_keys(),
    Tx0 = new(<<"0">>, 1, <<"owner">>),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign(Tx0, SigFun),
    Sig1 = signature(Tx1),
    ?assert(libp2p_crypto:verify(erlang:term_to_binary(Tx1#txn_oui{signature = <<>>}), Sig1, PubKey)).

is_valid_test() ->
    {PrivKey, PubKey} = libp2p_crypto:generate_keys(),
    Owner1 = libp2p_crypto:pubkey_to_address(PubKey),
    Tx0 = new(<<"0">>, 1, Owner1),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign(Tx0, SigFun),
    ?assert(is_valid(Tx1)),
    {_, PubKey2} = libp2p_crypto:generate_keys(),
    Owner2 = libp2p_crypto:pubkey_to_address(PubKey2),
    Tx2 = new(<<"0">>, 1, Owner2),
    Tx3 = sign(Tx2, SigFun),
    ?assertNot(is_valid(Tx3)).

is_test() ->
    Tx0 = new(<<"0">>, 1, <<"owner">>),
    ?assert(is(Tx0)).

-endif.
