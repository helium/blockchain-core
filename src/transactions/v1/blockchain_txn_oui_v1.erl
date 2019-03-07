%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction OUI ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_oui_v1).

-behavior(blockchain_txn).

-include("pb/blockchain_txn_oui_v1_pb.hrl").

-export([
    new/4,
    hash/1,
    oui/1,
    fee/1,
    owner/1,
    routing/1,
    signature/1,
    sign/2,
    is_valid/3,
    absorb/3
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type txn_oui() :: #blockchain_txn_oui_v1_pb{}.
-export_type([txn_oui/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(binary(), non_neg_integer(), libp2p_crypto:pubkey_bin(), string()) -> txn_oui().
new(OUI, Fee, Owner, Routing) ->
    #blockchain_txn_oui_v1_pb{
       oui=OUI,
       fee=Fee,
       owner=Owner,
       routing=Routing,
       signature= <<>>
      }.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec hash(txn_oui()) -> blockchain_txn:hash().
hash(Txn) ->
    BaseTxn = Txn#blockchain_txn_oui_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_oui_v1_pb:encode_msg(BaseTxn),
    crypto:hash(sha256, EncodedTxn).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec oui(txn_oui()) -> binary().
oui(Txn) ->
    Txn#blockchain_txn_oui_v1_pb.oui.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec fee(txn_oui()) -> non_neg_integer().
fee(Txn) ->
    Txn#blockchain_txn_oui_v1_pb.fee.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec owner(txn_oui()) -> libp2p_crypto:pubkey_bin().
owner(Txn) ->
    Txn#blockchain_txn_oui_v1_pb.owner.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec routing(txn_oui()) -> string().
routing(Txn) ->
    Txn#blockchain_txn_oui_v1_pb.routing.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec signature(txn_oui()) -> binary().
signature(Txn) ->
    Txn#blockchain_txn_oui_v1_pb.signature.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec sign(txn_oui(), libp2p_crypto:sig_fun()) -> txn_oui().
sign(Txn, SigFun) ->
    EncodedTxn = blockchain_txn_oui_v1_pb:encode_msg(Txn),
    Txn#blockchain_txn_oui_v1_pb{signature=SigFun(EncodedTxn)}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_valid(txn_oui(),
               blockchain_block:block(),
               blockchain_ledger_v1:ledger()) ->ok | {error, any()}.
is_valid(Txn, _Block, Ledger) ->
    Owner = ?MODULE:owner(Txn),
    Signature = ?MODULE:signature(Txn),
    PubKey = libp2p_crypto:bin_to_pubkey(Owner),
    BaseTxn = Txn#blockchain_txn_oui_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_oui_v1_pb:encode_msg(BaseTxn),
    case libp2p_crypto:verify(EncodedTxn, Signature, PubKey) of
        false ->
            {error, bad_signature};
        true ->
            Routing = ?MODULE:routing(Txn),
            case validate_routing(Routing) of
                false ->
                    {error, invalid_routing};
                true ->
                    Fee = ?MODULE:fee(Txn),
                    Owner = ?MODULE:owner(Txn),
                    blockchain_ledger_v1:check_balance(Owner, Fee, Ledger)
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb(txn_oui(),
             blockchain_block:block(),
             blockchain_ledger_v1:ledger()) -> ok | {error, any()}.
absorb(Txn, _Block, Ledger) ->
    Fee = ?MODULE:fee(Txn),
    Owner = ?MODULE:owner(Txn),
    blockchain_ledger_v1:debit_fee(Owner, Fee, Ledger).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

-spec validate_routing(string()) -> boolean().
validate_routing("") ->
    true;
validate_routing(Routing) ->
    Infos = string:split(Routing, ",", all),
    case erlang:length(Infos) of
        L when L =< 3 ->
            lists:all(fun is_p2p/1, Infos);
        _ ->
            false
    end.

-spec is_p2p(string()) -> boolean().
is_p2p(Address) ->
    case catch multiaddr:protocols(Address) of
        [{"p2p", _}] -> true;
        _ -> false
    end.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Tx = #blockchain_txn_oui_v1_pb{
        oui= <<"0">>,
        fee=1,
        owner= <<"owner">>,
        routing = "/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ",
        signature= <<>>
    },
    ?assertEqual(Tx, new(<<"0">>, 1, <<"owner">>, "/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ")).

oui_test() ->
    Tx = new(<<"0">>, 1, <<"owner">>, "/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ"),
    ?assertEqual(<<"0">>, oui(Tx)).

fee_test() ->
    Tx = new(<<"0">>, 1, <<"owner">>, "/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ"),
    ?assertEqual(1, fee(Tx)).

owner_test() ->
    Tx = new(<<"0">>, 1, <<"owner">>, "/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ"),
    ?assertEqual(<<"owner">>, owner(Tx)).

routing_test() ->
    Tx = new(<<"0">>, 1, <<"owner">>, "/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ"),
    ?assertEqual("/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ", routing(Tx)).

signature_test() ->
    Tx = new(<<"0">>, 1, <<"owner">>, "/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ"),
    ?assertEqual(<<>>, signature(Tx)).

sign_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Tx0 = new(<<"0">>, 1, <<"owner">>, "/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ"),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign(Tx0, SigFun),
    Sig1 = signature(Tx1),
    EncodedTx1 = blockchain_txn_oui_v1_pb:encode_msg(Tx1#blockchain_txn_oui_v1_pb{signature = <<>>}),
    ?assert(libp2p_crypto:verify(EncodedTx1, Sig1, PubKey)).

validate_routing_test() ->
    ?assert(validate_routing("")),
    ?assertNot(validate_routing(",")),
    ?assert(validate_routing("/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ")),
    ?assert(validate_routing("/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ,/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ")),
    ?assert(validate_routing("/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ,/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ,/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ")),
    ?assertNot(validate_routing("http://test.com")),
    ?assertNot(validate_routing("/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ,http://test.com")),
    ?assertNot(validate_routing("/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ,/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ,http://test.com")),
    ok.

-endif.
