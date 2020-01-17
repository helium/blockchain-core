%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Routing ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_routing_v1).

-behavior(blockchain_txn).

-include("../../pb/blockchain_txn_routing_v1_pb.hrl").
-include("blockchain_utils.hrl").

-export([
    new/5,
    hash/1,
    oui/1,
    owner/1,
    addresses/1,
    fee/1,
    nonce/1,
    signature/1,
    sign/2,
    is_valid/2,
    absorb/2,
    print/1
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type txn_routing() :: #blockchain_txn_routing_v1_pb{}.
-export_type([txn_routing/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(non_neg_integer(), libp2p_crypto:pubkey_bin(), [binary()], non_neg_integer(), non_neg_integer()) -> txn_routing().
new(OUI, Owner, Addresses, Fee, Nonce) ->
    #blockchain_txn_routing_v1_pb{
       oui=OUI,
       owner=Owner,
       addresses=Addresses,
       fee=Fee,
       nonce=Nonce,
       signature= <<>>
      }.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec hash(txn_routing()) -> blockchain_txn:hash().
hash(Txn) ->
    BaseTxn = Txn#blockchain_txn_routing_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_routing_v1_pb:encode_msg(BaseTxn),
    crypto:hash(sha256, EncodedTxn).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec oui(txn_routing()) -> non_neg_integer().
oui(Txn) ->
    Txn#blockchain_txn_routing_v1_pb.oui.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec owner(txn_routing()) -> libp2p_crypto:pubkey_bin().
owner(Txn) ->
    Txn#blockchain_txn_routing_v1_pb.owner.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec addresses(txn_routing()) -> [binary()].
addresses(Txn) ->
    Txn#blockchain_txn_routing_v1_pb.addresses.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec fee(txn_routing()) -> non_neg_integer().
fee(Txn) ->
    Txn#blockchain_txn_routing_v1_pb.fee.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec nonce(txn_routing()) -> non_neg_integer().
nonce(Txn) ->
    Txn#blockchain_txn_routing_v1_pb.nonce.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec signature(txn_routing()) -> binary().
signature(Txn) ->
    Txn#blockchain_txn_routing_v1_pb.signature.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec sign(txn_routing(), libp2p_crypto:sig_fun()) -> txn_routing().
sign(Txn, SigFun) ->
    EncodedTxn = blockchain_txn_routing_v1_pb:encode_msg(Txn),
    Txn#blockchain_txn_routing_v1_pb{signature=SigFun(EncodedTxn)}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_valid(txn_routing(),
               blockchain:blockchain()) -> ok | {error, any()}.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    OUI = ?MODULE:oui(Txn),
    case blockchain_ledger_v1:find_routing(OUI, Ledger) of
        {error, _}=Error ->
            Error;
        {ok, Routing} ->
            Owner = ?MODULE:owner(Txn),
            case Owner == blockchain_ledger_routing_v1:owner(Routing) of
                false ->
                    {error, bad_owner};
                true ->
                    Nonce = ?MODULE:nonce(Txn),
                    LedgerNonce = blockchain_ledger_routing_v1:nonce(Routing),
                    case Nonce == LedgerNonce + 1 of
                        false ->
                            {error, {bad_nonce, {routing, Nonce, LedgerNonce}}};
                        true ->
                            Signature = ?MODULE:signature(Txn),
                            PubKey = libp2p_crypto:bin_to_pubkey(Owner),
                            BaseTxn = Txn#blockchain_txn_routing_v1_pb{signature = <<>>},
                            EncodedTxn = blockchain_txn_routing_v1_pb:encode_msg(BaseTxn),
                            case libp2p_crypto:verify(EncodedTxn, Signature, PubKey) of
                                false ->
                                    {error, bad_signature};
                                true ->
                                    Addresses = ?MODULE:addresses(Txn),
                                    case validate_addresses(Addresses) of
                                        false ->
                                            {error, invalid_addresses};
                                        true ->
                                            Fee = ?MODULE:fee(Txn),
                                            Owner = ?MODULE:owner(Txn),
                                            blockchain_ledger_v1:check_dc_balance(Owner, Fee, Ledger)
                                    end
                            end
                    end
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb(txn_routing(),
             blockchain:blockchain()) -> ok | {error, any()}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Fee = ?MODULE:fee(Txn),
    Owner = ?MODULE:owner(Txn),
    case blockchain_ledger_v1:debit_fee(Owner, Fee, Ledger) of
        {error, _}=Error ->
            Error;
        ok ->
            OUI = ?MODULE:oui(Txn),
            Addresses = ?MODULE:addresses(Txn),
            Nonce = ?MODULE:nonce(Txn),
            blockchain_ledger_v1:add_routing(Owner, OUI, Addresses, Nonce, Ledger)
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec print(txn_routing()) -> iodata().
print(undefined) -> <<"type=routing undefined">>;
print(#blockchain_txn_routing_v1_pb{oui=OUI, owner=Owner,
                                    addresses=Addresses, fee=Fee,
                                    nonce=Nonce, signature=Sig}) ->
    io_lib:format("type=routing oui=~p owner=~p addresses=~p fee=~p nonce=~p signature=~p",
                  [OUI, ?TO_B58(Owner), Addresses, Fee, Nonce, Sig]).

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

-spec validate_addresses(string()) -> boolean().
validate_addresses([]) ->
    true;
validate_addresses(Addresses) ->
    case erlang:length(Addresses) of
        L when L =< 3 ->
            lists:all(fun is_p2p/1, Addresses);
        _ ->
            false
    end.

-spec is_p2p(binary()) -> boolean().
is_p2p(Address) ->
    case catch multiaddr:protocols(erlang:binary_to_list(Address)) of
        [{"p2p", _}] -> true;
        _ -> false
    end.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Tx = #blockchain_txn_routing_v1_pb{
        oui= 0,
        owner= <<"owner">>,
        addresses = [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>],
        fee=1,
        nonce = 0,
        signature= <<>>
    },
    ?assertEqual(Tx, new(0, <<"owner">>, [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>], 1, 0)).

oui_test() ->
    Tx = new(0, <<"owner">>, [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>], 1, 0),
    ?assertEqual(0, oui(Tx)).

fee_test() ->
    Tx = new(0, <<"owner">>, [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>], 1, 0),
    ?assertEqual(1, fee(Tx)).

owner_test() ->
    Tx = new(0, <<"owner">>, [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>], 1, 0),
    ?assertEqual(<<"owner">>, owner(Tx)).

addresses_test() ->
    Tx = new(0, <<"owner">>, [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>], 1, 0),
    ?assertEqual([<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>], addresses(Tx)).

nonce_test() ->
    Tx = new(0, <<"owner">>, [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>], 1, 0),
    ?assertEqual(0, nonce(Tx)).

signature_test() ->
    Tx = new(0, <<"owner">>, [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>], 1, 0),
    ?assertEqual(<<>>, signature(Tx)).

sign_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Tx0 = new(0, <<"owner">>, [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>], 1, 0),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign(Tx0, SigFun),
    Sig1 = signature(Tx1),
    EncodedTx1 = blockchain_txn_routing_v1_pb:encode_msg(Tx1#blockchain_txn_routing_v1_pb{signature = <<>>}),
    ?assert(libp2p_crypto:verify(EncodedTx1, Sig1, PubKey)).

ecode_decode_test() ->
    Tx = new(0, <<"owner">>, [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>, <<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>],  1, 0),
    ?assertEqual(Tx, blockchain_txn_routing_v1_pb:decode_msg(blockchain_txn_routing_v1_pb:encode_msg(Tx), blockchain_txn_routing_v1_pb)).

validate_addresses_test() ->
    ?assert(validate_addresses([])),
    ?assert(validate_addresses([<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>])),
    ?assert(validate_addresses([<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>, <<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>])),
    ?assert(validate_addresses([<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>, <<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>, <<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>])),
    ?assertNot(validate_addresses([<<"http://test.com">>])),
    ?assertNot(validate_addresses([<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>, <<"http://test.com">>])),
    ?assertNot(validate_addresses([<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>, <<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>, <<"http://test.com">>])),
    ok.

-endif.
