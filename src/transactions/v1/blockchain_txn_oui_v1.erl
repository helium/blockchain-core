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
    addresses/1,
    nonce/1,
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
-spec new(non_neg_integer(), non_neg_integer(), libp2p_crypto:pubkey_bin(), [binary()]) -> txn_oui().
new(OUI, Fee, Owner, Addresses) ->
    #blockchain_txn_oui_v1_pb{
       oui=OUI,
       fee=Fee,
       owner=Owner,
       addresses=Addresses,
       nonce=0,
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
-spec oui(txn_oui()) -> non_neg_integer().
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
-spec addresses(txn_oui()) -> [binary()].
addresses(Txn) ->
    Txn#blockchain_txn_oui_v1_pb.addresses.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec nonce(txn_oui()) -> 0.
nonce(Txn) ->
    Txn#blockchain_txn_oui_v1_pb.nonce.

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
-spec is_valid(txn_oui(), blockchain_ledger_v1:ledger()) ->ok | {error, any()}.
is_valid(Txn, Ledger) ->
    OUI = ?MODULE:oui(Txn),
    case blockchain_ledger_v1:find_routing(OUI, Ledger) of
        {error, not_found} ->
            Owner = ?MODULE:owner(Txn),
            Signature = ?MODULE:signature(Txn),
            PubKey = libp2p_crypto:bin_to_pubkey(Owner),
            BaseTxn = Txn#blockchain_txn_oui_v1_pb{signature = <<>>},
            EncodedTxn = blockchain_txn_oui_v1_pb:encode_msg(BaseTxn),
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
                            blockchain_ledger_v1:check_balance(Owner, Fee, Ledger)
                    end
            end;
        {error, _}=Error ->
            Error;
        {ok, _} ->
            {error, already_exist}
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
    case blockchain_ledger_v1:debit_fee(Owner, Fee, Ledger) of
        {error, _}=Error ->
            Error;
        ok ->
            OUI = ?MODULE:oui(Txn),
            Addresses = ?MODULE:addresses(Txn),
            Nonce = ?MODULE:nonce(Txn),
            blockchain_ledger_v1:add_routing(Owner, OUI, Addresses, Nonce, Ledger)
    end.

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
    Tx = #blockchain_txn_oui_v1_pb{
        oui= 0,
        fee=1,
        owner= <<"owner">>,
        addresses = [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>],
        nonce = 0,
        signature= <<>>
    },
    ?assertEqual(Tx, new(0, 1, <<"owner">>, [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>])).

oui_test() ->
    Tx = new(0, 1, <<"owner">>, [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>]),
    ?assertEqual(0, oui(Tx)).

fee_test() ->
    Tx = new(0, 1, <<"owner">>, [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>]),
    ?assertEqual(1, fee(Tx)).

owner_test() ->
    Tx = new(0, 1, <<"owner">>, [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>]),
    ?assertEqual(<<"owner">>, owner(Tx)).

addresses_test() ->
    Tx = new(0, 1, <<"owner">>, [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>]),
    ?assertEqual([<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>], addresses(Tx)).

nonce_test() ->
    Tx = new(0, 1, <<"owner">>, [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>]),
    ?assertEqual(0, nonce(Tx)).

signature_test() ->
    Tx = new(0, 1, <<"owner">>, [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>]),
    ?assertEqual(<<>>, signature(Tx)).

sign_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Tx0 = new(0, 1, <<"owner">>, [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>]),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign(Tx0, SigFun),
    Sig1 = signature(Tx1),
    EncodedTx1 = blockchain_txn_oui_v1_pb:encode_msg(Tx1#blockchain_txn_oui_v1_pb{signature = <<>>}),
    ?assert(libp2p_crypto:verify(EncodedTx1, Sig1, PubKey)).

ecode_decode_test() ->
    Tx = new(0, 1, <<"owner">>, [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>, <<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>]),
    ?assertEqual(Tx, blockchain_txn_oui_v1_pb:decode_msg(blockchain_txn_oui_v1_pb:encode_msg(Tx), blockchain_txn_oui_v1_pb)).

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
