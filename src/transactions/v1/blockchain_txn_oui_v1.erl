%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction OUI ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_oui_v1).

-behavior(blockchain_txn).

-include("pb/blockchain_txn_oui_v1_pb.hrl").

-export([
    new/3, new/4,
    hash/1,
    owner/1,
    addresses/1,
    payer/1,
    fee/1,
    owner_signature/1,
    payer_signature/1,
    sign/2,
    sign_payer/2,
    is_valid_owner/1,
    is_valid_payer/1,
    is_valid/2,
    absorb/2
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
-spec new(libp2p_crypto:pubkey_bin(), [binary()], non_neg_integer()) -> txn_oui().
new(Owner, Addresses, Fee) ->
    #blockchain_txn_oui_v1_pb{
        owner=Owner,
        addresses=Addresses,
        payer= <<>>,
        fee=Fee,
        owner_signature= <<>>,
        payer_signature= <<>>
    }.

-spec new(libp2p_crypto:pubkey_bin(), [binary()], libp2p_crypto:pubkey_bin(), non_neg_integer()) -> txn_oui().
new(Owner, Addresses, Payer, Fee) ->
    #blockchain_txn_oui_v1_pb{
        owner=Owner,
        addresses=Addresses,
        payer=Payer,
        fee=Fee,
        owner_signature= <<>>,
        payer_signature= <<>>
    }.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec hash(txn_oui()) -> blockchain_txn:hash().
hash(Txn) ->
    BaseTxn = Txn#blockchain_txn_oui_v1_pb{owner_signature = <<>>},
    EncodedTxn = blockchain_txn_oui_v1_pb:encode_msg(BaseTxn),
    crypto:hash(sha256, EncodedTxn).

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
-spec payer(txn_oui()) -> libp2p_crypto:pubkey_bin() | <<>> | undefined.
payer(Txn) ->
    Txn#blockchain_txn_oui_v1_pb.payer.

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
-spec owner_signature(txn_oui()) -> binary().
owner_signature(Txn) ->
    Txn#blockchain_txn_oui_v1_pb.owner_signature.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec payer_signature(txn_oui()) -> binary().
payer_signature(Txn) ->
    Txn#blockchain_txn_oui_v1_pb.payer_signature.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec sign(txn_oui(), libp2p_crypto:sig_fun()) -> txn_oui().
sign(Txn, SigFun) ->
    BaseTxn = Txn#blockchain_txn_oui_v1_pb{owner_signature= <<>>, payer_signature= <<>>},
    EncodedTxn = blockchain_txn_oui_v1_pb:encode_msg(BaseTxn),
    Txn#blockchain_txn_oui_v1_pb{owner_signature=SigFun(EncodedTxn)}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec sign_payer(txn_oui(), libp2p_crypto:sig_fun()) -> txn_oui().
sign_payer(Txn, SigFun) ->
    BaseTxn = Txn#blockchain_txn_oui_v1_pb{owner_signature= <<>>, payer_signature= <<>>},
    EncodedTxn = blockchain_txn_oui_v1_pb:encode_msg(BaseTxn),
    Txn#blockchain_txn_oui_v1_pb{payer_signature=SigFun(EncodedTxn)}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_valid_owner(txn_oui()) -> boolean().
is_valid_owner(#blockchain_txn_oui_v1_pb{owner=PubKeyBin,
                                         owner_signature=Signature}=Txn) ->
    BaseTxn = Txn#blockchain_txn_oui_v1_pb{owner_signature= <<>>, payer_signature= <<>>},
    EncodedTxn = blockchain_txn_oui_v1_pb:encode_msg(BaseTxn),
    PubKey = libp2p_crypto:bin_to_pubkey(PubKeyBin),
    libp2p_crypto:verify(EncodedTxn, Signature, PubKey).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_valid_payer(txn_oui()) -> boolean().
is_valid_payer(#blockchain_txn_oui_v1_pb{payer=PubKeyBin,
                                         payer_signature=Signature}=Txn) ->
    case PubKeyBin == undefined orelse PubKeyBin == <<>> of
        true ->
            true;
        false ->
            BaseTxn = Txn#blockchain_txn_oui_v1_pb{owner_signature= <<>>, payer_signature= <<>>},
            EncodedTxn = blockchain_txn_oui_v1_pb:encode_msg(BaseTxn),
            PubKey = libp2p_crypto:bin_to_pubkey(PubKeyBin),
            libp2p_crypto:verify(EncodedTxn, Signature, PubKey)
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_valid(txn_oui(), blockchain:blockchain()) -> ok | {error, any()}.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Owner = ?MODULE:owner(Txn),
    case {?MODULE:is_valid_owner(Txn),
          ?MODULE:is_valid_payer(Txn)} of
        {false, _} ->
            {error, bad_owner_signature};
        {_, false} ->
            {error, bad_payer_signature};
        {true, true} ->
            Addresses = ?MODULE:addresses(Txn),
            case validate_addresses(Addresses) of
                false ->
                    {error, invalid_addresses};
                true ->
                    Fee = ?MODULE:fee(Txn),
                    Owner = ?MODULE:owner(Txn),
                    Payer = ?MODULE:payer(Txn),
                    ActualPayer = case Payer == undefined orelse Payer == <<>> of
                        true -> Owner;
                        false -> Payer
                    end,
                    blockchain_ledger_v1:check_dc_balance(ActualPayer, Fee, Ledger)
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb(txn_oui(), blockchain:blockchain()) -> ok | {error, any()}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Fee = ?MODULE:fee(Txn),
    Owner = ?MODULE:owner(Txn),
    Payer = ?MODULE:payer(Txn),
    ActualPayer = case Payer == undefined orelse Payer == <<>> of
        true -> Owner;
        false -> Payer
    end,
    case blockchain_ledger_v1:debit_fee(ActualPayer, Fee, Ledger) of
        {error, _}=Error ->
            Error;
        ok ->
            Addresses = ?MODULE:addresses(Txn),
            blockchain_ledger_v1:add_oui(Owner, Addresses, Ledger)
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
        owner= <<"owner">>,
        addresses = [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>],
        payer = <<>>,
        fee=1,
        owner_signature= <<>>,
        payer_signature = <<>>
    },
    ?assertEqual(Tx, new(<<"owner">>, [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>], 1)).

owner_test() ->
    Tx = new(<<"owner">>, [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>], 1),
    ?assertEqual(<<"owner">>, owner(Tx)).

addresses_test() ->
    Tx = new(<<"owner">>, [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>], 1),
    ?assertEqual([<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>], addresses(Tx)).

fee_test() ->
    Tx = new(<<"owner">>, [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>], 1),
    ?assertEqual(1, fee(Tx)).

payer_test() ->
    Tx = new(<<"owner">>, [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>], <<"payer">>, 1),
    ?assertEqual(<<"payer">>, payer(Tx)).

owner_signature_test() ->
    Tx = new(<<"owner">>, [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>], 1),
    ?assertEqual(<<>>, owner_signature(Tx)).

payer_signature_test() ->
    Tx = new(<<"owner">>, [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>], 1),
    ?assertEqual(<<>>, payer_signature(Tx)).

sign_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Tx0 = new(<<"owner">>, [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>], 1),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign(Tx0, SigFun),
    Sig1 = owner_signature(Tx1),
    EncodedTx1 = blockchain_txn_oui_v1_pb:encode_msg(Tx1#blockchain_txn_oui_v1_pb{owner_signature = <<>>, payer_signature= <<>>}),
    ?assert(libp2p_crypto:verify(EncodedTx1, Sig1, PubKey)).

sign_payer_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Tx0 = new(<<"owner">>, [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>], <<"payer">>, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign_payer(Tx0, SigFun),
    Sig1 = payer_signature(Tx1),
    EncodedTx1 = blockchain_txn_oui_v1_pb:encode_msg(Tx1#blockchain_txn_oui_v1_pb{owner_signature = <<>>, payer_signature= <<>>}),
    ?assert(libp2p_crypto:verify(EncodedTx1, Sig1, PubKey)).

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
