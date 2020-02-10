%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction OUI ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_oui_v1).

-behavior(blockchain_txn).

-include("blockchain_utils.hrl").
-include_lib("helium_proto/include/blockchain_txn_oui_v1_pb.hrl").

-export([
    new/5, new/6,
    hash/1,
    owner/1,
    addresses/1,
    oui/1,
    payer/1,
    staking_fee/1,
    fee/1,
    owner_signature/1,
    payer_signature/1,
    sign/2,
    sign_payer/2,
    is_valid_owner/1,
    is_valid_payer/1,
    is_valid/2,
    absorb/2,
    calculate_staking_fee/1,
    print/1
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
-spec new(libp2p_crypto:pubkey_bin(), [binary()], pos_integer(), non_neg_integer(), non_neg_integer()) -> txn_oui().
new(Owner, Addresses, OUI, StakingFee, Fee) ->
    #blockchain_txn_oui_v1_pb{
        owner=Owner,
        addresses=Addresses,
        oui=OUI,
        payer= <<>>,
        staking_fee=StakingFee,
        fee=Fee,
        owner_signature= <<>>,
        payer_signature= <<>>
    }.

-spec new(libp2p_crypto:pubkey_bin(), [binary()], pos_integer(), libp2p_crypto:pubkey_bin(), non_neg_integer(), non_neg_integer()) -> txn_oui().
new(Owner, Addresses, OUI, Payer, StakingFee, Fee) ->
    #blockchain_txn_oui_v1_pb{
        owner=Owner,
        addresses=Addresses,
        oui=OUI,
        payer=Payer,
        staking_fee=StakingFee,
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
    BaseTxn = Txn#blockchain_txn_oui_v1_pb{owner_signature = <<>>, payer_signature = <<>>},
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
-spec oui(txn_oui()) -> pos_integer().
oui(Txn) ->
    Txn#blockchain_txn_oui_v1_pb.oui.

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
-spec staking_fee(txn_oui()) -> non_neg_integer().
staking_fee(Txn) ->
    Txn#blockchain_txn_oui_v1_pb.staking_fee.

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
is_valid_payer(#blockchain_txn_oui_v1_pb{payer=undefined}) ->
    %% no payer
    true;
is_valid_payer(#blockchain_txn_oui_v1_pb{payer= <<>>, payer_signature= <<>>}) ->
    %% empty payer, empty payer_signature
    true;
is_valid_payer(#blockchain_txn_oui_v1_pb{payer=PubKeyBin,
                                         payer_signature=Signature}=Txn) ->
    BaseTxn = Txn#blockchain_txn_oui_v1_pb{owner_signature= <<>>, payer_signature= <<>>},
    EncodedTxn = blockchain_txn_oui_v1_pb:encode_msg(BaseTxn),
    PubKey = libp2p_crypto:bin_to_pubkey(PubKeyBin),
    libp2p_crypto:verify(EncodedTxn, Signature, PubKey).

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
            {ok, CurrOUI} = blockchain_ledger_v1:get_oui_counter(Ledger),
            OUI = ?MODULE:oui(Txn),
            case CurrOUI+1 == OUI of
                false ->
                    {error, {invalid_oui, OUI, CurrOUI+1}};
                true ->
                    Addresses = ?MODULE:addresses(Txn),
                    case validate_addresses(Addresses) of
                        false ->
                            {error, invalid_addresses};
                        true ->
                            StakingFee = ?MODULE:staking_fee(Txn),
                            ExpectedStakingFee = ?MODULE:calculate_staking_fee(Chain),
                            case ExpectedStakingFee == StakingFee of
                                false ->
                                    {error, {wrong_stacking_fee, ExpectedStakingFee, StakingFee}}; 
                                true ->
                                    Fee = ?MODULE:fee(Txn),
                                    Owner = ?MODULE:owner(Txn),
                                    Payer = ?MODULE:payer(Txn),
                                    ActualPayer = case Payer == undefined orelse Payer == <<>> of
                                        true -> Owner;
                                        false -> Payer
                                    end,
                                    StakingFee = ?MODULE:staking_fee(Txn),
                                    blockchain_ledger_v1:check_dc_balance(ActualPayer, Fee + StakingFee, Ledger)
                            end
                    end
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb(txn_oui(), blockchain:blockchain()) -> ok | {error, any()}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    StakingFee = ?MODULE:staking_fee(Txn),
    Fee = ?MODULE:fee(Txn),
    Owner = ?MODULE:owner(Txn),
    Payer = ?MODULE:payer(Txn),
    ActualPayer = case Payer == undefined orelse Payer == <<>> of
        true -> Owner;
        false -> Payer
    end,
    case blockchain_ledger_v1:debit_fee(ActualPayer, Fee + StakingFee, Ledger) of
        {error, _}=Error ->
            Error;
        ok ->
            Addresses = ?MODULE:addresses(Txn),
            OUI = ?MODULE:oui(Txn),
            blockchain_ledger_v1:add_oui(Owner, Addresses, OUI, Ledger)
    end.

%%--------------------------------------------------------------------
%% @doc
%% TODO: We should calulate this (one we have a token burn rate)
%%       maybe using location and/or demand
%% @end
%%--------------------------------------------------------------------
-spec calculate_staking_fee(blockchain:blockchain()) -> non_neg_integer().
calculate_staking_fee(_Chain) ->
    1.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec print(txn_oui()) -> iodata().
print(undefined) -> <<"type=oui, undefined">>;
print(#blockchain_txn_oui_v1_pb{owner=Owner, addresses=Addresses,
                                oui=OUI, payer=Payer, staking_fee=StakingFee,
                                fee=Fee, owner_signature= OS,
                                payer_signature= PS}) ->
    io_lib:format("type=oui, owner=~p, addresses=~p, oui=~p, payer=~p, staking_fee=~p, fee=~p, owner_signature=~p, payer_signature=~p",
                  [?TO_B58(Owner), Addresses, OUI, ?TO_B58(Payer), StakingFee, Fee, OS, PS]).


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

missing_payer_signature_new() ->
    #{public := PubKey, secret := _PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    #blockchain_txn_oui_v1_pb{
       owner= <<"owner">>,
       addresses = [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>],
       payer= libp2p_crypto:pubkey_to_bin(PubKey),
       payer_signature= <<>>,
       staking_fee=1,
       fee=1,
       owner_signature= <<>>
      }.

new_test() ->
    Tx = #blockchain_txn_oui_v1_pb{
        owner= <<"owner">>,
        addresses = [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>],
        oui = 1,
        payer = <<>>,
        staking_fee=2,
        fee=3,
        owner_signature= <<>>,
        payer_signature = <<>>
    },
    ?assertEqual(Tx, new(<<"owner">>, [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>], 1, 2, 3)).

owner_test() ->
    Tx = new(<<"owner">>, [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>], 1, 2, 3),
    ?assertEqual(<<"owner">>, owner(Tx)).

addresses_test() ->
    Tx = new(<<"owner">>, [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>], 1, 2, 3),
    ?assertEqual([<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>], addresses(Tx)).

oui_test() ->
    Tx = new(<<"owner">>, [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>], 1, 2, 3),
    ?assertEqual(1, oui(Tx)).

staking_fee_test() ->
    Tx = new(<<"owner">>, [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>], 1, 2, 3),
    ?assertEqual(2, staking_fee(Tx)).

fee_test() ->
    Tx = new(<<"owner">>, [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>], 1, 2, 3),
    ?assertEqual(3, fee(Tx)).

payer_test() ->
    Tx = new(<<"owner">>, [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>], 1, <<"payer">>, 2, 3),
    ?assertEqual(<<"payer">>, payer(Tx)).

owner_signature_test() ->
    Tx = new(<<"owner">>, [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>], 1, 2, 3),
    ?assertEqual(<<>>, owner_signature(Tx)).

payer_signature_test() ->
    Tx = new(<<"owner">>, [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>], 1, 2, 3),
    ?assertEqual(<<>>, payer_signature(Tx)).

missing_payer_signature_test() ->
    Tx = missing_payer_signature_new(),
    ?assertNot(is_valid_payer(Tx)).

sign_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Tx0 = new(<<"owner">>, [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>], 1, 2, 3),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign(Tx0, SigFun),
    Sig1 = owner_signature(Tx1),
    EncodedTx1 = blockchain_txn_oui_v1_pb:encode_msg(Tx1#blockchain_txn_oui_v1_pb{owner_signature = <<>>, payer_signature= <<>>}),
    ?assert(libp2p_crypto:verify(EncodedTx1, Sig1, PubKey)).

sign_payer_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Tx0 = new(<<"owner">>, [<<"/p2p/1WgtwXKS6kxHYoewW4F7aymP6q9127DCvKBmuJVi6HECZ1V7QZ">>], 1, <<"payer">>, 2, 3),
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
