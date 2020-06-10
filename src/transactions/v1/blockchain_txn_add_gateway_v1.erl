%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Add Gateway ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_add_gateway_v1).

-behavior(blockchain_txn).

-behavior(blockchain_json).
-include("blockchain_json.hrl").
-include("blockchain_utils.hrl").
-include("blockchain_txn_fees.hrl").
-include_lib("helium_proto/include/blockchain_txn_add_gateway_v1_pb.hrl").

-export([
    new/4, new/5,
    hash/1,
    owner/1,
    gateway/1,
    owner_signature/1,
    gateway_signature/1,
    payer/1,
    payer_signature/1,
    staking_fee/1,
    fee/1,
    sign/2,
    sign_request/2,
    sign_payer/2,
    is_valid_gateway/1,
    is_valid_owner/1,
    is_valid_payer/1,
    is_valid_staking_key/2,
    is_valid/2,
    absorb/2,
    calculate_fee/2, calculate_staking_fee/2,
    print/1,
    to_json/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type txn_add_gateway() :: #blockchain_txn_add_gateway_v1_pb{}.
-export_type([txn_add_gateway/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(libp2p_crypto:pubkey_bin(), libp2p_crypto:pubkey_bin(),
          non_neg_integer(), non_neg_integer()) -> txn_add_gateway().
new(OwnerAddress, GatewayAddress, StakingFee, Fee) ->
    #blockchain_txn_add_gateway_v1_pb{
        owner=OwnerAddress,
        gateway=GatewayAddress,
        staking_fee=StakingFee,
        fee=Fee
    }.

-spec new(libp2p_crypto:pubkey_bin(), libp2p_crypto:pubkey_bin(), libp2p_crypto:pubkey_bin(),
          non_neg_integer(), non_neg_integer()) -> txn_add_gateway().
new(OwnerAddress, GatewayAddress, Payer, StakingFee, Fee) ->
    #blockchain_txn_add_gateway_v1_pb{
        owner=OwnerAddress,
        gateway=GatewayAddress,
        payer=Payer,
        staking_fee=StakingFee,
        fee=Fee
    }.


%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec hash(txn_add_gateway()) -> blockchain_txn:hash().
hash(Txn) ->
    BaseTxn = Txn#blockchain_txn_add_gateway_v1_pb{owner_signature = <<>>, gateway_signature = <<>>},
    EncodedTxn = blockchain_txn_add_gateway_v1_pb:encode_msg(BaseTxn),
    crypto:hash(sha256, EncodedTxn).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec owner(txn_add_gateway()) -> libp2p_crypto:pubkey_bin().
owner(Txn) ->
    Txn#blockchain_txn_add_gateway_v1_pb.owner.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec gateway(txn_add_gateway()) -> libp2p_crypto:pubkey_bin().
gateway(Txn) ->
    Txn#blockchain_txn_add_gateway_v1_pb.gateway.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec owner_signature(txn_add_gateway()) -> binary().
owner_signature(Txn) ->
    Txn#blockchain_txn_add_gateway_v1_pb.owner_signature.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec gateway_signature(txn_add_gateway()) -> binary().
gateway_signature(Txn) ->
    Txn#blockchain_txn_add_gateway_v1_pb.gateway_signature.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec payer(txn_add_gateway()) -> libp2p_crypto:pubkey_bin() | <<>> | undefined.
payer(Txn) ->
    Txn#blockchain_txn_add_gateway_v1_pb.payer.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec payer_signature(txn_add_gateway()) -> binary().
payer_signature(Txn) ->
    Txn#blockchain_txn_add_gateway_v1_pb.payer_signature.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec staking_fee(txn_add_gateway()) -> non_neg_integer().
staking_fee(Txn) ->
    Txn#blockchain_txn_add_gateway_v1_pb.staking_fee.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec fee(txn_add_gateway()) -> non_neg_integer().
fee(Txn) ->
    Txn#blockchain_txn_add_gateway_v1_pb.fee.

%%--------------------------------------------------------------------
%% @doc
%% Calculate the txn fee
%% Returned value is txn_byte_size / 24
%% @end
%%--------------------------------------------------------------------
-spec calculate_fee(txn_add_gateway(), blockchain:blockchain()) -> non_neg_integer().
calculate_fee(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    calculate_fee(Txn, Chain, blockchain_ledger_v1:txn_fees_active(Ledger)).

-spec calculate_fee(txn_add_gateway(), blockchain:blockchain(), boolean()) -> non_neg_integer().
calculate_fee(_Txn, _Chain, false) ->
    0;
calculate_fee(Txn, _Chain, true) ->
    ?fee(Txn#blockchain_txn_add_gateway_v1_pb{fee=0, staking_fee=0}).


%%--------------------------------------------------------------------
%% @doc
%% Calculate the staking fee using the price oracles
%% returns the fee in DC
%% @end
%%--------------------------------------------------------------------
-spec calculate_staking_fee(txn_add_gateway(), blockchain:blockchain()) -> non_neg_integer().
calculate_staking_fee(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    calculate_staking_fee(Txn, Chain, blockchain_ledger_v1:txn_fees_active(Ledger)).

-spec calculate_staking_fee(txn_add_gateway(), blockchain:blockchain(), boolean()) -> non_neg_integer().
calculate_staking_fee(_Txn, _Chain, false) ->
    1;
calculate_staking_fee(Txn, Chain, true) ->
    Ledger = blockchain:ledger(Chain),
    _Payer = ?MODULE:payer(Txn),
    %%TODO - what todo with the staking server addr ?
    _StakingServerAddr = blockchain_ledger_v1:staking_server_addr(Ledger),
    TxnPriceUSD = ?staking_fee(blockchain_txn:type(Txn)),
    FeeInDC = trunc((TxnPriceUSD / ?DC_PRICE)),
    FeeInDC.


%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec sign(txn_add_gateway(), libp2p_crypto:sig_fun()) -> txn_add_gateway().
sign(Txn, SigFun) ->
    BaseTxn = Txn#blockchain_txn_add_gateway_v1_pb{owner_signature= <<>>,
                                                   gateway_signature= <<>>,
                                                   payer_signature= <<>>},
    EncodedTxn = blockchain_txn_add_gateway_v1_pb:encode_msg(BaseTxn),
    Txn#blockchain_txn_add_gateway_v1_pb{owner_signature=SigFun(EncodedTxn)}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec sign_request(txn_add_gateway(), fun()) -> txn_add_gateway().
sign_request(Txn, SigFun) ->
    BaseTxn = Txn#blockchain_txn_add_gateway_v1_pb{owner_signature= <<>>,
                                                   gateway_signature= <<>>,
                                                   payer_signature= <<>>},
    EncodedTxn = blockchain_txn_add_gateway_v1_pb:encode_msg(BaseTxn),
    Txn#blockchain_txn_add_gateway_v1_pb{gateway_signature=SigFun(EncodedTxn)}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec sign_payer(txn_add_gateway(), fun()) -> txn_add_gateway().
sign_payer(Txn, SigFun) ->
    BaseTxn = Txn#blockchain_txn_add_gateway_v1_pb{owner_signature= <<>>,
                                                   gateway_signature= <<>>,
                                                   payer_signature= <<>>},
    EncodedTxn = blockchain_txn_add_gateway_v1_pb:encode_msg(BaseTxn),
    Txn#blockchain_txn_add_gateway_v1_pb{payer_signature=SigFun(EncodedTxn)}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_valid_gateway(txn_add_gateway()) -> boolean().
is_valid_gateway(#blockchain_txn_add_gateway_v1_pb{gateway=PubKeyBin,
                                                   gateway_signature=Signature}=Txn) ->
    BaseTxn = Txn#blockchain_txn_add_gateway_v1_pb{owner_signature= <<>>,
                                                   gateway_signature= <<>>,
                                                   payer_signature= <<>>},
    EncodedTxn = blockchain_txn_add_gateway_v1_pb:encode_msg(BaseTxn),
    PubKey = libp2p_crypto:bin_to_pubkey(PubKeyBin),
    libp2p_crypto:verify(EncodedTxn, Signature, PubKey).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_valid_owner(txn_add_gateway()) -> boolean().
is_valid_owner(#blockchain_txn_add_gateway_v1_pb{owner=PubKeyBin,
                                                 owner_signature=Signature}=Txn) ->
    BaseTxn = Txn#blockchain_txn_add_gateway_v1_pb{owner_signature= <<>>,
                                                   gateway_signature= <<>>,
                                                   payer_signature= <<>>},
    EncodedTxn = blockchain_txn_add_gateway_v1_pb:encode_msg(BaseTxn),
    PubKey = libp2p_crypto:bin_to_pubkey(PubKeyBin),
    libp2p_crypto:verify(EncodedTxn, Signature, PubKey).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_valid_payer(txn_add_gateway()) -> boolean().
is_valid_payer(#blockchain_txn_add_gateway_v1_pb{payer=undefined}) ->
    %% no payer
    true;
is_valid_payer(#blockchain_txn_add_gateway_v1_pb{payer= <<>>, payer_signature= <<>>}) ->
    %% payer and payer_signature are empty
    true;
is_valid_payer(#blockchain_txn_add_gateway_v1_pb{payer=PubKeyBin,
                                                 payer_signature=Signature}=Txn) ->
    BaseTxn = Txn#blockchain_txn_add_gateway_v1_pb{owner_signature= <<>>,
                                                    gateway_signature= <<>>,
                                                    payer_signature= <<>>},
    EncodedTxn = blockchain_txn_add_gateway_v1_pb:encode_msg(BaseTxn),
    PubKey = libp2p_crypto:bin_to_pubkey(PubKeyBin),
    libp2p_crypto:verify(EncodedTxn, Signature, PubKey).

is_valid_staking_key(#blockchain_txn_add_gateway_v1_pb{payer=Payer}=Txn, Ledger) ->
    case blockchain_ledger_v1:staking_keys(Ledger) of
        not_found -> true; %% chain var not active, so default to true
        Keys -> lists:member(Payer, Keys)
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_valid(txn_add_gateway(), blockchain:blockchain()) -> ok | {error, any()}.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    case {?MODULE:is_valid_owner(Txn),
          ?MODULE:is_valid_gateway(Txn),
          ?MODULE:is_valid_payer(Txn),
          ?MODULE:is_valid_staking_key(Txn, Ledger)}
        of
        {false, _, _, _} ->
            {error, bad_owner_signature};
        {_, false, _, _} ->
            {error, bad_gateway_signature};
        {_, _, false, _} ->
            {error, bad_payer_signature};
        {_, _, _, false} ->
            {error, payer_invalid_staking_key};
        {true, true, true, true} ->
            AreFeesEnabled = blockchain_ledger_v1:txn_fees_active(Ledger),
            StakingFee = ?MODULE:staking_fee(Txn),
            ExpectedStakingFee = ?MODULE:calculate_staking_fee(Txn, Chain),
            TxnFee = ?MODULE:fee(Txn),
            ExpectedTxnFee = ?MODULE:calculate_fee(Txn, Chain),
            case {(ExpectedTxnFee == TxnFee orelse not AreFeesEnabled), ExpectedStakingFee == StakingFee} of
                {false,_} ->
                    {error, {wrong_txn_fee, ExpectedTxnFee, TxnFee}};
                {_,false} ->
                    {error, {wrong_staking_fee, ExpectedStakingFee, StakingFee}};
                {true, true} ->
                    Payer = ?MODULE:payer(Txn),
                    Owner = ?MODULE:owner(Txn),
                    ActualPayer = case Payer == undefined orelse Payer == <<>> of
                        true -> Owner;
                        false -> Payer
                    end,
                    blockchain_ledger_v1:check_dc_or_hnt_balance(ActualPayer, Fee + StakingFee, Ledger, AreFeesEnabled)
            end


    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb(txn_add_gateway(), blockchain:blockchain()) -> ok | {error, any()}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    AreFeesEnabled = blockchain_ledger_v1:txn_fees_active(Ledger),
    Owner = ?MODULE:owner(Txn),
    Gateway = ?MODULE:gateway(Txn),
    Payer = ?MODULE:payer(Txn),
    Fee = ?MODULE:fee(Txn),
    StakingFee = ?MODULE:staking_fee(Txn),
    ActualPayer = case Payer == undefined orelse Payer == <<>> of
        true -> Owner;
        false -> Payer
    end,
    case blockchain_ledger_v1:debit_fee(ActualPayer, Fee + StakingFee, Ledger, AreFeesEnabled) of
        {error, _Reason}=Error -> Error;
        ok -> blockchain_ledger_v1:add_gateway(Owner, Gateway, Ledger)
    end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec print(txn_add_gateway()) -> iodata().
print(undefined) -> <<"type=add_gateway, undefined">>;
print(#blockchain_txn_add_gateway_v1_pb{
         owner = O,
         gateway = GW,
         payer = P,
         staking_fee = SF,
         fee = F}) ->
    io_lib:format("type=add_gateway, owner=~p, gateway=~p, payer=~p, staking_fee=~p, fee=~p",
                  [?TO_B58(O), ?TO_ANIMAL_NAME(GW), ?TO_B58(P), SF, F]).


-spec to_json(txn_add_gateway(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Txn, _Opts) ->
    #{
      type => <<"add_gateway_v1">>,
      hash => ?BIN_TO_B64(hash(Txn)),
      gateway => ?BIN_TO_B58(gateway(Txn)),
      owner => ?BIN_TO_B58(owner(Txn)),
      payer => ?MAYBE_B58(payer(Txn)),
      staking_fee => staking_fee(Txn),
      fee => fee(Txn)
     }.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

missing_payer_signature_new() ->
    #{public := PubKey, secret := _PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    #blockchain_txn_add_gateway_v1_pb{
        owner= <<"owner_address">>,
        gateway= <<"gateway_address">>,
        owner_signature= <<>>,
        gateway_signature = <<>>,
        payer= libp2p_crypto:pubkey_to_bin(PubKey),
        payer_signature = <<>>,
        staking_fee = 1,
        fee = 1
      }.

valid_payer_new() ->
    #blockchain_txn_add_gateway_v1_pb{
        owner= <<"owner_address">>,
        gateway= <<"gateway_address">>,
        owner_signature= <<>>,
        payer= <<>>,
        payer_signature= <<>>,
        gateway_signature = <<>>,
        staking_fee = 1,
        fee = 1
      }.

new_test() ->
    Tx = #blockchain_txn_add_gateway_v1_pb{
        owner= <<"owner_address">>,
        gateway= <<"gateway_address">>,
        owner_signature= <<>>,
        gateway_signature = <<>>,
        payer = <<>>,
        payer_signature = <<>>,
        staking_fee = 1,
        fee = 1
    },
    ?assertEqual(Tx, new(<<"owner_address">>, <<"gateway_address">>, 1, 1)).

owner_address_test() ->
    Tx = new(<<"owner_address">>, <<"gateway_address">>, 1, 1),
    ?assertEqual(<<"owner_address">>, owner(Tx)).

gateway_address_test() ->
    Tx = new(<<"owner_address">>, <<"gateway_address">>, 1, 1),
    ?assertEqual(<<"gateway_address">>, gateway(Tx)).

staking_fee_test() ->
    Tx = new(<<"owner_address">>, <<"gateway_address">>, 2, 1),
    ?assertEqual(2, staking_fee(Tx)).

payer_test() ->
    Tx = new(<<"owner_address">>, <<"gateway_address">>, <<"payer">>, 2, 1),
    ?assertEqual(<<"payer">>, payer(Tx)).

fee_test() ->
    Tx = new(<<"owner_address">>, <<"gateway_address">>, 2, 1),
    ?assertEqual(1, fee(Tx)).

owner_signature_test() ->
    Tx = new(<<"owner_address">>, <<"gateway_address">>, 1, 1),
    ?assertEqual(<<>>, owner_signature(Tx)).

gateway_signature_test() ->
    Tx = new(<<"owner_address">>, <<"gateway_address">>, 1, 1),
    ?assertEqual(<<>>, gateway_signature(Tx)).

payer_signature_test() ->
    Tx = new(<<"owner_address">>, <<"gateway_address">>, <<"payer">>, 1, 1),
    ?assertEqual(<<>>, payer_signature(Tx)).

payer_signature_missing_test() ->
    Tx = missing_payer_signature_new(),
    ?assertNot(is_valid_payer(Tx)).

valid_new_payer_test() ->
    Tx = valid_payer_new(),
    ?assert(is_valid_payer(Tx)).

sign_request_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Tx0 = new(<<"owner_address">>, <<"gateway_address">>, 1, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign_request(Tx0, SigFun),
    Sig1 = gateway_signature(Tx1),
    BaseTx1 = Tx1#blockchain_txn_add_gateway_v1_pb{gateway_signature = <<>>, owner_signature = <<>>, payer_signature= <<>>},
    ?assert(libp2p_crypto:verify(blockchain_txn_add_gateway_v1_pb:encode_msg(BaseTx1), Sig1, PubKey)).

sign_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Tx0 = new(<<"owner_address">>, <<"gateway_address">>, 1, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign_request(Tx0, SigFun),
    Tx2 = sign(Tx1, SigFun),
    Sig2 = owner_signature(Tx2),
    BaseTx1 = Tx1#blockchain_txn_add_gateway_v1_pb{gateway_signature = <<>>, owner_signature = <<>>, payer_signature= <<>>},
    ?assert(libp2p_crypto:verify(blockchain_txn_add_gateway_v1_pb:encode_msg(BaseTx1), Sig2, PubKey)).

sign_payer_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Tx0 = new(<<"owner_address">>, <<"gateway_address">>, <<"payer">>, 1, 1),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign_request(Tx0, SigFun),
    Tx2 = sign_payer(Tx1, SigFun),
    Tx3 = sign(Tx2, SigFun),
    Sig2 = payer_signature(Tx2),
    BaseTx1 = Tx3#blockchain_txn_add_gateway_v1_pb{gateway_signature = <<>>, owner_signature = <<>>, payer_signature= <<>>},
    ?assert(libp2p_crypto:verify(blockchain_txn_add_gateway_v1_pb:encode_msg(BaseTx1), Sig2, PubKey)).

to_json_test() ->
    Tx = new(<<"owner_address">>, <<"gateway_address">>, 1, 1),
    Json = to_json(Tx, []),
    ?assert(lists:all(fun(K) -> maps:is_key(K, Json) end,
                      [type, hash, gateway, owner, payer, fee, staking_fee])).


-endif.
