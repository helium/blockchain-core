%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain State Channel Open ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_state_channel_open_v1).

-behavior(blockchain_txn).

-behavior(blockchain_json).
-include("blockchain_json.hrl").
-include("blockchain_txn_fees.hrl").
-include("blockchain_utils.hrl").
-include("include/blockchain_vars.hrl").
-include_lib("helium_proto/include/blockchain_txn_state_channel_open_v1_pb.hrl").

-export([
    new/6,
    hash/1,
    id/1,
    owner/1,
    oui/1,
    nonce/1,
    amount/1,
    expire_within/1,
    fee/1, fee/2,
    calculate_fee/2, calculate_fee/5,
    signature/1,
    sign/2,
    is_valid/2,
    absorb/2,
    print/1,
    to_json/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type txn_state_channel_open() :: #blockchain_txn_state_channel_open_v1_pb{}.
-export_type([txn_state_channel_open/0]).

-spec new(ID :: binary(),
          Owner :: libp2p_crypto:pubkey_bin(),
          ExpireWithin :: pos_integer(),
          OUI :: non_neg_integer(),
          Nonce :: non_neg_integer(),
          Amount :: non_neg_integer()
         ) -> txn_state_channel_open().
new(ID, Owner, ExpireWithin, OUI, Nonce, Amount) ->
    #blockchain_txn_state_channel_open_v1_pb{
        id=ID,
        owner=Owner,
        expire_within=ExpireWithin,
        oui=OUI,
        nonce=Nonce,
        amount=Amount,
        fee=?LEGACY_TXN_FEE,
        signature = <<>>
    }.

-spec hash(Txn :: txn_state_channel_open()) -> blockchain_txn:hash().
hash(Txn) ->
    BaseTxn = Txn#blockchain_txn_state_channel_open_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_state_channel_open_v1_pb:encode_msg(BaseTxn),
    crypto:hash(sha256, EncodedTxn).

-spec id(Txn :: txn_state_channel_open()) -> binary().
id(Txn) ->
    Txn#blockchain_txn_state_channel_open_v1_pb.id.

-spec owner(Txn :: txn_state_channel_open()) -> libp2p_crypto:pubkey_bin().
owner(Txn) ->
    Txn#blockchain_txn_state_channel_open_v1_pb.owner.

-spec nonce(Txn :: txn_state_channel_open()) -> non_neg_integer().
nonce(Txn) ->
    Txn#blockchain_txn_state_channel_open_v1_pb.nonce.

-spec amount(Txn :: txn_state_channel_open()) -> non_neg_integer().
amount(Txn) ->
    Txn#blockchain_txn_state_channel_open_v1_pb.amount.

-spec oui(Txn :: txn_state_channel_open()) -> non_neg_integer().
oui(Txn) ->
    Txn#blockchain_txn_state_channel_open_v1_pb.oui.

-spec expire_within(Txn :: txn_state_channel_open()) -> pos_integer().
expire_within(Txn) ->
    Txn#blockchain_txn_state_channel_open_v1_pb.expire_within.

-spec fee(txn_state_channel_open()) -> non_neg_integer().
fee(Txn) ->
    Txn#blockchain_txn_state_channel_open_v1_pb.fee.

-spec fee(txn_state_channel_open(), non_neg_integer()) -> txn_state_channel_open().
fee(Txn, Fee) ->
    Txn#blockchain_txn_state_channel_open_v1_pb{fee=Fee}.

-spec signature(Txn :: txn_state_channel_open()) -> binary().
signature(Txn) ->
    Txn#blockchain_txn_state_channel_open_v1_pb.signature.

-spec sign(Txn :: txn_state_channel_open(),
           SigFun :: libp2p_crypto:sig_fun()) -> txn_state_channel_open().
sign(Txn, SigFun) ->
    EncodedTxn = blockchain_txn_state_channel_open_v1_pb:encode_msg(Txn),
    Txn#blockchain_txn_state_channel_open_v1_pb{signature=SigFun(EncodedTxn)}.

%%--------------------------------------------------------------------
%% @doc
%% Calculate the txn fee
%% Returned value is txn_byte_size / 24
%% @end
%%--------------------------------------------------------------------
-spec calculate_fee(txn_state_channel_open(), blockchain:blockchain()) -> non_neg_integer().
calculate_fee(Txn, Chain) ->
    ?calculate_fee_prep(Txn, Chain).

-spec calculate_fee(txn_state_channel_open(), blockchain_ledger_v1:ledger(), pos_integer(), pos_integer(), boolean()) -> non_neg_integer().
calculate_fee(_Txn, _Ledger, _DCPayloadSize, _TxnFeeMultiplier, false) ->
    ?LEGACY_TXN_FEE;
calculate_fee(Txn, Ledger, DCPayloadSize, TxnFeeMultiplier, true) ->
    ?calculate_fee(Txn#blockchain_txn_state_channel_open_v1_pb{fee=0, signature = <<0:512>>}, Ledger, DCPayloadSize, TxnFeeMultiplier).

-spec is_valid(Txn :: txn_state_channel_open(),
               Chain :: blockchain:blockchain()) -> ok | {error, any()}.
is_valid(Txn, Chain) ->
    Owner = ?MODULE:owner(Txn),
    Signature = ?MODULE:signature(Txn),
    PubKey = libp2p_crypto:bin_to_pubkey(Owner),
    BaseTxn = Txn#blockchain_txn_state_channel_open_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_state_channel_open_v1_pb:encode_msg(BaseTxn),
    case libp2p_crypto:verify(EncodedTxn, Signature, PubKey) of
        false ->
            {error, bad_signature};
        true ->
            do_is_valid_checks(Txn, Chain)
    end.

-spec absorb(Txn :: txn_state_channel_open(),
             Chain :: blockchain:blockchain()) -> ok | {error, any()}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    AreFeesEnabled = blockchain_ledger_v1:txn_fees_active(Ledger),
    ID = ?MODULE:id(Txn),
    Owner = ?MODULE:owner(Txn),
    ExpireWithin = ?MODULE:expire_within(Txn),
    Nonce = ?MODULE:nonce(Txn),
    Original = ?MODULE:amount(Txn),
    TxnFee = ?MODULE:fee(Txn),
    case blockchain_ledger_v1:debit_fee(Owner, TxnFee, Ledger, AreFeesEnabled) of
        {error, _Reason}=Error ->
            Error;
        ok ->
            Amount = case blockchain_ledger_v1:config(?sc_overcommit, Ledger) of
                        {ok, Overcommit} -> Original * Overcommit;
                        _ -> 0
                     end,
            case blockchain_ledger_v1:debit_dc(Owner, Nonce, Amount, Ledger) of
                {error, _}=Error2 ->
                    Error2;
                ok ->
                    blockchain_ledger_v1:add_state_channel(ID, Owner, ExpireWithin,
                                                           Nonce, Original, Amount, Ledger)
            end
    end.

-spec print(txn_state_channel_open()) -> iodata().
print(undefined) -> <<"type=state_channel_open, undefined">>;
print(#blockchain_txn_state_channel_open_v1_pb{id=ID, owner=Owner, expire_within=ExpireWithin}) ->
    io_lib:format("type=state_channel_open, id=~p, owner=~p, expire_within=~p",
                  [ID, ?TO_B58(Owner), ExpireWithin]).

-spec to_json(txn_state_channel_open(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Txn, _Opts) ->
    #{
      type => <<"state_channel_open_v1">>,
      hash => ?BIN_TO_B64(hash(Txn)),
      id => ?BIN_TO_B64(id(Txn)),
      owner => ?BIN_TO_B58(owner(Txn)),
      oui => oui(Txn),
      fee => fee(Txn),
      nonce => nonce(Txn),
      expire_within => expire_within(Txn)
     }.

-spec do_is_valid_checks(txn_state_channel_open(), blockchain:blockchain()) -> ok | {error, any()}.
do_is_valid_checks(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    ExpireWithin = ?MODULE:expire_within(Txn),
    ID = ?MODULE:id(Txn),
    Owner = ?MODULE:owner(Txn),
    OUI = ?MODULE:oui(Txn),

    case blockchain:config(?min_expire_within, Ledger) of
        {ok, MinExpireWithin} ->
            case blockchain:config(?max_open_sc, Ledger) of
                {ok, MaxOpenSC} ->
                    case ExpireWithin > MinExpireWithin andalso ExpireWithin < blockchain_utils:approx_blocks_in_week(Ledger) of
                        false ->
                            {error, invalid_expire_at_block};
                        true ->
                            case blockchain_ledger_v1:find_routing(OUI, Ledger) of
                                {error, not_found} ->
                                    lager:error("oui: ~p not found for this router: ~p", [OUI, Owner]),
                                    {error, {not_found, OUI, Owner}};
                                {ok, Routing} ->
                                    KnownRouters = blockchain_ledger_routing_v1:addresses(Routing),
                                    case lists:member(Owner, KnownRouters) of
                                        false ->
                                            lager:error("unknown router: ~p, known routers: ~p", [Owner, KnownRouters]),
                                            {error, unknown_router};
                                        true ->
                                            case blockchain_ledger_v1:find_sc_ids_by_owner(Owner, Ledger) of
                                                {ok, BinIds} when length(BinIds) >= MaxOpenSC ->
                                                    lager:error("already have max open state_channels for router: ~p", [Owner]),
                                                    {error, {max_scs_open, Owner}};
                                                _ ->
                                                    case blockchain_ledger_v1:find_state_channel(ID, Owner, Ledger) of
                                                        {error, not_found} ->
                                                            %% No state channel with this ID for this Owner exists
                                                            AreFeesEnabled = blockchain_ledger_v1:txn_fees_active(Ledger),
                                                            TxnFee = ?MODULE:fee(Txn),
                                                            ExpectedTxnFee = ?MODULE:calculate_fee(Txn, Chain),
                                                            case ExpectedTxnFee =< TxnFee orelse not AreFeesEnabled of
                                                                false ->
                                                                    {error, {wrong_txn_fee, ExpectedTxnFee, TxnFee}};
                                                                true ->
                                                                    blockchain_ledger_v1:check_dc_or_hnt_balance(Owner, TxnFee, Ledger, AreFeesEnabled)
                                                            end;
                                                        {ok, _} ->
                                                            {error, state_channel_already_exists};
                                                        {error, _}=Err ->
                                                            Err
                                                    end
                                            end
                                    end
                            end
                    end;
                _ ->
                    {error, max_open_sc_not_set}
            end;
        _ ->
            {error, min_expire_within_not_set}
    end.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Tx = #blockchain_txn_state_channel_open_v1_pb{
        id = <<"id">>,
        owner= <<"owner">>,
        expire_within=10,
        oui=1,
        amount=10,
        nonce=1,
        fee=?LEGACY_TXN_FEE,
        signature = <<>>
    },
    ?assertEqual(Tx, new(<<"id">>, <<"owner">>, 10, 1, 1, 10)).

id_test() ->
    Tx = new(<<"id">>, <<"owner">>, 10, 1, 1, 10),
    ?assertEqual(<<"id">>, id(Tx)).

owner_test() ->
    Tx = new(<<"id">>, <<"owner">>, 10, 1, 1, 10),
    ?assertEqual(<<"owner">>, owner(Tx)).

signature_test() ->
    Tx = new(<<"id">>, <<"owner">>, 10, 1, 1, 10),
    ?assertEqual(<<>>, signature(Tx)).

amount_test() ->
    Tx = new(<<"id">>, <<"owner">>, 10, 1, 1, 10),
    ?assertEqual(10, amount(Tx)).

oui_test() ->
    Tx = new(<<"id">>, <<"owner">>, 10, 1, 1, 10),
    ?assertEqual(1, oui(Tx)).

fee_test() ->
    Tx = new(<<"id">>, <<"owner">>, 10, 1, 1, 10),
    ?assertEqual(?LEGACY_TXN_FEE, fee(Tx)).

sign_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Tx0 = new(<<"id">>, <<"owner">>, 10, 1, 1, 10),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign(Tx0, SigFun),
    Sig1 = signature(Tx1),
    EncodedTx1 = blockchain_txn_state_channel_open_v1_pb:encode_msg(Tx1#blockchain_txn_state_channel_open_v1_pb{signature = <<>>}),
    ?assert(libp2p_crypto:verify(EncodedTx1, Sig1, PubKey)).

to_json_test() ->
    Tx = new(<<"id">>, <<"owner">>, 10, 1, 1, 10),
    Json = to_json(Tx, []),
    ?assert(lists:all(fun(K) -> maps:is_key(K, Json) end,
                      [type, hash, id, owner, amount, oui, fee, nonce, expire_within])).

-endif.
