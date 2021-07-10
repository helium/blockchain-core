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
    fee_payer/2,
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
-type id() :: binary().
-export_type([id/0, txn_state_channel_open/0]).

-spec new(ID :: id(),
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

-spec id(Txn :: txn_state_channel_open()) -> id().
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

-spec fee_payer(txn_state_channel_open(), blockchain_ledger_v1:ledger()) -> libp2p_crypto:pubkey_bin() | undefined.
fee_payer(Txn, _Ledger) ->
    owner(Txn).

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

-spec is_valid(Txn :: txn_state_channel_open(), Chain :: blockchain:blockchain()) ->
    ok | {error, atom() | {atom(), _}}.
is_valid(Txn, Chain) ->
    % FIXME Choose only one: is_valid_1 or is_valid_2 and rename to is_valid
    % TODO Specs for each is_valid_* componenet function
    IsValid = is_valid_1(Txn, Chain),
    IsValid = is_valid_2(Txn, Chain),
    IsValid.

is_valid_1(Txn, Chain) ->
    %% Advantage:
    %% - no exceptions
    %% - individual components testable
    %% Disadvantage:
    %% - need to manually weave dependencies through funs
    Ledger = blockchain:ledger(Chain),
    Owner = ?MODULE:owner(Txn),
    Result =
        result_pipe({}, [
            fun({}) -> is_valid_sig(Txn, Owner) end,
            fun({}) ->
                case blockchain:config(?min_expire_within, Ledger) of
                    {ok, _}=Ok -> Ok;
                    {error, _} -> {error, min_expire_within_not_set}
                end
            end,
            fun(MinExpireWithin) ->
                case blockchain:config(?max_open_sc, Ledger) of
                    {ok, MaxOpenSC} -> {ok, {MinExpireWithin, MaxOpenSC}};
                    {error, _}      -> {error, max_open_sc_not_set}
                end
            end,
            fun({MinExpireWithin, MaxOpenSC}) ->
                case is_valid_expiry(Txn, Ledger, MinExpireWithin) of
                    {ok, {}} ->
                        {ok, MaxOpenSC};
                    {error, _}=Err ->
                        Err
                end
            end,
            fun(MaxOpenSC) ->
                case is_valid_router(Txn, Ledger, Owner) of
                    {ok, {}} ->
                        {ok, MaxOpenSC};
                    {error, _}=Err ->
                        Err
                end
            end,
            fun(MaxOpenSC) -> is_valid_sc_open_limit(MaxOpenSC, Ledger, Owner) end,
            fun({}) -> is_valid_sc_exists(?MODULE:id(Txn), Ledger, Owner) end,
            fun({}) -> is_valid_nonce(?MODULE:nonce(Txn), Ledger, Owner) end,
            fun({}) -> is_valid_txn_fee(Txn, Chain, Ledger, Owner) end
        ]),
    case Result of
        {ok, {}} -> ok;
        {error, _}=E -> E
    end.

is_valid_2(Txn, Chain) ->
    %% Advantage:
    %% - no need to manually weave output through chained funs
    %% - reuses the exceptionless, individually testable funs from is_valid_1
    %% Disadvantage:
    %% - technically uses exceptions, though they don't bleed into the API
    %% - unfamiliar operator
    Ledger = blockchain:ledger(Chain),
    Owner = ?MODULE:owner(Txn),
    try
        '<-'(is_valid_sig(Txn, Owner)),
        MinExpireWithin = '<-'(
            blockchain:config(?min_expire_within, Ledger),
            min_expire_within_not_set
        ),
        MaxOpenSC = '<-'(
            blockchain:config(?max_open_sc, Ledger),
            min_expire_within_not_set
        ),
        '<-'(is_valid_expiry(Txn, Ledger, MinExpireWithin)),
        '<-'(is_valid_router(Txn, Ledger, Owner)),
        '<-'(is_valid_sc_open_limit(MaxOpenSC, Ledger, Owner)),
        '<-'(is_valid_sc_exists(?MODULE:id(Txn), Ledger, Owner)),
        '<-'(is_valid_nonce(?MODULE:nonce(Txn), Ledger, Owner)),
        '<-'(is_valid_txn_fee(Txn, Chain, Ledger, Owner)),
        ok
    catch throw:Reason ->
        {error, Reason}
    end.

is_valid_sig(Txn, Owner) ->
    Signature = ?MODULE:signature(Txn),
    PubKey = libp2p_crypto:bin_to_pubkey(Owner),
    BaseTxn = Txn#blockchain_txn_state_channel_open_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_state_channel_open_v1_pb:encode_msg(BaseTxn),
    case libp2p_crypto:verify(EncodedTxn, Signature, PubKey) of
        false -> {error, bad_signature};
        true -> {ok, {}}
    end.

is_valid_expiry(Txn, Ledger, MinExpireWithin) ->
    ExpireWithin = ?MODULE:expire_within(Txn),
    case
        ExpireWithin > MinExpireWithin andalso
        ExpireWithin < blockchain_utils:approx_blocks_in_week(Ledger)
    of
        false ->
            {error, invalid_expire_at_block};
        true ->
            {ok, {}}
    end.

is_valid_router(Txn, Ledger, Owner) ->
    OUI = ?MODULE:oui(Txn),
    case blockchain_ledger_v1:find_routing(OUI, Ledger) of
        {error, not_found} ->
            lager:error("oui: ~p not found for this router: ~p", [OUI, Owner]),
            {error, {not_found, {OUI, Owner}}};
        {ok, Routing} ->
            KnownRouters = blockchain_ledger_routing_v1:addresses(Routing),
            case lists:member(Owner, KnownRouters) of
                false ->
                    lager:error("unknown router: ~p, known routers: ~p", [Owner, KnownRouters]),
                    {error, unknown_router};
                true ->
                    {ok, {}}
            end
    end.

is_valid_sc_open_limit(MaxOpenSC, Ledger, Owner) ->
    case blockchain_ledger_v1:find_sc_ids_by_owner(Owner, Ledger) of
        {ok, BinIds} when length(BinIds) >= MaxOpenSC ->
            lager:error("already have max open state_channels for router: ~p", [Owner]),
            {error, {max_scs_open, Owner}};
        _ ->
            {ok, {}}
    end.

is_valid_sc_exists(ID, Ledger, Owner) ->
    case blockchain_ledger_v1:find_state_channel(ID, Owner, Ledger) of
        {error, not_found} ->
            %% No state channel with this ID for this Owner exists
            {ok, {}};
        {ok, _} ->
            {error, state_channel_already_exists};
        {error, _}=Err ->
            Err
    end.

is_valid_nonce(TxnNonce, Ledger, Owner) ->
    LedgerNonce =
        case blockchain_ledger_v1:find_dc_entry(Owner, Ledger) of
            {error, _} ->
                %% if we dont have a DC entry then default expected
                %% next nonce to 1
                0;
            {ok, Entry} ->
                blockchain_ledger_data_credits_entry_v1:nonce(Entry)
        end,
    case TxnNonce =:= LedgerNonce + 1 of
        false ->
            {error, {bad_nonce, {state_channel_open, TxnNonce, LedgerNonce}}};
        true ->
            {ok, {}}
    end.

is_valid_txn_fee(Txn, Chain, Ledger, Owner) ->
    AreFeesEnabled = blockchain_ledger_v1:txn_fees_active(Ledger),
    TxnFee = ?MODULE:fee(Txn),
    OriginalAmount = ?MODULE:amount(Txn),
    ActualAmount = actual_amount(OriginalAmount, Ledger),
    ExpectedTxnFee = ?MODULE:calculate_fee(Txn, Chain),
    case ExpectedTxnFee =< TxnFee orelse not AreFeesEnabled of
        false ->
            {error, {wrong_txn_fee, {ExpectedTxnFee, TxnFee}}};
        true ->
            case blockchain:config(?sc_open_validation_bugfix, Ledger) of
                {ok, 1} ->
                    %% Check whether the actual amount
                    %% (overcommit * original amount) + txn_fee
                    %% is payable by this owner
                    blockchain_ledger_v1:check_dc_balance(Owner, ActualAmount + TxnFee, Ledger);
                _ ->
                    blockchain_ledger_v1:check_dc_or_hnt_balance(Owner, TxnFee, Ledger, AreFeesEnabled)
            end
    end.

%% monadic bind-like thing 1
-spec result_pipe(A, [fun((A) -> {ok, B} | {error, C})]) ->
    {ok, B} | {error, C}.
result_pipe(X, []) ->
    {ok, X};
result_pipe(X, [F | Fs]) ->
    case F(X) of
        {ok, Y} ->
            result_pipe(Y, Fs);
        {error, _}=Error ->
            Error
    end.

%% monadic bind-like thing 2
'<-'(Result) ->
   '<-'(Result, undefined).

'<-'({ok, X}, _) -> X;
'<-'({error, Reason}, undefined) -> throw(Reason);
'<-'({error, _}, CustomReason) -> throw(CustomReason).

-spec absorb(Txn :: txn_state_channel_open(),
             Chain :: blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    AreFeesEnabled = blockchain_ledger_v1:txn_fees_active(Ledger),
    ID = ?MODULE:id(Txn),
    Owner = ?MODULE:owner(Txn),
    ExpireWithin = ?MODULE:expire_within(Txn),
    Nonce = ?MODULE:nonce(Txn),
    OriginalAmount = ?MODULE:amount(Txn),
    TxnFee = ?MODULE:fee(Txn),
    TxnHash = ?MODULE:hash(Txn),
    case blockchain_ledger_v1:debit_fee(Owner, TxnFee, Ledger, AreFeesEnabled, TxnHash, Chain) of
        {error, _Reason}=Error ->
            Error;
        ok ->
            Amount = actual_amount(OriginalAmount, Ledger),
            case blockchain_ledger_v1:debit_dc(Owner, Nonce, Amount, Ledger) of
                {error, _}=Error2 ->
                    Error2;
                ok ->
                    blockchain_ledger_v1:add_state_channel(ID, Owner, ExpireWithin,
                                                           Nonce, OriginalAmount, Amount, Ledger)
            end
    end.

-spec actual_amount(OriginalAmount :: pos_integer(),
                    Ledger :: blockchain_ledger_v1:ledger()) -> non_neg_integer().
actual_amount(OriginalAmount, Ledger) ->
    case blockchain_ledger_v1:config(?sc_overcommit, Ledger) of
        {ok, Overcommit} -> OriginalAmount * Overcommit;
        _ ->
            case blockchain_ledger_v1:config(?sc_version, Ledger) of
                {ok, SCVer} when SCVer > 1 -> OriginalAmount;
                _ ->
                    0
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
      expire_within => expire_within(Txn),
      amount => amount(Txn)
     }.

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
