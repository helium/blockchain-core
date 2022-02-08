%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Validator Heartbeat ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_validator_heartbeat_v1).

-behavior(blockchain_txn).

-behavior(blockchain_json).
-include("blockchain_json.hrl").
-include("blockchain_utils.hrl").
-include("blockchain_txn_fees.hrl").
-include("blockchain_vars.hrl").
-include_lib("helium_proto/include/blockchain_txn_validator_heartbeat_v1_pb.hrl").

-export([
         new/3,
         hash/1,
         address/1,
         height/1,
         signature/1,
         version/1,
         fee/1,
         fee_payer/2,
         sign/2,
         is_valid/2,
         is_well_formed/1,
         is_prompt/2,
         absorb/2,
         print/1,
         json_type/0,
         to_json/2
        ]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(T, #blockchain_txn_validator_heartbeat_v1_pb).

-type t() :: txn_validator_heartbeat().

-type txn_validator_heartbeat() :: ?T{}.

-export_type([t/0, txn_validator_heartbeat/0]).

-spec new(libp2p_crypto:pubkey_bin(), pos_integer(), pos_integer()) ->
          txn_validator_heartbeat().
new(Address, Height, Version) ->
    #blockchain_txn_validator_heartbeat_v1_pb{
       address = Address,
       height = Height,
       version = Version
    }.

-spec hash(txn_validator_heartbeat()) -> blockchain_txn:hash().
hash(Txn) ->
    BaseTxn = Txn#blockchain_txn_validator_heartbeat_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_validator_heartbeat_v1_pb:encode_msg(BaseTxn),
    crypto:hash(sha256, EncodedTxn).

-spec address(txn_validator_heartbeat()) -> libp2p_crypto:pubkey_bin().
address(Txn) ->
    Txn#blockchain_txn_validator_heartbeat_v1_pb.address.

-spec height(txn_validator_heartbeat()) -> pos_integer().
height(Txn) ->
    Txn#blockchain_txn_validator_heartbeat_v1_pb.height.

-spec version(txn_validator_heartbeat()) -> pos_integer().
version(Txn) ->
    Txn#blockchain_txn_validator_heartbeat_v1_pb.version.

-spec signature(txn_validator_heartbeat()) -> binary().
signature(Txn) ->
    Txn#blockchain_txn_validator_heartbeat_v1_pb.signature.

-spec fee(txn_validator_heartbeat()) -> 0.
fee(_Txn) ->
    0.

-spec fee_payer(txn_validator_heartbeat(), blockchain_ledger_v1:ledger()) -> libp2p_crypto:pubkey_bin() | undefined.
fee_payer(_Txn, _Ledger) ->
    undefined.

-spec sign(txn_validator_heartbeat(), libp2p_crypto:sig_fun()) -> txn_validator_heartbeat().
sign(Txn, SigFun) ->
    BaseTxn = Txn#blockchain_txn_validator_heartbeat_v1_pb{signature= <<>>},
    EncodedTxn = blockchain_txn_validator_heartbeat_v1_pb:encode_msg(BaseTxn),
    Txn#blockchain_txn_validator_heartbeat_v1_pb{signature=SigFun(EncodedTxn)}.

-spec is_valid_sig(txn_validator_heartbeat()) -> boolean().
is_valid_sig(#blockchain_txn_validator_heartbeat_v1_pb{address=PubKeyBin,
                                                       signature=Signature}=Txn) ->
    BaseTxn = Txn#blockchain_txn_validator_heartbeat_v1_pb{signature= <<>>},
    EncodedTxn = blockchain_txn_validator_heartbeat_v1_pb:encode_msg(BaseTxn),
    PubKey = libp2p_crypto:bin_to_pubkey(PubKeyBin),
    libp2p_crypto:verify(EncodedTxn, Signature, PubKey).

-spec is_valid(txn_validator_heartbeat(), blockchain:blockchain()) ->
          ok | {error, atom()} | {error, {atom(), any()}}.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Validator = address(Txn),
    Version = version(Txn),
    TxnHeight = height(Txn),
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
    case is_valid_sig(Txn) of
        false ->
            {error, bad_signature};
        _ ->
            try
                case blockchain:config(?validator_version, Ledger) of
                    {ok, Vers} when Vers >= 1 ->
                        ok;
                    _ -> throw(unsupported_txn)
                end,
                %% make sure that this validator exists and is staked
                case blockchain_ledger_v1:get_validator(Validator, Ledger) of
                    {ok, V} ->
                        {ok, Interval} = blockchain_ledger_v1:config(?validator_liveness_interval, Ledger),
                        Status = blockchain_ledger_validator_v1:status(V),
                        HB = blockchain_ledger_validator_v1:last_heartbeat(V),
                        case Status == staked
                            andalso TxnHeight >= (Interval + HB)
                            andalso TxnHeight =< Height of
                            true -> ok;
                            _ -> throw({bad_height, prev, HB, height, Height, got, TxnHeight})
                        end;
                    {error, not_found} -> throw(nonexistent_validator);
                    {error, Reason} -> throw({validator_fetch_error, Reason})
                end,
                case valid_version(Version)  of
                    true -> ok;
                    false -> throw({bad_version, Version})
                end,
                ok
            catch throw:Cause ->
                    {error, Cause}
            end
    end.

-spec is_well_formed(t()) -> ok | {error, {contract_breach, any()}}.
is_well_formed(?T{}) ->
    ok.

-spec is_prompt(t(), blockchain:blockchain()) ->
    {ok, blockchain_txn:is_prompt()} | {error, any()}.
is_prompt(?T{}, _) ->
    {ok, yes}.

%% oh dialyzer
valid_version(V) when is_integer(V) andalso V > 0 ->
    true;
valid_version(_) ->
    false.

-spec absorb(txn_validator_heartbeat(), blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Validator = address(Txn),
    Version = version(Txn),
    TxnHeight = height(Txn),

    case blockchain_ledger_v1:get_validator(Validator, Ledger) of
        {ok, V} ->
            V1 = blockchain_ledger_validator_v1:last_heartbeat(TxnHeight, V),
            V2 = blockchain_ledger_validator_v1:version(Version, V1),
            blockchain_ledger_v1:update_validator(Validator, V2, Ledger);
        Err -> Err
    end.

-spec print(txn_validator_heartbeat()) -> iodata().
print(undefined) -> <<"type=validator_heartbeat, undefined">>;
print(#blockchain_txn_validator_heartbeat_v1_pb{
         address = Val,
         height = H,
         version = V}) ->
    io_lib:format("type=validator_heartbeat, validator=~p, height=~p, version=~p",
                  [?TO_ANIMAL_NAME(Val), H, V]).

json_type() ->
    <<"validator_heartbeat_v1">>.

-spec to_json(txn_validator_heartbeat(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Txn, _Opts) ->
    #{
      type => ?MODULE:json_type(),
      hash => ?BIN_TO_B64(hash(Txn)),
      address => ?BIN_TO_B58(address(Txn)),
      height => height(Txn),
      signature => ?BIN_TO_B64(signature(Txn)),
      version => version(Txn)
     }.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

to_json_test() ->
    Tx = new(<<"validator_address">>, 20000, 1),
    Json = to_json(Tx, []),
    ?assertEqual(lists:sort(maps:keys(Json)),
                 lists:sort([type, hash] ++ record_info(fields, blockchain_txn_validator_heartbeat_v1_pb))).


-endif.
