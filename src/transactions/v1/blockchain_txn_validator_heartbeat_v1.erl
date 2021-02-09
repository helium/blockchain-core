%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Stake Validator ==
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
         new/2,
         hash/1,
         addr/1,
         height/1,
         signature/1,
         fee/1,
         sign/2,
         is_valid/2,
         absorb/2,
         print/1,
         to_json/2
        ]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type txn_validator_heartbeat() :: #blockchain_txn_validator_heartbeat_v1_pb{}.
-export_type([txn_validator_heartbeat/0]).

-spec new(libp2p_crypto:pubkey_bin(), pos_integer()) ->
          txn_validator_heartbeat().
new(Address, Height) ->
    #blockchain_txn_validator_heartbeat_v1_pb{
       addr = Address,
       height = Height
    }.

-spec hash(txn_validator_heartbeat()) -> blockchain_txn:hash().
hash(Txn) ->
    BaseTxn = Txn#blockchain_txn_validator_heartbeat_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_validator_heartbeat_v1_pb:encode_msg(BaseTxn),
    crypto:hash(sha256, EncodedTxn).

-spec addr(txn_validator_heartbeat()) -> libp2p_crypto:pubkey_bin().
addr(Txn) ->
    Txn#blockchain_txn_validator_heartbeat_v1_pb.addr.

-spec height(txn_validator_heartbeat()) -> pos_integer().
height(Txn) ->
    Txn#blockchain_txn_validator_heartbeat_v1_pb.height.

-spec signature(txn_validator_heartbeat()) -> binary().
signature(Txn) ->
    Txn#blockchain_txn_validator_heartbeat_v1_pb.signature.

-spec fee(txn_validator_heartbeat()) -> 0.
fee(_Txn) ->
    0.

-spec sign(txn_validator_heartbeat(), libp2p_crypto:sig_fun()) -> txn_validator_heartbeat().
sign(Txn, SigFun) ->
    BaseTxn = Txn#blockchain_txn_validator_heartbeat_v1_pb{signature= <<>>},
    EncodedTxn = blockchain_txn_validator_heartbeat_v1_pb:encode_msg(BaseTxn),
    Txn#blockchain_txn_validator_heartbeat_v1_pb{signature=SigFun(EncodedTxn)}.

-spec is_valid_sig(txn_validator_heartbeat()) -> boolean().
is_valid_sig(#blockchain_txn_validator_heartbeat_v1_pb{addr=PubKeyBin,
                                                         signature=Signature}=Txn) ->
    BaseTxn = Txn#blockchain_txn_validator_heartbeat_v1_pb{signature= <<>>},
    EncodedTxn = blockchain_txn_validator_heartbeat_v1_pb:encode_msg(BaseTxn),
    PubKey = libp2p_crypto:bin_to_pubkey(PubKeyBin),
    libp2p_crypto:verify(EncodedTxn, Signature, PubKey).

-spec is_valid(txn_validator_heartbeat(), blockchain:blockchain()) ->
          ok | {error, atom()} | {error, {atom(), any()}}.
is_valid(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Validator = addr(Txn),
    TxnHeight = height(Txn),
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),
    case is_valid_sig(Txn) of
        false ->
            {error, bad_signature};
        _ ->
            try
                %% make sure that this validator exists and is staked
                case blockchain_ledger_v1:get_validator(Validator, Ledger) of
                    {ok, V} ->
                        {ok, Interval} = blockchain_ledger_v1:config(?validator_liveness_interval, Ledger),
                        {ok, HB} = blockchain_ledger_validator_v1:last_heartbeat(V),
                        case TxnHeight > (Interval + HB) andalso TxnHeight =< Height of
                            true -> ok;
                            _ -> throw({bad_height, prev, HB, height, Height, got, TxnHeight})
                        end;
                    {error, not_found} -> throw(nonexistent_validator);
                    {error, Reason} -> throw({validator_fetch_error, Reason})
                end,
                ok
            catch throw:Cause ->
                    {error, Cause}
            end
    end.

-spec absorb(txn_validator_heartbeat(), blockchain:blockchain()) -> ok | {error, atom()} | {error, {atom(), any()}}.
absorb(Txn, Chain) ->
    Ledger = blockchain:ledger(Chain),
    Validator = addr(Txn),
    TxnHeight = height(Txn),

    case blockchain_ledger_v1:get_validator(Validator, Ledger) of
        {ok, V} ->
            V1 = blockchain_ledger_validator_v1:last_heartbeat(TxnHeight, V),
            blockchain_ledger_v1:update_validator(Validator, V1, Ledger);
        Err -> Err
    end.

-spec print(txn_validator_heartbeat()) -> iodata().
print(undefined) -> <<"type=validator_heartbeat, undefined">>;
print(#blockchain_txn_validator_heartbeat_v1_pb{
         addr = Val,
         height = H}) ->
    io_lib:format("type=validator_heartbeat, validator=~p, height=~p",
                  [?TO_ANIMAL_NAME(Val), H]).


-spec to_json(txn_validator_heartbeat(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Txn, _Opts) ->
    #{
      type => <<"validator_heartbeat_v1">>,
      hash => ?BIN_TO_B64(hash(Txn)),
      addr => ?BIN_TO_B58(addr(Txn)),
      height => height(Txn),
      signature => ?BIN_TO_B64(signature(Txn))
     }.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

to_json_test() ->
    Tx = new(<<"validator_address">>, 20000, <<"sdasdasdasd">>),
    Json = to_json(Tx, []),
    ?assertEqual(lists:sort(maps:keys(Json)),
                 lists:sort([type, hash] ++ record_info(fields, blockchain_txn_validator_heartbeat_v1_pb))).


-endif.
