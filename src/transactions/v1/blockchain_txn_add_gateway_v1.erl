%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Transaction Add Gateway ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_txn_add_gateway_v1).

-behavior(blockchain_txn).

-include("pb/blockchain_txn_add_gateway_v1_pb.hrl").

-export([
    new/2,
    hash/1,
    owner/1,
    gateway/1,
    owner_signature/1,
    gateway_signature/1,
    sign/2,
    sign_request/2,
    is_valid_gateway/1,
    is_valid_owner/1,
    is/1,
    absorb/2
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
-spec new(libp2p_crypto:pubkey_bin(), libp2p_crypto:pubkey_bin()) -> txn_add_gateway().
new(OwnerAddress, GatewayAddress) ->
    #blockchain_txn_add_gateway_v1_pb{
        owner=OwnerAddress,
        gateway=GatewayAddress
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
-spec sign(txn_add_gateway(), libp2p_crypto:sig_fun()) -> txn_add_gateway().
sign(Txn, SigFun) ->
    BaseTxn = Txn#blockchain_txn_add_gateway_v1_pb{owner_signature= <<>>,
                                                   gateway_signature= <<>>},
    EncodedTxn = blockchain_txn_add_gateway_v1_pb:encode_msg(BaseTxn),
    Txn#blockchain_txn_add_gateway_v1_pb{owner_signature=SigFun(EncodedTxn)}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec sign_request(txn_add_gateway(), fun()) -> txn_add_gateway().
sign_request(Txn, SigFun) ->
    BaseTxn = Txn#blockchain_txn_add_gateway_v1_pb{owner_signature= <<>>,
                                                   gateway_signature= <<>>},
    EncodedTxn = blockchain_txn_add_gateway_v1_pb:encode_msg(BaseTxn),
    Txn#blockchain_txn_add_gateway_v1_pb{gateway_signature=SigFun(EncodedTxn)}.


%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is_valid_gateway(txn_add_gateway()) -> boolean().
is_valid_gateway(#blockchain_txn_add_gateway_v1_pb{gateway=PubKeyBin,
                                                   gateway_signature=Signature}=Txn) ->
    BaseTxn = Txn#blockchain_txn_add_gateway_v1_pb{owner_signature= <<>>,
                                                   gateway_signature= <<>>},
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
                                                   gateway_signature= <<>>},
    EncodedTxn = blockchain_txn_add_gateway_v1_pb:encode_msg(BaseTxn),
    PubKey = libp2p_crypto:bin_to_pubkey(PubKeyBin),
    libp2p_crypto:verify(EncodedTxn, Signature, PubKey).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec is(blockchain_transactions:transaction()) -> boolean().
is(Txn) ->
    erlang:is_record(Txn, blockchain_txn_add_gateway_v1_pb).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec absorb(txn_add_gateway(), blockchain_ledger_v1:ledger()) -> ok | {error, any()}.
absorb(Txn, Ledger) ->
    case {?MODULE:is_valid_owner(Txn),
          ?MODULE:is_valid_gateway(Txn)} of
        {false, _} ->
            {error, bad_owner_signature};
        {_, false} ->
            {error, bad_gateway_signature};
        {true, true} ->
            Owner = ?MODULE:owner(Txn),
            Gateway = ?MODULE:gateway(Txn),
            %% NOTE: This causes a chain fork, commenting out till we roll new rules new chain
            %% case blockchain_ledger_v1:transaction_fee(Ledger) of
            %%     {error, Error} ->
            %%         Error;
            %%     {ok, MinerFee} ->
            %%         case blockchain_ledger_v1:debit_fee(OwnerAddress, MinerFee, Ledger) of
            %%             {error, _Reason}=Error -> Error;
            %%             ok -> blockchain_ledger_v1:add_gateway(OwnerAddress, GatewayAddress, Ledger)
            %%         end
            %% end
            case blockchain_ledger_v1:add_gateway(Owner, Gateway, Ledger) of
                {error, _Reason}=Error -> Error;
                ok -> ok
            end
    end.

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Tx = #blockchain_txn_add_gateway_v1_pb{
        owner= <<"owner_address">>,
        gateway= <<"gateway_address">>,
        owner_signature= <<>>,
        gateway_signature = <<>>
    },
    ?assertEqual(Tx, new(<<"owner_address">>, <<"gateway_address">>)).

owner_address_test() ->
    Tx = new(<<"owner_address">>, <<"gateway_address">>),
    ?assertEqual(<<"owner_address">>, owner(Tx)).

gateway_address_test() ->
    Tx = new(<<"owner_address">>, <<"gateway_address">>),
    ?assertEqual(<<"gateway_address">>, gateway(Tx)).

owner_signature_test() ->
    Tx = new(<<"owner_address">>, <<"gateway_address">>),
    ?assertEqual(<<>>, owner_signature(Tx)).

gateway_signature_test() ->
    Tx = new(<<"owner_address">>, <<"gateway_address">>),
    ?assertEqual(<<>>, gateway_signature(Tx)).

sign_request_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Tx0 = new(<<"owner_address">>, <<"gateway_address">>),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign_request(Tx0, SigFun),
    Sig1 = gateway_signature(Tx1),
    BaseTx1 = Tx1#blockchain_txn_add_gateway_v1_pb{gateway_signature = <<>>, owner_signature = << >>},
    ?assert(libp2p_crypto:verify(blockchain_txn_add_gateway_v1_pb:encode_msg(BaseTx1), Sig1, PubKey)).

sign_test() ->
    #{public := PubKey, secret := PrivKey} = libp2p_crypto:generate_keys(ecc_compact),
    Tx0 = new(<<"owner_address">>, <<"gateway_address">>),
    SigFun = libp2p_crypto:mk_sig_fun(PrivKey),
    Tx1 = sign_request(Tx0, SigFun),
    Tx2 = sign(Tx1, SigFun),
    Sig2 = owner_signature(Tx2),
    BaseTx1 = Tx1#blockchain_txn_add_gateway_v1_pb{gateway_signature = <<>>, owner_signature = << >>},
    ?assert(libp2p_crypto:verify(blockchain_txn_add_gateway_v1_pb:encode_msg(BaseTx1), Sig2, PubKey)).

is_test() ->
    Tx0 = new(<<"owner_address">>, <<"gateway_address">>),
    ?assert(is(Tx0)).

-endif.
