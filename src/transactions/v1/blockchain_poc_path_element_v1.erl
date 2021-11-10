%%%-------------------------------------------------------------------
%% @doc
%%%-------------------------------------------------------------------
-module(blockchain_poc_path_element_v1).

-behavior(blockchain_json).
-include("blockchain_json.hrl").

-include("blockchain_utils.hrl").
-include_lib("helium_proto/include/blockchain_txn_poc_receipts_v1_pb.hrl").

-export([
    new/3,
    challengee/1,
    receipt/1,
    witnesses/1,
    print/1,
    json_type/0,
    to_json/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type poc_element() :: #blockchain_poc_path_element_v1_pb{}.
-type poc_path() :: [poc_element()].

-export_type([poc_element/0, poc_path/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(Challengee :: libp2p_crypto:pubkey_bin(),
          Receipt :: blockchain_poc_receipt_v1:poc_receipt() | undefined,
          Witnesses :: blockchain_poc_witness_v1:poc_witnesses()) -> poc_element().
new(Challengee, Receipt, Witnesses) ->
    #blockchain_poc_path_element_v1_pb{
        challengee=Challengee,
        receipt=Receipt,
        witnesses=Witnesses
    }.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec challengee(Element :: poc_element()) -> libp2p_crypto:pubkey_bin().
challengee(Element) ->
    Element#blockchain_poc_path_element_v1_pb.challengee.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec receipt(Element :: poc_element()) -> blockchain_poc_receipt_v1:poc_receipt() | undefined.
receipt(Element) ->
    Element#blockchain_poc_path_element_v1_pb.receipt.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec witnesses(Element :: poc_element()) -> blockchain_poc_witness_v1:poc_witnesses().
witnesses(Element) ->
    Element#blockchain_poc_path_element_v1_pb.witnesses.

print(undefined) ->
    <<"type=element undefined">>;
print(#blockchain_poc_path_element_v1_pb{
         challengee=Challengee,
         receipt=Receipt,
         witnesses=Witnesses
        }) ->
    io_lib:format("type=element challengee: ~s, receipt: ~s\n\t\twitnesses: ~s",
                  [
                   ?TO_ANIMAL_NAME(Challengee),
                   blockchain_poc_receipt_v1:print(Receipt),
                   string:join(lists:map(fun(Witness) ->
                                                 blockchain_poc_witness_v1:print(Witness)
                                         end,
                                         Witnesses), "\n\t\t")
                  ]).

json_type() ->
    undefiend.

-spec to_json(poc_element(), blockchain_json:opts()) -> blockchain_json:json_object().
to_json(Elem, Opts) ->
    ReceiptOpts =
        case lists:keyfind(valid_receipt, 1, Opts) of
            false -> [];
            {valid_receipt, undefined} -> [{is_valid, false}];
            {valid_receipt, _} -> [{is_valid, true}]
        end,
    Witnesses =
        case lists:keyfind(tagged_witnesses, 1, Opts) of
            false ->
                [{W, []} || W <- witnesses(Elem)];
            {tagged_witnesses, TaggedWitnesses} ->
                lists:map(fun
                              ({true, _, W}) ->
                                  {W, [{is_valid, true}]};
                              ({false, InvalidReason, W}) ->
                                  {W, [{is_valid, false}, {invalid_reason, InvalidReason}]}
                          end, TaggedWitnesses)
        end,
    #{
      challengee => ?BIN_TO_B58(challengee(Elem)),
      receipt => blockchain_poc_receipt_v1:to_json(receipt(Elem), ReceiptOpts),
      witnesses => [blockchain_poc_witness_v1:to_json(W, WOpts) || {W, WOpts} <- Witnesses]
     }.


%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Element = #blockchain_poc_path_element_v1_pb{
        challengee= <<"challengee">>,
        receipt= undefined,
        witnesses= []
    },
    ?assertEqual(Element, new(<<"challengee">>, undefined, [])).

challengee_test() ->
    Element = new(<<"challengee">>, undefined, []),
    ?assertEqual(<<"challengee">>, challengee(Element)).

receipt_test() ->
    Element = new(<<"challengee">>, undefined, []),
    ?assertEqual(undefined, receipt(Element)).

witnesses_test() ->
    Element = new(<<"challengee">>, undefined, []),
    ?assertEqual([], witnesses(Element)).

to_json_test() ->
    Element = new(<<"challengee">>, undefined, []),
    Json = to_json(Element, []),
    ?assert(lists:all(fun(K) -> maps:is_key(K, Json) end,
                      [challengee, receipt, witnesses])).


-endif.
