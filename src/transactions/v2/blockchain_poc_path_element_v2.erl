%%%-------------------------------------------------------------------
%% @doc
%%%-------------------------------------------------------------------
-module(blockchain_poc_path_element_v2).

-include("blockchain_utils.hrl").
-include_lib("helium_proto/include/blockchain_txn_poc_receipts_v2_pb.hrl").

-export([
    new/3,
    challengee/1,
    receipt/1,
    witnesses/1,
    print/1
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type poc_element() :: #blockchain_poc_path_element_v2_pb{}.
-type poc_path() :: [poc_element()].

-export_type([poc_element/0, poc_path/0]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec new(Challengee :: libp2p_crypto:pubkey_bin(),
          Receipt :: blockchain_poc_receipt_v2:poc_receipt() | undefined,
          Witnesses :: blockchain_poc_witness_v2:poc_witnesses()) -> poc_element().
new(Challengee, Receipt, Witnesses) ->
    #blockchain_poc_path_element_v2_pb{
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
    Element#blockchain_poc_path_element_v2_pb.challengee.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec receipt(Element :: poc_element()) -> blockchain_poc_receipt_v2:poc_receipt() | undefined.
receipt(Element) ->
    Element#blockchain_poc_path_element_v2_pb.receipt.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec witnesses(Element :: poc_element()) -> blockchain_poc_witness_v2:poc_witnesses().
witnesses(Element) ->
    Element#blockchain_poc_path_element_v2_pb.witnesses.

print(undefined) ->
    <<"type=element undefined">>;
print(#blockchain_poc_path_element_v2_pb{
         challengee=Challengee,
         receipt=Receipt,
         witnesses=Witnesses
        }) ->
    io_lib:format("type=element challengee: ~s, receipt: ~s\n\t\twitnesses: ~s",
                  [
                   ?TO_ANIMAL_NAME(Challengee),
                   blockchain_poc_receipt_v2:print(Receipt),
                   string:join(lists:map(fun(Witness) ->
                                                 blockchain_poc_witness_v2:print(Witness)
                                         end,
                                         Witnesses), "\n\t\t")
                  ]).


%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

new_test() ->
    Element = #blockchain_poc_path_element_v2_pb{
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

-endif.
