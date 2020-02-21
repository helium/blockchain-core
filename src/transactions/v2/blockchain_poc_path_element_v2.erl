%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Proof of Coverage Path Element V2 ==
%%%-------------------------------------------------------------------
-module(blockchain_poc_path_element_v2).

-include("blockchain_utils.hrl").
-include("blockchain_vars.hrl").
-include_lib("helium_proto/include/blockchain_txn_poc_receipts_v2_pb.hrl").

-export([
         new/3,
         challengee/1,
         receipt/1, add_receipt/2,
         witnesses/1, add_witness/2,
         print/1,

         distant_witnesses/2,
         check_path_continuation/1,
         total_eligible_witnesses/2,
         squish/4
        ]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type poc_element() :: #blockchain_poc_path_element_v2_pb{}.
-type poc_path() :: [poc_element()].

-export_type([poc_element/0, poc_path/0]).

-spec new(Challengee :: libp2p_crypto:pubkey_bin(),
          Receipt :: blockchain_poc_receipt_v2:poc_receipt() | undefined,
          Witnesses :: blockchain_poc_witness_v2:poc_witnesses()) -> poc_element().
new(Challengee, Receipt, Witnesses) ->
    #blockchain_poc_path_element_v2_pb{
       challengee=Challengee,
       receipt=Receipt,
       witnesses=Witnesses
      }.

-spec challengee(Element :: poc_element()) -> libp2p_crypto:pubkey_bin().
challengee(Element) ->
    Element#blockchain_poc_path_element_v2_pb.challengee.

-spec receipt(Element :: poc_element()) -> blockchain_poc_receipt_v2:poc_receipt() | undefined.
receipt(Element) ->
    Element#blockchain_poc_path_element_v2_pb.receipt.

-spec add_receipt(Element :: poc_element(),
                  Receipt :: blockchain_poc_receipt_v2:poc_receipt()) -> {error, receipt_replay} | poc_element().
add_receipt(Element, Receipt) ->
    case receipt(Element) of
        R when R == Receipt ->
            %% Re-adding the same receipt should not be allowed
            {error, receipt_replay};
        _ ->
            Element#blockchain_poc_path_element_v2_pb{receipt=Receipt}
    end.

-spec add_witness(Element :: poc_element(),
                  Witness :: blockchain_poc_witness_v2:poc_witness()) -> {error, witness_already_added} | poc_element().
add_witness(Element = #blockchain_poc_path_element_v2_pb{witnesses=Witnesses}, Witness) ->
    %% Only add a witness to this path element if:
    %% It has not been a witness for this path element earlier
    case has_witness(Element, Witness) of
        false ->
            %% ok to add
            Element#blockchain_poc_path_element_v2_pb{witnesses=lists:sort([Witness | Witnesses])};
        true ->
            {error, witness_already_added}
    end.

-spec has_witness(Element :: poc_element(),
                  Witness :: blockchain_poc_witness_v2:poc_witness()) -> boolean().
has_witness(#blockchain_poc_path_element_v2_pb{witnesses=Witnesses}, Witness) ->
    lists:member(Witness, Witnesses).

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

-spec distant_witnesses(Element :: blockchain_poc_path_element_v2:poc_element(),
                        Ledger :: blockchain_ledger_v1:ledger()) -> [blockchain_poc_witness_v2:poc_witness()].
distant_witnesses(#blockchain_poc_path_element_v2_pb{witnesses=Witnesses,
                                                     challengee=Challengee},
                  Ledger) ->
    {ok, ParentRes} = blockchain_ledger_v1:config(?poc_v4_parent_res, Ledger),
    {ok, ExclusionCells} = blockchain_ledger_v1:config(?poc_v4_exclusion_cells, Ledger),

    {ok, ChallengeeGw} = blockchain_ledger_v1:find_gateway_info(Challengee, Ledger),
    ChallengeeLoc = blockchain_ledger_gateway_v2:location(ChallengeeGw),
    ChallengeeParentIndex = h3:parent(ChallengeeLoc, ParentRes),

    %% Get those witnesses which are far from the challengee gateway
    lists:filter(fun(Witness) ->
                         WitnessPubkeyBin = blockchain_poc_witness_v2:gateway(Witness),
                         {ok, WitnessGw} = blockchain_ledger_v1:find_gateway_info(WitnessPubkeyBin, Ledger),
                         WitnessGwLoc = blockchain_ledger_gateway_v2:location(WitnessGw),
                         WitnessParentIndex = h3:parent(WitnessGwLoc, ParentRes),
                         %% Check that the witness is far from the challengee hotspot
                         check_witness_challengee_distance(WitnessParentIndex, ChallengeeParentIndex, ExclusionCells)
                 end,
                 Witnesses).

-spec drop_random_eligible_witness(Hash :: binary(),
                                   Element :: poc_element(),
                                   Ledger :: blockchain_ledger_v1:ledger()) -> poc_element().
drop_random_eligible_witness(Hash, Element, Ledger) ->
    Witnesses = ?MODULE:witnesses(Element),
    ToDrop = hd(blockchain_utils:shuffle_from_hash(Hash, distant_witnesses(Element, Ledger))),
    Element#blockchain_poc_path_element_v2_pb{witnesses=lists:sort(Witnesses -- [ToDrop])}.

-spec check_witness_challengee_distance(WitnessParentIndex :: h3:h3_index(),
                                        ChallengeeParentIndex :: h3:h3_index(),
                                        ExclusionCells :: non_neg_integer()) -> boolean().
check_witness_challengee_distance(WitnessParentIndex, ChallengeeParentIndex, ExclusionCells) ->
    try h3:grid_distance(WitnessParentIndex, ChallengeeParentIndex) >= ExclusionCells of
        Res -> Res
    catch
        %% Grid distance may badarg because of pentagonal distortion or
        %% non matching resolutions or just being too far.
        %% In either of those cases, we assume that the gateway
        %% is potentially illegitimate to be a witness.
        _:_ -> false
    end.

-spec check_path_continuation(NextElements :: [poc_element()]) -> boolean().
check_path_continuation(NextElements) ->
    lists:any(fun(E) ->
                      ?MODULE:receipt(E) /= undefined orelse
                      ?MODULE:witnesses(E) /= []
              end,
              NextElements).

-spec total_eligible_witnesses(Path :: poc_path(),
                               Ledger :: blockchain_ledger_v1:ledger()) -> non_neg_integer().
total_eligible_witnesses(Path, Ledger) ->
    %% sum all the distant witnesses in the path
    lists:foldl(fun(Element, Acc) ->
                        length(distant_witnesses(Element, Ledger)) + Acc
                end,
                0,
                Path).

-spec squish(Path :: poc_path(),
             Hash :: binary(),
             Ledger :: blockchain_ledger_v1:ledger(),
             ToSquish :: non_neg_integer()) -> poc_path().
squish(Path, _Hash, _Ledger, 0) ->
    %% No more squishing needed
    Path;
squish(Path, Hash, Ledger, ToSquish) ->
    %% get length of the path
    PathLength = length(Path),

    case PathLength == 1 of
        true ->
            %% do nothing, single element path
            Path;
        false ->
            %% get the element with max number of witnesses
            MaxWitnessElement = max_witness_element(Path),
            %% get the index of that element
            MaxWitnessElementIndex = blockchain_utils:index_of(MaxWitnessElement, Path),
            %% drop a random eligible witness from that element
            NewMaxWitnessElement = drop_random_eligible_witness(Hash, MaxWitnessElement, Ledger),

            case MaxWitnessElementIndex of
                1 ->
                    %% replace at head
                    NewPath = [NewMaxWitnessElement] ++ tl(Path),
                    squish(NewPath, Hash, Ledger, ToSquish - 1);
                L when L == PathLength ->
                    %% replace at tail
                    [_ | Tail] = lists:reverse(Path),
                    NewPath = lists:reverse([NewMaxWitnessElement | Tail]),
                    squish(NewPath, Hash, Ledger, ToSquish - 1);
                _ ->
                    %% do sublist nonsense
                    NewPath = lists:sublist(Path, MaxWitnessElementIndex - 1) ++
                    [NewMaxWitnessElement] ++
                    lists:sublist(Path, MaxWitnessElementIndex + 1, PathLength),
                    squish(NewPath, Hash, Ledger, ToSquish - 1)
            end
    end.

-spec max_witness_element(Path :: poc_path()) -> poc_element().
max_witness_element(Path) ->
    hd(lists:sort(fun(E1, E2) ->
                          length(witnesses(E1)) >= length(witnesses(E2))
                  end,
                  Path)).

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

add_receipt_test() ->
    Receipt = blockchain_poc_receipt_v2:new(<<"gateway">>, -110, <<"data">>, radio, 2, 666, 667, 1, 1),
    Element0 = new(<<"challengee">>, undefined, []),
    Element = add_receipt(Element0, Receipt),
    ?assertEqual(Receipt, receipt(Element)).

add_receipt_replay_test() ->
    Receipt = blockchain_poc_receipt_v2:new(<<"gateway">>, -110, <<"data">>, radio, 2, 666, 667, 1, 1),
    Element0 = new(<<"challengee">>, undefined, []),
    Element = add_receipt(Element0, Receipt),
    ?assertEqual({error, receipt_replay}, add_receipt(Element, Receipt)).

add_witness_test() ->
    Witness = blockchain_poc_witness_v2:new(<<"gateway">>, -110, <<"hash">>, 2, erlang:system_time(microsecond), 1, 1),
    Element0 = new(<<"challengee">>, undefined, []),
    Element = add_witness(Element0, Witness),

    ?assert(lists:member(Witness, witnesses(Element))).

duplicate_witness_test() ->
    Witness = blockchain_poc_witness_v2:new(<<"gateway">>, -110, <<"hash">>, 2, erlang:system_time(microsecond), 1, 1),
    Element0 = new(<<"challengee">>, undefined, []),
    Element1 = add_witness(Element0, Witness),
    ?assertEqual({error, witness_already_added}, add_witness(Element1, Witness)).

-endif.
