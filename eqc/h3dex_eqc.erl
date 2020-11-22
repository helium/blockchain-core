-module(h3dex_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(INDICES, lists:flatten([ h3:children(I, 5) || I <- h3:get_res0_indexes()])).

-export([prop_h3dex_check/0]).

prop_h3dex_check() ->
    ?FORALL({Hex1, Hex2, Resolution}, ?SUCHTHAT({X1, X2, _}, {gen_h3(), gen_h3(), choose(6, 6)}, X1 /= 0 andalso X2 /= 0 andalso X1 /= X2),
            begin
                Children1 = h3:children(Hex1, Resolution) -- [0],
                Children2 = h3:children(Hex2, Resolution) -- [0],

                End = h3_to_key(Hex1),
                Start = find_lower_bound_hex(Hex1),
                ?WHENFAIL(begin
                              io:format("Hex 1: ~p (~w -> ~w), Hex 2: ~p (~w -> ~w), Resolution ~p~n", [Hex1, find_lower_bound_hex(Hex1), h3_to_key(Hex1), Hex2, find_lower_bound_hex(Hex2), h3_to_key(Hex2), Resolution])
                          end,
                          noshrink(conjunction(
                                     [{all_children_in_range, eqc:equals([], lists:filter(fun(X) -> X < Start orelse X > End end, lists:sort(lists:map(fun h3_to_key/1, Children1)))) },
                                      {all_non_children_out_of_range, eqc:equals([], lists:filter(fun(E) -> X = h3_to_key(E), X > Start andalso X =< End end, Children2)) },
                                      {unparse_works, eqc:equals(Hex1, key_to_h3(h3_to_key(Hex1)))}
                                     ]
                                    )
                                  )
                         )
            end).

gen_h3() ->
    elements(?INDICES).

h3_to_key(H3) ->
    %% both reserved fields must be 0 and Mode must be 1 for this to be a h3 cell
    <<0:1/integer-unsigned-big, 1:4/integer-unsigned-big, 0:3/integer-unsigned-big, Resolution:4/integer-unsigned-big, BaseCell:7/integer-unsigned-big, Digits:45/integer-unsigned-big>> = <<H3:64/integer-unsigned-big>>,
    %% store the resolution inverted (15 - Resolution) so it sorts later
    <<BaseCell:7/integer-unsigned-big, Digits:45/integer-unsigned-big, (15 - Resolution):4/integer-unsigned-big>>.

key_to_h3(Key) ->
    <<BaseCell:7/integer-unsigned-big, Digits:45/integer-unsigned-big, InverseResolution:4/integer-unsigned-big>> = Key,
    <<H3:64/integer-unsigned-big>> = <<0:1, 1:4/integer-unsigned-big, 0:3, (15 - InverseResolution):4/integer-unsigned-big, BaseCell:7/integer-unsigned-big, Digits:45/integer-unsigned-big>>,
    H3.

find_lower_bound_hex(Hex) ->
    %% both reserved fields must be 0 and Mode must be 1 for this to be a h3 cell
    <<0:1, 1:4/integer-unsigned-big, 0:3, Resolution:4/integer-unsigned-big, BaseCell:7/integer-unsigned-big, Digits/bitstring>> = <<Hex:64/integer-unsigned-big>>,
    ActualDigitCount = Resolution * 3,
    %% pull out the actual digits used and dump the rest
    <<ActualDigits:ActualDigitCount/integer-unsigned-big, _/bitstring>> = Digits,
    Padding = 45 - ActualDigitCount,
    %% store the resolution inverted (15 - 15) = 0 so it sorts earlier
    %% pad the actual digits used with 0s on the end
    <<BaseCell:7/integer-unsigned-big, ActualDigits:ActualDigitCount/integer-unsigned-big, 0:Padding, 0:4/integer-unsigned-big>>.
