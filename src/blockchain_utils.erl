%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain Utils ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_utils).

-export([
    shuffle_from_hash/2,
    shuffle/1,
    rand_from_hash/1,
    normalize_float/1,
    challenge_interval/1,
    serialize_hash/1, deserialize_hash/1,
    hex_to_bin/1, bin_to_hex/1,
    pmap/2,
    addr2name/1
]).

-define(CHALLENGE_INTERVAL, poc_challenge_interval).

%%--------------------------------------------------------------------
%% @doc Shuffle a list deterministically using a random binary as the seed.
%% @end
%%--------------------------------------------------------------------
-spec shuffle_from_hash(binary(), list()) -> list().
shuffle_from_hash(Hash, L) ->
    ?MODULE:rand_from_hash(Hash),
    [X ||{_, X} <- lists:sort([{rand:uniform(), E} || E <- L])].

%%--------------------------------------------------------------------
%% @doc Shuffle a list randomly.
%% @end
%%--------------------------------------------------------------------
shuffle(List) ->
    [X || {_,X} <- lists:sort([{rand:uniform(), N} || N <- List])].

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec rand_from_hash(binary()) -> any().
rand_from_hash(Hash) ->
    <<I1:86/integer, I2:85/integer, I3:85/integer, _/binary>> = Hash,
    rand:seed(exs1024, {I1, I2, I3}).

%%--------------------------------------------------------------------
%% @doc normalize a float by converting it to fixed point and back
%% using 16 bits of exponent precision. This should be well above
%% the floating point error threshold and doing this will prevent
%% errors from accumulating.
%% @end
%%--------------------------------------------------------------------
normalize_float(Float) ->
    round(Float * 65536) / 65536.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec challenge_interval(blockchain_ledger_v1:ledger()) -> non_neg_integer().
challenge_interval(Ledger) ->
    {ok, Interval} = blockchain:config(?CHALLENGE_INTERVAL, Ledger),
    Interval.

-spec serialize_hash(binary()) -> string().
serialize_hash(Hash) ->
    libp2p_crypto:bin_to_b58(Hash).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec deserialize_hash(string()) -> binary().
deserialize_hash(String) ->
    libp2p_crypto:b58_to_bin(String).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec bin_to_hex(binary()) -> string().
bin_to_hex(Bin) ->
  lists:flatten([[io_lib:format("~2.16.0b",[X]) || <<X:8>> <= Bin ]]).

-spec hex_to_bin(binary()) -> binary().
hex_to_bin(Hex) ->
  << begin {ok, [V], []} = io_lib:fread("~16u", [X, Y]), <<V:8/integer-little>> end || <<X:8/integer, Y:8/integer>> <= Hex >>.

pmap(F, L) ->
    Parent = self(),
    lists:foldl(
      fun(X, N) ->
              spawn(
                fun() ->
                        Parent ! {pmap, N, F(X)}
                end),
              N+1
      end, 0, L),
    L2 = [receive
              {pmap, N, R} -> {N,R}
          end || _ <- L],
    {_, L3} = lists:unzip(lists:keysort(1, L2)),
    L3.

addr2name(Addr) ->
    B58Addr = libp2p_crypto:bin_to_b58(Addr),
    {ok, N} = erl_angry_purple_tiger:animal_name(B58Addr),
    N.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% EUNIT Tests
%% ------------------------------------------------------------------
-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

serialize_deserialize_test() ->
    Hash = <<"123abc">>,
    ?assertEqual(Hash, deserialize_hash(serialize_hash(Hash))).

-endif.
