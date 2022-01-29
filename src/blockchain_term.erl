%%%
%%% https://www.erlang.org/doc/apps/erts/erl_ext_dist.html
%%%

-module(blockchain_term).

-export([
    from_bin/1,
    from_file_stream_bin_list/1
]).

-export_type([
    t/0,
    result/0,
    stream/1,
    error/0,
    frame/0,
    unsound/0,
    frame_error/0,
    envelope_error/0
]).

-type t() ::
      integer()
    | float()
    | atom()
    | string()
    | binary()
    | tuple()
    | [t()]
    | nonempty_improper_list(t(), t())
    | #{t() => t()}
    .

-type frame() ::
      compressed
    | 'FLOAT_EXT'
    | 'NEW_FLOAT_EXT'
    | 'ATOM_EXT'
    | 'ATOM_CACHE_REF'
    | 'ATOM_UTF8_EXT'
    | 'SMALL_ATOM_UTF8_EXT'
    | 'SMALL_ATOM_EXT'
    | 'BIT_BINARY_EXT'
    | 'BINARY_EXT'
    | 'PORT_EXT'
    | 'NEW_PORT_EXT'
    | 'V4_PORT_EXT'
    | 'PID_EXT'
    | 'NEW_PID_EXT'
    | 'REFERENCE_EXT'
    | 'NEW_REFERENCE_EXT'
    | 'NEWER_REFERENCE_EXT'
    | 'FUN_EXT'
    | 'NEW_FUN_EXT'
    | 'EXPORT_EXT'
    | 'SMALL_INTEGER_EXT'
    | 'INTEGER_EXT'
    | 'SMALL_TUPLE_EXT'
    | 'LARGE_TUPLE_EXT'
    | 'STRING_EXT'
    | 'LIST_EXT'
    | 'SMALL_BIG_EXT'
    | 'LARGE_BIG_EXT'
    | 'MAP_EXT'
    .

-type unsound() ::
      {uncompressed_data_bad_size, {expected, non_neg_integer(), actual, non_neg_integer()}}
    | list_empty_with_non_nil_tail
    | atom_length_exceeds_system_limit
    | atom_characters_invalid
    | {sign_invalid, integer()}
    | {big_int_data_of_bad_size, binary()}
    .

-type frame_error() ::
      unsupported
    | {unmatched, binary()}
    | {unsound, unsound()}
    .

-type envelope_error() ::
      {head_version_unsupported, non_neg_integer(), expected, [pos_integer()]}
    | {tail_data_remains, binary()}
    | {unmatched, binary()}
    .

-type error() ::
      {envelope, envelope_error()}
    | {frame, frame(), frame_error()}
    | {frame_unrecognized, binary()}
    | {frame_unrecognized_tag, integer()}
    .

%% General for this module:
-type result(Ok) :: {ok, Ok} | {error, error()}.

%% Internal API - term + remaining, unparsed data:
-type result_internal(Ok) :: result({Ok, binary()}).  % general
-type result_internal()   :: result_internal(t()).    % common

%% Public API - just the final, parsed term:
-type result() :: result(t()).

-include("blockchain_term.hrl").

%% TODO Maybe use a map?
-type file_handle() ::
    {
        file:fd(),
        Pos :: non_neg_integer(),
        Len :: pos_integer()
    }.

-type stream(A) :: fun(() -> none | {some, {A, stream(A)}}).

-spec from_bin(binary()) -> result().
from_bin(<<Bin/binary>>) ->
    envelope(Bin).

%% Tries to stream a list of binaries from file.
%% TODO Generalize.
-spec from_file_stream_bin_list(file_handle()) -> stream(result()).
from_file_stream_bin_list({Fd, Pos, Len}) ->
    {ok, Pos} = file:position(Fd, {bof, Pos}),
    case file:read(Fd, 6) of
        {ok, <<?ETF_VERSION, ?ETF_TAG_LIST_EXT, N:32/integer-unsigned-big>>} ->
            stream_bin_list_elements(N, {Fd, Pos + 6, Len});
        {ok, <<V/binary>>} ->
            {some, {{error, {bad_etf_version_and_tag_and_len, V}}, stream_end()}};
        {error, _}=Err ->
            {some, {Err, stream_end()}}
    end.

stream_bin_list_elements(0, {_, _, _}) ->
    fun () -> none end;
stream_bin_list_elements(N, {Fd, Pos0, L}) ->
    fun () ->
        {ok, Pos1} = file:position(Fd, {bof, Pos0}),
        case file:read(Fd, 5) of
            {ok, <<?ETF_TAG_BINARY_EXT, Len:32/integer-unsigned-big>>} ->
                {ok, Pos2} = file:position(Fd, {bof, Pos1 + 5}),
                case file:read(Fd, Len) of
                    {ok, <<Bin/binary>>} ->
                        {some, {Bin, stream_bin_list_elements(N - 1, {Fd, Pos2 + Len, L})}};
                    {error, _}=Err ->
                        {some, {Err, stream_end()}}
                end;
            {ok, <<_/binary>>} ->
                {some, {{error, bad_bin_list_element}, stream_end()}};
            {error, _}=Err ->
                {some, {Err, stream_end()}}
        end
    end.

stream_end() ->
    fun () -> none end.

%% TODO -spec from_bin_with_contract(binary(), blockchain_contract:t()) ->
%%     {ok, t()} | {error, error() | blockchain_contract:error()}.
%% from_bin_with_contract(<<Bin/binary>>, Contract) ->
%%     case from_bin(Bin) of
%%         {ok, Term}=Ok ->
%%             case blockchain_contract:check(Term, Contract) of
%%                 ok ->
%%                     Ok;
%%                 {error, _}=Err ->
%%                     Err
%%             end;
%%         {error, _}=Err ->
%%             Err
%%     end.


%% 1       N
%% 131     Data
%% TODO Distribution Header?
-spec envelope(binary()) -> result().
envelope(<<?ETF_VERSION, Rest/binary>>) ->
    case frame(Rest) of
        {ok, {Term, <<>>}} ->
            {ok, Term};
        {ok, {_, <<Rest/binary>>}} ->
            %% TODO Would it make any sense to return OK here?
            %%      Let's say if we concatenated multiple t2b outputs.
            {error, {envelope, {tail_data_remains, Rest}}};
        {error, _}=Err ->
            Err
    end;
envelope(<<Version:8/integer, _/binary>>) ->
    {error, {envelope, {head_version_unsupported, Version, expected, [?ETF_VERSION]}}};
envelope(<<Bin/binary>>) ->
    {error, {envelope, {unmatched, Bin}}}.

%% term:
%% -----
%% 1       N
%% Tag     Data
-spec frame(binary()) -> result_internal().
frame(<<?ETF_TAG_COMPRESSED          , R/binary>>) -> compressed(R);
frame(<<?ETF_TAG_SMALL_INTEGER_EXT   , R/binary>>) -> small_integer_ext(R);
frame(<<?ETF_TAG_INTEGER_EXT         , R/binary>>) -> integer_ext(R);
frame(<<?ETF_TAG_ATOM_EXT            , R/binary>>) -> atom_ext(R);
frame(<<?ETF_TAG_ATOM_UTF8_EXT       , R/binary>>) -> atom_utf8_ext(R);
frame(<<?ETF_TAG_SMALL_ATOM_UTF8_EXT , R/binary>>) -> small_atom_utf8_ext(R);
frame(<<?ETF_TAG_SMALL_TUPLE_EXT     , R/binary>>) -> small_tuple_ext(R);
frame(<<?ETF_TAG_LARGE_TUPLE_EXT     , R/binary>>) -> large_tuple_ext(R);
frame(<<?ETF_TAG_NIL_EXT             , R/binary>>) -> {ok, {[], R}};
frame(<<?ETF_TAG_STRING_EXT          , R/binary>>) -> string_ext(R);
frame(<<?ETF_TAG_LIST_EXT            , R/binary>>) -> list_ext(R);
frame(<<?ETF_TAG_BINARY_EXT          , R/binary>>) -> binary_ext(R);
frame(<<?ETF_TAG_SMALL_BIG_EXT       , R/binary>>) -> small_big_ext(R);
frame(<<?ETF_TAG_LARGE_BIG_EXT       , R/binary>>) -> large_big_ext(R);
frame(<<?ETF_TAG_MAP_EXT             , R/binary>>) -> map_ext(R);
frame(<<?ETF_TAG_NEW_FLOAT_EXT       , R/binary>>) -> new_float_ext(R);
frame(<<?ETF_TAG_FLOAT_EXT           , _/binary>>) -> {error, {frame, 'FLOAT_EXT'          , unsupported}};  % TODO
frame(<<?ETF_TAG_ATOM_CACHE_REF      , _/binary>>) -> {error, {frame, 'ATOM_CACHE_REF'     , unsupported}};  % TODO
frame(<<?ETF_TAG_SMALL_ATOM_EXT      , _/binary>>) -> {error, {frame, 'SMALL_ATOM_EXT'     , unsupported}};  % TODO
frame(<<?ETF_TAG_BIT_BINARY_EXT      , _/binary>>) -> {error, {frame, 'BIT_BINARY_EXT'     , unsupported}};  % TODO
frame(<<?ETF_TAG_PORT_EXT            , _/binary>>) -> {error, {frame, 'PORT_EXT'           , unsupported}};  % TODO
frame(<<?ETF_TAG_NEW_PORT_EXT        , _/binary>>) -> {error, {frame, 'NEW_PORT_EXT'       , unsupported}};  % TODO
frame(<<?ETF_TAG_V4_PORT_EXT         , _/binary>>) -> {error, {frame, 'V4_PORT_EXT'        , unsupported}};  % TODO
frame(<<?ETF_TAG_PID_EXT             , _/binary>>) -> {error, {frame, 'PID_EXT'            , unsupported}};  % TODO
frame(<<?ETF_TAG_NEW_PID_EXT         , _/binary>>) -> {error, {frame, 'NEW_PID_EXT'        , unsupported}};  % TODO
frame(<<?ETF_TAG_REFERENCE_EXT       , _/binary>>) -> {error, {frame, 'REFERENCE_EXT'      , unsupported}};  % TODO
frame(<<?ETF_TAG_NEW_REFERENCE_EXT   , _/binary>>) -> {error, {frame, 'NEW_REFERENCE_EXT'  , unsupported}};  % TODO
frame(<<?ETF_TAG_NEWER_REFERENCE_EXT , _/binary>>) -> {error, {frame, 'NEWER_REFERENCE_EXT', unsupported}};  % TODO
frame(<<?ETF_TAG_FUN_EXT             , _/binary>>) -> {error, {frame, 'FUN_EXT'            , unsupported}};  % TODO
frame(<<?ETF_TAG_NEW_FUN_EXT         , _/binary>>) -> {error, {frame, 'NEW_FUN_EXT'        , unsupported}};  % TODO
frame(<<?ETF_TAG_EXPORT_EXT          , _/binary>>) -> {error, {frame, 'EXPORT_EXT'         , unsupported}};  % TODO
frame(<<Tag:8/integer                , _/binary>>) -> {error, {frame_unrecognized_tag, Tag}};
frame(<<Bin/binary>>                             ) -> {error, {frame_unrecognized, Bin}}.

%% BINARY_EXT
%% 4       Len
%% Len     Data
%%
%% Binaries are generated with bit syntax expression or with
%% erlang:list_to_binary/1, erlang:term_to_binary/1, or as input from binary
%% ports. The Len length field is an unsigned 4 byte integer (big-endian).
-spec binary_ext(binary()) -> result_internal(binary()).
binary_ext(<<Len:32/integer-unsigned-big, Bin:Len/binary, Rest/binary>>) ->
    {ok, {Bin, Rest}};
binary_ext(<<Bin/binary>>) ->
    {error, {frame, 'BINARY_EXT', {unmatched, Bin}}}.

%% MAP_EXT
%% 4       N
%% Arity   Pairs
%%
%% Encodes a map. The Arity field is an unsigned 4 byte integer in big-endian
%% format that determines the number of key-value pairs in the map. Key and
%% value pairs (Ki => Vi) are encoded in section Pairs in the following order:
%% K1, V1, K2, V2,..., Kn, Vn. Duplicate keys are not allowed within the same
%% map.
-spec map_ext(binary()) -> result_internal(#{t() => t()}).
map_ext(<<Arity:32/integer-unsigned-big, Rest0/binary>>) ->
    case map_pairs(Arity, Rest0) of
        {ok, {Pairs, Rest1}} ->
            Term = maps:from_list(Pairs), % TODO Handle errors?
            {ok, {Term, Rest1}};
        {error, _}=Err ->
            Err
    end;
map_ext(<<Bin/binary>>) ->
    {error, {frame, 'MAP_EXT', {unmatched, Bin}}}.

map_pairs(N, <<Rest/binary>>) ->
    map_pairs(N, [], <<Rest/binary>>).

-spec map_pairs(non_neg_integer(), [{t(), t()}], binary()) ->
    result_internal([{t(), t()}]).
map_pairs(0, Pairs, <<Rest/binary>>) ->
    {ok, {Pairs, Rest}};
map_pairs(N, Pairs, <<Rest0/binary>>) ->
    case frame(Rest0) of
        {ok, {Key, Rest1}} ->
            case frame(Rest1) of
                {ok, {Val, Rest2}} ->
                    map_pairs(N - 1, [{Key, Val} | Pairs], Rest2);
                {error, _}=Err ->
                    Err
            end;
        {error, _}=Err ->
            Err
    end.

%% NEW_FLOAT_EXT
%% 8
%% IEEE float
%%
%% A finite float (i.e. not inf, -inf or NaN) is stored as 8 bytes in
%% big-endian IEEE format.
%%
%% This term is used in minor version 1 of the external format.
-spec new_float_ext(binary()) -> result_internal(float()).
new_float_ext(<<N:64/float-big, Rest/binary>>) ->
    {ok, {N, Rest}};
new_float_ext(<<Bin/binary>>) ->
    {error, {frame, 'NEW_FLOAT_EXT', {unmatched, Bin}}}.

%% SMALL_INTEGER_EXT
%% 1
%% Int
-spec small_integer_ext(binary()) -> result_internal(integer()).
small_integer_ext(<<Int/integer, Rest/binary>>) ->
    {ok, {Int, Rest}};
small_integer_ext(<<Bin/binary>>) ->
    {error, {frame, 'SMALL_INTEGER_EXT', {unmatched, Bin}}}.

%% INTEGER_EXT
%% 4
%% Int
%%
%% Signed 32-bit integer in big-endian format.
-spec integer_ext(binary()) -> result_internal(integer()).
integer_ext(<<Int:32/integer-big, Rest/binary>>) ->
    {ok, {Int, Rest}};
integer_ext(<<Bin/binary>>) ->
    {error, {frame, 'INTEGER_EXT', {unmatched, Bin}}}.

%% compressed term:
%% ----------------------------------------
%% 4                   N
%% UncompressedSize    Zlib-compressedData
%%
%% Uncompressed size (unsigned 32-bit integer in big-endian byte order) is the
%% size of the data before it was compressed. The compressed data has the
%% following format when it has been expanded:
%% 1    Uncompressed Size
%% Tag  Data
%%
-spec compressed(binary()) -> result_internal().
compressed(<<UncompressedSize:32/integer-unsigned-big, ZlibCompressedData/binary>>) ->
    % TODO More kinds of errors? Exceptions?
    case zlib:uncompress(ZlibCompressedData) of
        <<Data:UncompressedSize/binary>> ->
            frame(Data);
        <<Data/binary>> ->
            {error,
                {uncompressed_data_bad_size,
                    {expected, UncompressedSize, actual, bit_size(Data)}}} % TODO or byte?
    end;
compressed(<<Bin/binary>>) ->
    {error, {frame, compressed, {unmatched, Bin}}}.

%% LIST_EXT
%% 4
%% Length  Elements    Tail
-spec list_ext(binary()) -> result_internal(maybe_improper_list(t(), t())).
list_ext(<<Len:32/integer-unsigned-big, Rest0/binary>>) ->
    case list_elements(Len, Rest0) of
        {ok, {Elements, Rest1}} ->
            case frame(Rest1) of
                {ok, {Tail, Rest2}} ->
                    R = Rest2,
                    case {Elements, Tail} of
                        {[], []} -> {ok, {[], R}};
                        {_ , []} -> {ok, {Elements, R}};
                        {[], _ } -> {error, {frame, 'LIST_EXT', {unsound, list_empty_with_non_nil_tail}}};
                        {_ , _ } -> {ok, {list_improper(Elements ++ [Tail]), R}}
                    end;
                {error, _}=Err ->
                    Err
            end;
        {error, _}=Err ->
            Err
    end;
list_ext(<<Bin/binary>>) ->
    {error, {frame, 'LIST_EXT', {unmatched, Bin}}}.

list_elements(N, <<Rest/binary>>) ->
    list_elements(N, <<Rest/binary>>, []).

-spec list_elements(non_neg_integer(), binary(), [t()]) ->
    result_internal([t()]).
list_elements(0, <<Rest/binary>>, Xs) ->
    {ok, {lists:reverse(Xs), Rest}};
list_elements(N, <<Rest0/binary>>, Xs) ->
    case frame(Rest0) of
        {ok, {X, Rest1}} ->
            list_elements(N - 1, Rest1, [X | Xs]);
        {error, _}=Err ->
            Err
    end.

%% XXX Caller must ensure at least 2 elements!
-spec list_improper(nonempty_list(A | B)) -> nonempty_improper_list(A, B) | A | B.
list_improper([X]) -> X;
list_improper([X | Xs]) -> [X | list_improper(Xs)].

%% SMALL_TUPLE_EXT
%% 1       N
%% Arity   Elements
%%
%% Encodes a tuple. The Arity field is an unsigned byte that determines how
%% many elements that follows in section Elements.
-spec small_tuple_ext(binary()) -> result_internal(tuple()).
small_tuple_ext(<<Arity:8/integer-unsigned, Rest/binary>>) ->
    tuple_ext(Arity, Rest);
small_tuple_ext(<<Bin/binary>>) ->
    {error, {frame, 'SMALL_TUPLE_EXT', {unmatched, Bin}}}.

%% LARGE_TUPLE_EXT
%% 4       N
%% Arity   Elements
%%
%% Same as SMALL_TUPLE_EXT except that Arity is an unsigned 4 byte integer in
%% big-endian format.
-spec large_tuple_ext(binary()) -> result_internal(tuple()).
large_tuple_ext(<<Arity:32/integer-unsigned-big, Rest/binary>>) ->
    tuple_ext(Arity, Rest);
large_tuple_ext(<<Bin/binary>>) ->
    {error, {frame, 'LARGE_TUPLE_EXT', {unmatched, Bin}}}.

-spec tuple_ext(non_neg_integer(), binary()) -> result_internal(tuple()).
tuple_ext(Arity, <<Rest0/binary>>) ->
    case list_elements(Arity, Rest0) of
        {ok, {Elements, Rest1}} ->
            {ok, {list_to_tuple(Elements), Rest1}};
        {error, _}=Err ->
            Err
    end.

%% BEGIN atoms ================================================================

%% Encoding atoms
%% --------------
%%
%% > As from ERTS 9.0 (OTP 20), atoms may contain any Unicode characters.
%% >
%% > Atoms sent over node distribution are always encoded in UTF-8 using
%% > either ATOM_UTF8_EXT, SMALL_ATOM_UTF8_EXT or ATOM_CACHE_REF.
%% >
%% > Atoms encoded with erlang:term_to_binary/1,2 or
%% > erlang:term_to_iovec/1,2 are by default still using the old
%% > deprecated Latin-1 format ATOM_EXT for atoms that only contain
%% > Latin-1 characters (Unicode code points 0-255). Atoms with higher
%% > code points will be encoded in UTF-8 using either ATOM_UTF8_EXT or
%% > SMALL_ATOM_UTF8_EXT.
%% >
%% > The maximum number of allowed characters in an atom is 255. In the
%% > UTF-8 case, each character can need 4 bytes to be encoded.

%% SMALL_ATOM_UTF8_EXT
%% 1       Len
%% Len     AtomName
%%
%% An atom is stored with a 1 byte unsigned length, followed by Len
%% bytes containing the AtomName encoded in UTF-8. Longer atoms encoded
%% in UTF-8 can be represented using ATOM_UTF8_EXT.

-spec small_atom_utf8_ext(binary()) -> result_internal(atom()).
small_atom_utf8_ext(<<Len:8/integer-unsigned, AtomName:Len/binary, Rest/binary>>) ->
    bin_to_atom('SMALL_ATOM_UTF8_EXT', AtomName, Rest);
small_atom_utf8_ext(<<Bin/binary>>) ->
    {error, {frame, 'SMALL_ATOM_UTF8_EXT', {unmatched, Bin}}}.

%% ATOM_UTF8_EXT
%% 2       Len
%% Len     AtomName
%%
%% An atom is stored with a 2 byte unsigned length in big-endian order,
%% followed by Len bytes containing the AtomName encoded in UTF-8.
-spec atom_utf8_ext(binary()) -> result_internal(atom()).
atom_utf8_ext(<<Len:16/integer-unsigned, AtomName:Len/binary, Rest/binary>>) ->
    bin_to_atom('ATOM_UTF8_EXT', AtomName, Rest);
atom_utf8_ext(<<Bin/binary>>) ->
    {error, {frame, 'ATOM_UTF8_EXT', {unmatched, Bin}}}.

%% ATOM_EXT (deprecated):
%% ---------------------
%% 2 	Len
%% Len 	AtomName
%%
%% An atom is stored with a 2 byte unsigned length in big-endian order,
%% followed by Len numbers of 8-bit Latin-1 characters that forms the AtomName.
%% The maximum allowed value for Len is 255.
-spec atom_ext(binary()) -> result_internal(atom()).
atom_ext(<<Len:16/integer-unsigned-big, AtomName:Len/binary, Rest/binary>>) ->
    bin_to_atom('ATOM_EXT', AtomName, Rest);
atom_ext(<<Bin/binary>>) ->
    {error, {frame, 'ATOM_EXT', {unmatched, Bin}}}.

-spec bin_to_atom(frame(), binary(), binary()) -> result_internal(atom()).
bin_to_atom(F, <<AtomName/binary>>, <<Rest/binary>>) ->
    try
        {ok, {list_to_atom(unicode:characters_to_list(AtomName)), Rest}}
    catch
        error:badarg ->
            {error, {frame, F, {unsound, atom_characters_invalid}}};
        error:system_limit ->
            {error, {frame, F, {unsound, atom_length_exceeds_system_limit}}}
    end.

%% END atoms ==================================================================

%% STRING_EXT
%% 2       Len
%% Length  Characters
%%
%% String does not have a corresponding Erlang representation, but is an
%% optimization for sending lists of bytes (integer in the range 0-255) more
%% efficiently over the distribution. As field Length is an unsigned 2 byte
%% integer (big-endian), implementations must ensure that lists longer than
%% 65535 elements are encoded as LIST_EXT.
-spec string_ext(binary()) -> result_internal(string()).
string_ext(<<Len:16/integer-unsigned-big, Chars:Len/binary, Rest/binary>>) ->
    {ok, {binary_to_list(Chars), Rest}};
string_ext(<<Bin/binary>>) ->
    {error, {frame, 'STRING_EXT', {unmatched, Bin}}}.


%% SMALL_BIG_EXT
%% 1   1       n
%% n   Sign    d(0) ... d(n-1)
%%
%% Bignums are stored in unary form with a Sign byte, that is, 0 if the bignum
%% is positive and 1 if it is negative. The digits are stored with the least
%% significant byte stored first. To calculate the integer, the following
%% formula can be used:
%%
%% B = 256
%% (d0*B^0 + d1*B^1 + d2*B^2 + ... d(N-1)*B^(n-1))
-spec small_big_ext(binary()) -> result_internal(integer()).
small_big_ext(<<N:8/integer-unsigned, Sign:8/integer-unsigned, Data:N/binary, Rest/binary>>) ->
    big_ext('SMALL_BIG_EXT', Sign, Data, <<Rest/binary>>);
small_big_ext(<<Bin/binary>>) ->
    {error, {frame, 'SMALL_BIG_EXT', {unmatched, Bin}}}.

%% LARGE_BIG_EXT
%% 4   1       n
%% n   Sign    d(0) ... d(n-1)
%%
%% Same as SMALL_BIG_EXT except that the length field is an unsigned 4 byte
%% integer.
-spec large_big_ext(binary()) -> result_internal(integer()).
large_big_ext(<<N:32/integer-unsigned, Sign:8/integer-unsigned, Data:N/binary, Rest/binary>>) ->
    big_ext('LARGE_BIG_EXT', Sign, Data, <<Rest/binary>>);
large_big_ext(<<Bin/binary>>) ->
    {error, {frame, 'LARGE_BIG_EXT', {unmatched, Bin}}}.

-spec big_ext(frame(), 0 | 1, binary(), binary()) ->
    result_internal(integer()).
big_ext(F, Sign, <<Data/binary>>, <<Rest/binary>>) ->
    case big_int_sign_to_multiplier(F, Sign) of
        {ok, Multiplier} ->
            case big_int_data(F, Data) of
                {ok, Int} ->
                    Term = Int * Multiplier,
                    {ok, {Term, Rest}};
                {error, _}=Err ->
                    Err
            end;
        {error, _}=Err ->
            Err
    end.

-spec big_int_sign_to_multiplier(frame(), integer()) -> result(1 | -1).
big_int_sign_to_multiplier(_, 0) -> {ok,  1};
big_int_sign_to_multiplier(_, 1) -> {ok, -1};
big_int_sign_to_multiplier(F, S) -> {error, {frame, F, {unsound, {sign_invalid, S}}}}.

big_int_data(F, Data) ->
    big_int_data(F, Data, 0, 0).

-spec big_int_data(frame(), binary(), integer(), integer()) -> result(integer()).
big_int_data(_, <<>>, _, Int) ->
    {ok, Int};
big_int_data(F, <<B:8/integer, Rest/binary>>, Pos, Int) ->
    big_int_data(F, Rest, Pos + 1, Int + (B bsl (8 * Pos)));
big_int_data(F, <<Bin/binary>>, _, _) ->
    {error, {frame, F, {unsound, {big_int_data_of_bad_size, Bin}}}}.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%% TODO Individual term test cases that check both ok and error results.
%% TODO quickcheck/proper

supported_term_test_() ->
    [
        {
            lists:flatten(io_lib:format("Opts:~p Term: ~p", [Opts, Term])),
            ?_assertEqual({ok, Term}, from_bin(term_to_binary(Term)))
        }
    ||
        Term <- [
            %% ATOM_EXT
            a,
            b,
            ab,
            abcdefghijklmnopqrstuvwxyz,
            aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa,

            %% SMALL_ATOM_UTF8_EXT
            'λ',
            'λαμβδα',
            'ламбда',

            %% ATOM_UTF8_EXT
            list_to_atom(lists:duplicate(255, $λ)),
            list_to_atom(
                %% XXX Without the following redundant list dance - erlc crashes,
                %%     for mysterious reasons, with an
                %%     "internal error in beam_asm".
                lists:flatten(
                    [
                        "У лукоморья дуб зелёный;"
                        "Златая цепь на дубе том:"
                        "И днём и ночью кот учёный"
                        "Всё ходит по цепи кругом;"
                        "Идёт направо - песнь заводит,"
                        "Налево - сказку говорит."
                        "Там чудеса: там леший бродит"
                        "Русалка на ветвях сидит;"
                        "Там на неведомых дорожках"
                        "Следы невиданных зверей;"
                    ]
                )
            ),

            [],
            "",
            1.1,
            0,
            "a",
            "abcdefghijklmnopqrstuvwxyz",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            lists:seq(0, 100),
            lists:seq(0, 1000),
            [500],
            9999999999999999999999999999999999999999999999999999999999999999999,
            -9999999999999999999999999999999999999999999999999999999999999999999,
            [a | b],
            [1000, 2000, 3000, 4000 | improper_list_tail],
            [foo],
            ["foo"],

            %% TODO A non-hand-wavy LARGE_BIG_EXT
            ceil(math:pow(10, 300)) * ceil(math:pow(10, 300)) * ceil(math:pow(10, 300)),

            {},
            {1, 2, 3},
            [{}],
            [{k, v}],
            [{"k", "v"}],
            [<<"foo">>],
            [{<<"k">>, <<"v">>}],
            #{a => 1},
            #{1 => a},
            #{0 => 1},
            #{k => v},
            #{"k" => "v"},
            #{<<"k">> => <<"v">>},
            #{k1 => #{k2 => #{k3 => hi}}}
        ],
        Opts <- [[], [compressed] | [[{compressed, N}] || N <- lists:seq(0, 9)]]
    ].

unsupported_term_test_() ->
    [
        {
            lists:flatten(io_lib:format("Opts:~p Term: ~p", [Opts, Term])),
            ?_assertMatch({error, {frame, _, unsupported}}, from_bin(term_to_binary(Term)))
        }
    ||
        Term <- [
            make_ref(),
            fun() -> foo end,
            hd(erlang:ports()), % TODO misc port versions?
            self(),
            fun ?MODULE:from_bin/1
            %% TODO ATOM_CACHE_REF
            %% TODO BIT_BINARY_EXT
        ],
        Opts <- [[], [compressed] | [[{compressed, N}] || N <- lists:seq(0, 9)]]
    ].

-endif.
