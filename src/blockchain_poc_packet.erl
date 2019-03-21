%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain PoC Path ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_poc_packet).

-export([build/3, decrypt/2]).

-spec decrypt(Packet :: binary(), ECDHFun :: libp2p_crypto:ecdh_fun()) -> error | {Payload :: binary(), NextLayer :: binary()}.
decrypt(<<IV:12/binary, OnionCompactKey:33/binary, Tag:4/binary, CipherText/binary>>, ECDHFun) ->
    PubKey = libp2p_crypto:bin_to_pubkey(OnionCompactKey),
    SharedKey = ECDHFun(PubKey),
    case crypto:block_decrypt(aes_gcm, SharedKey, IV, {IV, CipherText, Tag}) of
        <<DataSize:8/integer, Data:DataSize/binary, Rest/binary>> ->
            Padding = binary:part(crypto:hash(sha512, Data), 0, DataSize+1+4),
            {<<DataSize:8/integer, Data/binary>>, <<IV/binary, OnionCompactKey/binary, Rest/binary, Padding/binary>>};
        _ ->
            error
    end.

-spec build(OnionKey :: libp2p_crypto:key_map(), IV :: <<_:96>>, KeysAndData :: [{libp2p_crypto:pubkey_bin(), binary()}, ...]) -> Layers :: [binary()].
build(#{secret := OnionPrivKey, public := OnionPubKey}, IV, PubKeysAndData) ->
    ECDHFun = libp2p_crypto:mk_ecdh_fun(OnionPrivKey),
    OnionCompactKey = libp2p_crypto:pubkey_to_bin(OnionPubKey),
    N = length(PubKeysAndData),

    MatrixLength = N*(N+1),
    EntryMatrix = list_to_tuple([undefined || _ <- lists:seq(1, MatrixLength)]),

    %% fill in the data cells
    DataMatrix = lists:foldl(fun({Row, Col=1}, Acc) ->
                                     %% For column 1, the value is the Payload for that row
                                     setelement(((Row-1)*N)+Col, Acc, element(2, lists:nth(Row, PubKeysAndData)));
                                ({Row, Col}, Acc) ->
                                     %% For other columns, the value is (Row+1, Column -1) ^ Key(Row+1)
                                     setelement(((Row-1)*N)+Col, Acc, encrypt_cell(Row+1, Col-1, N, Acc, OnionCompactKey, ECDHFun, IV, PubKeysAndData))
                             end, EntryMatrix, lists:reverse(lists:sort([ {X, Y} || X <- lists:seq(1, N+1), Y <- lists:seq(1, N), X+Y =< N+1])) ),

    %% fill in the padding cells
    PaddingMatrix = lists:foldl(fun({Row, Col}, Acc) when Col == N ->
                                        %% For column N, the value is Hash(Row-1, 1) ^ Key(Row)
                                        <<Size:8/integer, Data/binary>> = element(2, lists:nth(Row-1, PubKeysAndData)),
                                        DataSize = Size + 1 + 4,
                                        <<Hash:DataSize/binary, _/binary>> = crypto:hash(sha512, Data),
                                        case Row > N of
                                            false ->
                                                ExtraTagBytes = ((N-Row)*4),
                                                TempMatrix = setelement(((Row-1)*N)+Col, Acc, <<0:(ExtraTagBytes*8)/integer, Hash/binary>>),
                                                <<_:ExtraTagBytes/binary, Cell/binary>> = encrypt_cell(Row, Col, N, TempMatrix, OnionCompactKey, ECDHFun, IV, PubKeysAndData),
                                                setelement(((Row-1)*N)+Col, Acc, Cell);
                                            true ->
                                                setelement(((Row-1)*N)+Col, Acc, Hash)
                                        end;
                                   ({Row, Col}, Acc) ->
                                        %% For column < N, the value is (Row-1, Column +1) ^ Key(Row)
                                        Data = element(((Row-2)*N) + (Col+1), Acc),
                                        case Row > N of
                                            false ->
                                                ExtraTagBytes = ((N-Row)*4),
                                                TempMatrix = setelement(((Row-1)*N)+Col, Acc, <<0:(ExtraTagBytes*8)/integer, Data/binary>>),
                                                <<_:ExtraTagBytes/binary, Cell/binary>>= encrypt_cell(Row, Col, N, TempMatrix, OnionCompactKey, ECDHFun, IV, PubKeysAndData),
                                                setelement(((Row-1)*N)+Col, Acc, Cell);
                                            true ->
                                                setelement(((Row-1)*N)+Col, Acc, Data)
                                        end
                                end, DataMatrix, lists:sort([ {X, Y} || X <- lists:seq(1, N+1), Y <- lists:seq(1, N), X+Y > N+1])),

    %% now we need to re-encrypt the data cells now we have the padding in place, row by row, removing the padding bytes from the previous row
    %% and propogating the tags upwards
    EncryptedMatrix = lists:foldl(fun(R, Acc) ->
                                          PaddingSize = byte_size(element(2, lists:nth(R, PubKeysAndData))) + (4 * 1),
                                          Row = encrypt_row(R, N, Acc, OnionCompactKey, ECDHFun, IV, PubKeysAndData),
                                          TAcc = setelement(((R-2)*N)+2, Acc, binary:part(Row, 0, byte_size(Row) - PaddingSize)),
                                          lists:foldl(fun(E, Acc2) ->
                                                              %% zero out all the other columns but 1 and 2 for this row
                                                              setelement(((R-2)*N)+E, Acc2, <<>>)
                                                      end, TAcc, lists:seq(3, N))
                                  end, setelement(N, PaddingMatrix, <<>>), lists:reverse(lists:seq(2,N))),

    lists:map(fun(RowNumber) ->
                      case RowNumber > N of
                          true ->
                              %% the last row is all padding
                              Bins = lists:sublist(tuple_to_list(EncryptedMatrix), ((RowNumber-1)*N)+1, N),
                              list_to_binary(Bins);
                          false ->
                              encrypt_row(RowNumber, N, EncryptedMatrix, OnionCompactKey, ECDHFun, IV, PubKeysAndData)
                      end
              end, lists:seq(1, N+1)).

%% internal functions

encrypt_cell(Row, Column, N, Matrix, OnionCompactKey, ECDHFun, IV, KeysAndData) ->
    SecretKey = ECDHFun(element(1, lists:nth(Row, KeysAndData))),
    Bins = lists:sublist(tuple_to_list(Matrix), ((Row-1)*N)+1, Column),
    Offset = lists:sum([byte_size(X) || X <- Bins]) - byte_size(lists:last(Bins)),
    {CipherText, _Tag} = crypto:block_encrypt(aes_gcm,
                                             SecretKey,
                                             IV, {<<IV/binary, OnionCompactKey/binary>>,
                                                  list_to_binary(Bins), 4}),
    << _:Offset/binary, Cell/binary>> = CipherText,
    Cell.

encrypt_row(Row, N, Matrix, OnionCompactKey, ECDHFun, IV, KeysAndData) ->
    SecretKey = ECDHFun(element(1, lists:nth(Row, KeysAndData))),
    Bins = lists:sublist(tuple_to_list(Matrix), ((Row-1)*N)+1, N),
    {CipherText, Tag} = crypto:block_encrypt(aes_gcm,
                                             SecretKey,
                                             IV, {<<IV/binary, OnionCompactKey/binary>>,
                                                  list_to_binary(Bins), 4}),
    <<Tag/binary, CipherText/binary>>.
