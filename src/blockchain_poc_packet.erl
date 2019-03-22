%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain PoC Path ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_poc_packet).

-export([build/3, decrypt/2]).

%% @doc A module for constructing a v2 onion packet.
%%
%% Onion packets are nested encrypted packets that have 4 important properties:
%%
%% * All layers are the same size
%% * No decrypter knows how many layers remain
%% * The padding added at each layer is deterministic
%% * No decryptor knows the target of the next layer
%%
%% The outermost packet looks like this:
%% <<IV:12/binary, PublicKey:33/binary, Tag:4/binary, CipherText/binary>>
%%
%% The authenticated data is the IV and the public key. The tag is the AES-GCM message
%% authentication code.
%%
%% After decryption the plaintext looks like this:
%% <<Length:8/integer, Data:Length/binary, InnerLayer/binary>>
%%
%% The decryptor then appends the first Length+1 bytes of the SHA512
%% of the Data field. The new packet thus looks like this:
%%
%% <<IV:12/binary, PublicKey:33/binary, NextTag/binary, CipherText/binary, Padding/binary>>
%%
%% Thus the next decryptor sees an identical length packet which it can decrypt in the same way.
%%
%% At the end of the packet, the final layer is entirely padding and cannot be decrypted.

%% TODO explain the packet construction

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-spec decrypt(Packet :: binary(), ECDHFun :: libp2p_crypto:ecdh_fun()) -> error | {Payload :: binary(), NextLayer :: binary()}.
decrypt(<<IV:12/binary, OnionCompactKey:33/binary, Tag:4/binary, CipherText/binary>>, ECDHFun) ->
    PubKey = libp2p_crypto:bin_to_pubkey(OnionCompactKey),
    SharedKey = ECDHFun(PubKey),
    case crypto:block_decrypt(aes_gcm, SharedKey, IV, {<<IV/binary, OnionCompactKey/binary>>,
                                                       CipherText, Tag}) of
        <<DataSize:8/integer, Data:DataSize/binary, Rest/binary>> ->
            Padding = binary:part(crypto:hash(sha512, Data), 0, DataSize+1+4),
            {<<Data/binary>>, <<IV/binary, OnionCompactKey/binary, Rest/binary, Padding/binary>>};
        _ ->
            error
    end.

-spec build(OnionKey :: libp2p_crypto:key_map(), IV :: <<_:96>>, KeysAndData :: [{libp2p_crypto:pubkey_bin(), binary()}, ...]) -> {OuterLayer :: binary, Layers :: [binary()]}.
build(#{secret := OnionPrivKey, public := OnionPubKey}, IV, PubKeysAndData) ->
    ECDHFun = libp2p_crypto:mk_ecdh_fun(OnionPrivKey),
    OnionCompactKey = libp2p_crypto:pubkey_to_bin(OnionPubKey),
    N = length(PubKeysAndData),

    MatrixLength = N*(N+1),
    EntryMatrix = list_to_tuple([undefined || _ <- lists:seq(1, MatrixLength)]),

    %% fill in the data cells
    DataMatrix = lists:foldl(fun({Row, Col=1}, Acc) ->
                                     %% For column 1, the value is the Payload for that row
                                     Data = element(2, lists:nth(Row, PubKeysAndData)),
                                     DataSize = byte_size(Data),
                                     setelement(((Row-1)*N)+Col, Acc, <<DataSize:8/integer, Data/binary>>);
                                ({Row, Col}, Acc) ->
                                     %% For other columns, the value is (Row+1, Column -1) ^ Key(Row+1)
                                     setelement(((Row-1)*N)+Col, Acc, encrypt_cell(Row+1, Col-1, N, Acc, OnionCompactKey, ECDHFun, IV, PubKeysAndData))
                             end, EntryMatrix, lists:reverse(lists:sort([ {X, Y} || X <- lists:seq(1, N+1), Y <- lists:seq(1, N), X+Y =< N+1])) ),

    %% fill in the padding cells
    PaddingMatrix = lists:foldl(fun({Row, Col}, Acc) when Col == N ->
                                        %% For column N, the value is Hash(Row-1, 1) ^ Key(Row)
                                        Data = element(2, lists:nth(Row-1, PubKeysAndData)),
                                        DataSize = byte_size(Data) + 1 + 4,
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
                                          PaddingSize = byte_size(element(2, lists:nth(R, PubKeysAndData))) + 5,
                                          Row = encrypt_row(R, N, Acc, OnionCompactKey, ECDHFun, IV, PubKeysAndData),
                                          TAcc = setelement(((R-2)*N)+2, Acc, binary:part(Row, 0, byte_size(Row) - PaddingSize)),
                                          lists:foldl(fun(E, Acc2) ->
                                                              %% zero out all the other columns but 1 and 2 for this row
                                                              setelement(((R-2)*N)+E, Acc2, <<>>)
                                                      end, TAcc, lists:seq(3, N))
                                  end, setelement(N, PaddingMatrix, <<>>), lists:reverse(lists:seq(2,N))),

    [FirstRow|_] = PacketRows = lists:map(fun(RowNumber) ->
                                                  case RowNumber > N of
                                                      true ->
                                                          %% the last row is all padding
                                                          Bins = lists:sublist(tuple_to_list(EncryptedMatrix), ((RowNumber-1)*N)+1, N),
                                                          list_to_binary([IV, OnionCompactKey, Bins]);
                                                      false ->
                                                          encrypt_row(RowNumber, N, EncryptedMatrix, OnionCompactKey, ECDHFun, IV, PubKeysAndData)
                                                  end
                                          end, lists:seq(1, N+1)),
    {<<IV/binary, OnionCompactKey/binary, FirstRow/binary>>, PacketRows}.


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

-ifdef(TEST).

encrypt_decrypt_test() ->
    #{secret := PrivKey1, public := PubKey1} = libp2p_crypto:generate_keys(ecc_compact),
    #{secret := PrivKey2, public := PubKey2} = libp2p_crypto:generate_keys(ecc_compact),
    #{secret := PrivKey3, public := PubKey3} = libp2p_crypto:generate_keys(ecc_compact),
    #{secret := PrivKey4, public := PubKey4} = libp2p_crypto:generate_keys(ecc_compact),
    #{secret := PrivKey5, public := PubKey5} = libp2p_crypto:generate_keys(ecc_compact),
    #{secret := PrivKey6, public := PubKey6} = libp2p_crypto:generate_keys(ecc_compact),
    #{secret := PrivKey7, public := PubKey7} = libp2p_crypto:generate_keys(ecc_compact),
    #{secret := PrivKey8, public := PubKey8} = libp2p_crypto:generate_keys(ecc_compact),

    OnionKey = libp2p_crypto:generate_keys(ecc_compact),

    PubKeys = [PubKey1, PubKey2, PubKey3, PubKey4, PubKey5, PubKey6, PubKey7, PubKey8],
    PrivKeys = [PrivKey1, PrivKey2, PrivKey3, PrivKey4, PrivKey5, PrivKey6, PrivKey7, PrivKey8],

    LayerData = [<<"abc">>, <<"def">>, <<"ghi">>, <<"jhk">>, <<"lmn">>, <<"opq">>, <<"rst">>, <<"uvw">>],

    KeysAndData = lists:zip(PubKeys, LayerData),

    IV = crypto:strong_rand_bytes(12),
    {OuterPacket, Rows} = build(OnionKey, IV, KeysAndData),



    #{secret := PrivOnionKey, public := PubOnionKey} = OnionKey,

    ECDHFun1 = libp2p_crypto:mk_ecdh_fun(PrivKey1),
    ECDHFun2 = libp2p_crypto:mk_ecdh_fun(PrivOnionKey),
    SecretKey1 = ECDHFun1(PubOnionKey),
    SecretKey2 = ECDHFun2(PubKey1),
    ?assertEqual(SecretKey1, SecretKey2),
    {<<"abc">>, Remainder1} = decrypt(OuterPacket, libp2p_crypto:mk_ecdh_fun(PrivKey1)),
    ?assert(lists:all(fun(E) -> E == error end, [ decrypt(OuterPacket, libp2p_crypto:mk_ecdh_fun(PK)) || PK <- PrivKeys -- [PrivKey1]])),
    OnionCompactKey = libp2p_crypto:pubkey_to_bin(PubOnionKey),
    <<IV:12/binary, OnionCompactKey:33/binary, _Rest/binary>> = Remainder1,
    {<<"def">>, Remainder2} = decrypt(Remainder1, libp2p_crypto:mk_ecdh_fun(PrivKey2)),
    ?assert(lists:all(fun(E) -> E == error end, [ decrypt(Remainder1, libp2p_crypto:mk_ecdh_fun(PK)) || PK <- PrivKeys -- [PrivKey2]])),
    {<<"ghi">>, Remainder3} = decrypt(Remainder2, libp2p_crypto:mk_ecdh_fun(PrivKey3)),
    ?assert(lists:all(fun(E) -> E == error end, [ decrypt(Remainder2, libp2p_crypto:mk_ecdh_fun(PK)) || PK <- PrivKeys -- [PrivKey3]])),
    {<<"jhk">>, Remainder4} = decrypt(Remainder3, libp2p_crypto:mk_ecdh_fun(PrivKey4)),
    ?assert(lists:all(fun(E) -> E == error end, [ decrypt(Remainder3, libp2p_crypto:mk_ecdh_fun(PK)) || PK <- PrivKeys -- [PrivKey4]])),
    {<<"lmn">>, Remainder5} = decrypt(Remainder4, libp2p_crypto:mk_ecdh_fun(PrivKey5)),
    ?assert(lists:all(fun(E) -> E == error end, [ decrypt(Remainder4, libp2p_crypto:mk_ecdh_fun(PK)) || PK <- PrivKeys -- [PrivKey5]])),
    {<<"opq">>, Remainder6} = decrypt(Remainder5, libp2p_crypto:mk_ecdh_fun(PrivKey6)),
    ?assert(lists:all(fun(E) -> E == error end, [ decrypt(Remainder5, libp2p_crypto:mk_ecdh_fun(PK)) || PK <- PrivKeys -- [PrivKey6]])),
    {<<"rst">>, Remainder7} = decrypt(Remainder6, libp2p_crypto:mk_ecdh_fun(PrivKey7)),
    ?assert(lists:all(fun(E) -> E == error end, [ decrypt(Remainder6, libp2p_crypto:mk_ecdh_fun(PK)) || PK <- PrivKeys -- [PrivKey7]])),
    {<<"uvw">>, Remainder8} = decrypt(Remainder7, libp2p_crypto:mk_ecdh_fun(PrivKey8)),
    ?assert(lists:all(fun(E) -> E == error end, [ decrypt(Remainder7, libp2p_crypto:mk_ecdh_fun(PK)) || PK <- PrivKeys -- [PrivKey8]])),
    ?assertEqual(Remainder8, lists:last(Rows)),
    %% check all packets are the same length
    ?assertEqual(1, length(lists:usort([ byte_size(B) || B <- [OuterPacket, Remainder1, Remainder2, Remainder3, Remainder4, Remainder5, Remainder6, Remainder7, Remainder8]]))),
    ok.

-endif.
