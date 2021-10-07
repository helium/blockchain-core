%%%-------------------------------------------------------------------
%% @doc
%% == Blockchain PoC Packet ==
%% @end
%%%-------------------------------------------------------------------
-module(blockchain_poc_packet).

-export([build/5, decrypt/4]).

-include("blockchain_vars.hrl").

-if(?OTP_RELEASE > 22).
%% Ericsson why do you hate us so?
-define(ENCRYPT(Key, IV, AAD, PlainText, TagLength), crypto:crypto_one_time_aead(aes_256_gcm, Key, IV, PlainText, AAD, TagLength, true)).
-define(DECRYPT(Key, IV, AAD, CipherText, Tag), crypto:crypto_one_time_aead(aes_256_gcm, Key, IV, CipherText, AAD, Tag, false)).
-else.
-define(ENCRYPT(Key, IV, AAD, PlainText, TagLength), crypto:block_encrypt(aes_gcm, Key, IV, {AAD, PlainText, TagLength})).
-define(DECRYPT(Key, IV, AAD, CipherText, Tag), crypto:block_decrypt(aes_gcm, Key, IV, {AAD, CipherText, Tag})).
-endif.

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
%% <<IV:16/integer-little, PublicKey:33/binary, Tag:4/binary, CipherText/binary>>
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
%% <<IV:16/integer-little, PublicKey:33/binary, NextTag/binary, CipherText/binary, Padding/binary>>
%%
%% Thus the next decryptor sees an identical length packet which it can decrypt in the same way.
%%
%% At the end of the packet, the final layer is entirely padding and cannot be decrypted.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% @doc Attempt to decrypt the outer layer of a PoC onion packet.
%% If the decryption was sucessfull, return the per-layer data and the next layer of the onion packet, with padding applied.
%% If the decryption fails, return `error'.
-spec decrypt(Packet :: binary(), ECDHFun :: libp2p_crypto:ecdh_fun(), BlockHash :: binary(), Ledger :: blockchain_ledger_v1:ledger()) -> error | {Payload :: binary(), NextLayer :: binary()}.
decrypt(<<IV0:16/integer-unsigned-little, OnionCompactKey:33/binary, Tag:4/binary, CipherText/binary>>, ECDHFun, BlockHash, Ledger) ->
    try libp2p_crypto:bin_to_pubkey(OnionCompactKey) of
        PubKey ->
            SecretKey = ECDHFun(PubKey),
            IV = <<0:80/integer, IV0:16/integer-unsigned-little>>,
            BlockKey = block_key(SecretKey, BlockHash, Ledger),
            case ?DECRYPT(BlockKey, IV, <<IV/binary, OnionCompactKey/binary>>,
                          CipherText, Tag) of
                <<DataSize:8/integer, Data:DataSize/binary, Rest/binary>> ->
                    PaddingSize = DataSize +5,
                    <<Padding:PaddingSize/binary, Xor:16/integer-unsigned-little, _/binary>> = crypto:hash(sha512, Data),
                    NextIV = <<((IV0 bxor Xor) band 16#ffff):16/integer-unsigned-little>>,
                    {<<Data/binary>>, <<NextIV/binary, OnionCompactKey/binary, Rest/binary, Padding/binary>>};
                _ ->
                    error
            end
    catch error:enotsup ->
              %% corrupted or invalid key
              error
    end.

%% @doc Construct a PoC onion packet.
%% The packets are encrypted for each layer's public key using an ECDH exchange with the private key of the ephemeral onion key.
%% All the layer data should be the same size. The general overhead of the packet is 33+2 + (5 * LayerCount) in addition to the size of all the
%% layer data fields. The IV should be a random 16 bit number. The IV will change for each layer (although this is not strictly necessary).
-spec build(OnionKey :: libp2p_crypto:key_map(),
            IV :: non_neg_integer(),
            KeysAndData :: [{libp2p_crypto:pubkey(), binary()}, ...],
            BlockHash :: binary(),
            Ledger :: blockchain_ledger_v1:ledger()) -> {OuterLayer :: binary(), Layers :: [binary()]}.
build(#{secret := OnionPrivKey, public := OnionPubKey}, IV, PubKeysAndData, BlockHash, Ledger) ->
    ECDHFun = libp2p_crypto:mk_ecdh_fun(OnionPrivKey),
    OnionCompactKey = libp2p_crypto:pubkey_to_bin(OnionPubKey),
    N = length(PubKeysAndData),

    {Keys, Data} = lists:unzip(PubKeysAndData),

    ECDHKeys = lists:map(fun(Key) -> ECDHFun(Key) end, Keys),

    IVs = compute_ivs(IV, PubKeysAndData),

    MatrixLength = N*(N+1),
    EntryMatrix = list_to_tuple([undefined || _ <- lists:seq(1, MatrixLength)]),

    %% TODO document the packet construction

    %% fill in the data cells
    DataMatrix = lists:foldl(fun({Row, Col=1}, Acc) ->
                                     %% For column 1, the value is the Payload for that row
                                     CellData = lists:nth(Row, Data),
                                     DataSize = byte_size(CellData),
                                     setelement(((Row-1)*N)+Col, Acc, <<DataSize:8/integer, CellData/binary>>);
                                ({Row, Col}, Acc) ->
                                     %% For other columns, the value is (Row+1, Column -1) ^ Key(Row+1)
                                     setelement(((Row-1)*N)+Col, Acc, encrypt_cell(Row+1, Col-1, N, Acc, OnionCompactKey, ECDHKeys, IVs, BlockHash, Ledger))
                             end, EntryMatrix, lists:reverse(lists:sort([ {X, Y} || X <- lists:seq(1, N+1), Y <- lists:seq(1, N), X+Y =< N+1])) ),

    %% fill in the padding cells
    PaddingMatrix = lists:foldl(fun({Row, Col}, Acc) when Col == N ->
                                        %% For column N, the value is Hash(Row-1, 1) ^ Key(Row)
                                        CellData = lists:nth(Row-1, Data),
                                        DataSize = byte_size(CellData) + 1 + 4,
                                        <<Hash:DataSize/binary, _/binary>> = crypto:hash(sha512, CellData),
                                        case Row > N of
                                            false ->
                                                ExtraTagBytes = ((N-Row)*4),
                                                TempMatrix = setelement(((Row-1)*N)+Col, Acc, <<0:(ExtraTagBytes*8)/integer, Hash/binary>>),
                                                <<_:ExtraTagBytes/binary, Cell/binary>> = encrypt_cell(Row, Col, N, TempMatrix, OnionCompactKey, ECDHKeys, IVs, BlockHash, Ledger),
                                                setelement(((Row-1)*N)+Col, Acc, Cell);
                                            true ->
                                                setelement(((Row-1)*N)+Col, Acc, Hash)
                                        end;
                                   ({Row, Col}, Acc) ->
                                        %% For column < N, the value is (Row-1, Column +1) ^ Key(Row)
                                        CellData = element(((Row-2)*N) + (Col+1), Acc),
                                        case Row > N of
                                            false ->
                                                ExtraTagBytes = ((N-Row)*4),
                                                TempMatrix = setelement(((Row-1)*N)+Col, Acc, <<0:(ExtraTagBytes*8)/integer, CellData/binary>>),
                                                <<_:ExtraTagBytes/binary, Cell/binary>>= encrypt_cell(Row, Col, N, TempMatrix, OnionCompactKey, ECDHKeys, IVs, BlockHash, Ledger),
                                                setelement(((Row-1)*N)+Col, Acc, Cell);
                                            true ->
                                                setelement(((Row-1)*N)+Col, Acc, CellData)
                                        end
                                end, DataMatrix, lists:sort([ {X, Y} || X <- lists:seq(1, N+1), Y <- lists:seq(1, N), X+Y > N+1])),

    %% now we need to re-encrypt the data cells now we have the padding in place, row by row, removing the padding bytes from the previous row
    %% and propogating the tags upwards
    EncryptedMatrix = case N  of
                         1 ->
                              %% for a depth of 1 this step is unnecessary
                             PaddingMatrix;
                         _ ->
                              lists:foldl(fun(R, Acc) ->
                                                  PaddingSize = byte_size(lists:nth(R, Data)) + 5,
                                                  Row = encrypt_row(R, N, Acc, OnionCompactKey, ECDHKeys, IVs, BlockHash, Ledger),
                                                  TAcc = setelement(((R-2)*N)+2, Acc, binary:part(Row, 0, byte_size(Row) - PaddingSize)),
                                                  lists:foldl(fun(E, Acc2) ->
                                                                      %% zero out all the other columns but 1 and 2 for this row
                                                                      setelement(((R-2)*N)+E, Acc2, <<>>)
                                                              end, TAcc, lists:seq(3, N))
                                          end, setelement(N, PaddingMatrix, <<>>), lists:reverse(lists:seq(2,N)))
                      end,

    [FirstRow|_] = PacketRows = lists:map(fun(RowNumber) ->
                                                  case RowNumber > N of
                                                      true ->
                                                          %% the last row is all padding
                                                          Bins = lists:sublist(tuple_to_list(EncryptedMatrix), ((RowNumber-1)*N)+1, N),
                                                          list_to_binary(Bins);
                                                      false ->
                                                          encrypt_row(RowNumber, N, EncryptedMatrix, OnionCompactKey, ECDHKeys, IVs, BlockHash, Ledger)
                                                  end
                                          end, lists:seq(1, N+1)),
    {<<(hd(IVs)):16/integer-unsigned-little, OnionCompactKey/binary, FirstRow/binary>>, PacketRows}.


%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

encrypt_cell(Row, Column, N, Matrix, OnionCompactKey, ECDHKeys, IVs, BlockHash, Ledger) ->
    SecretKey = lists:nth(Row, ECDHKeys),
    Bins = lists:sublist(tuple_to_list(Matrix), ((Row-1)*N)+1, Column),
    Offset = lists:sum([byte_size(X) || X <- Bins]) - byte_size(lists:last(Bins)),
    IV0 = lists:nth(Row, IVs),
    IV = <<0:80/integer, IV0:16/integer-unsigned-little>>,
    {CipherText, _Tag} = ?ENCRYPT(block_key(SecretKey, BlockHash, Ledger),
                                  IV, <<IV/binary, OnionCompactKey/binary>>,
                                  list_to_binary(Bins), 4),
    << _:Offset/binary, Cell/binary>> = CipherText,
    Cell.

encrypt_row(Row, N, Matrix, OnionCompactKey, ECDHKeys, IVs, BlockHash, Ledger) ->
    SecretKey = lists:nth(Row, ECDHKeys),
    Bins = lists:sublist(tuple_to_list(Matrix), ((Row-1)*N)+1, N),
    IV0 = lists:nth(Row, IVs),
    IV = <<0:80/integer, IV0:16/integer-unsigned-little>>,
    {CipherText, Tag} = ?ENCRYPT(block_key(SecretKey, BlockHash, Ledger),
                                 IV, <<IV/binary, OnionCompactKey/binary>>,
                                 list_to_binary(Bins), 4),
    <<Tag/binary, CipherText/binary>>.

compute_ivs(InitialIV, KeysAndData) ->
    lists:foldl(fun({_, Data}, [H|_]=Acc) ->
                        PaddingSize = byte_size(Data) + 5,
                        <<_:PaddingSize/binary, Xor:16/integer-unsigned-little, _/binary>> = crypto:hash(sha512, Data),
                        [(H bxor Xor) band 16#ffff | Acc]
                end, [InitialIV], lists:reverse(KeysAndData)).


%% TODO - make this better
block_key(SecretKey, _BlockHash, <<"ignore_ledger">>) ->
    SecretKey;
block_key(SecretKey, BlockHash, Ledger) ->
    case blockchain:config(?poc_version, Ledger) of
        {ok, V} when V >= 2 ->
            crypto:hash(sha256, <<SecretKey/binary, BlockHash/binary>>);
        _ ->
            SecretKey
    end.


-ifdef(TEST).

encrypt_decrypt_multi_layer_poc_v4_test_() ->
    [{"no blockhash entropy", fun() ->
        TestDir = test_utils:tmp_dir("encrypt_decrypt_test_1"),
        Ledger = blockchain_ledger_v1:new(TestDir),
        encrypt_decrypt(Ledger),
        test_utils:cleanup_tmp_dir(TestDir)
      end},
     {"added blockhash entropy", fun() ->
         TestDir = test_utils:tmp_dir("encrypt_decrypt_test_2"),
         Ledger = blockchain_ledger_v1:new(TestDir),
         Ledger1 = blockchain_ledger_v1:new_context(Ledger),
         blockchain_ledger_v1:vars(#{?poc_version => 4}, [], Ledger1),
         encrypt_decrypt(Ledger1),
         test_utils:cleanup_tmp_dir(TestDir)
      end}].

encrypt_decrypt_single_layer_poc_v4_test_() ->
    [{"no blockhash entropy", fun() ->
        TestDir = test_utils:tmp_dir("encrypt_decrypt_test_1"),
        Ledger = blockchain_ledger_v1:new(TestDir),
        encrypt_decrypt_single_layer(Ledger),
        test_utils:cleanup_tmp_dir(TestDir)
      end},
     {"added blockhash entropy", fun() ->
         TestDir = test_utils:tmp_dir("encrypt_decrypt_test_2"),
         Ledger = blockchain_ledger_v1:new(TestDir),
         Ledger1 = blockchain_ledger_v1:new_context(Ledger),
         blockchain_ledger_v1:vars(#{?poc_version => 4}, [], Ledger1),
         encrypt_decrypt_single_layer(Ledger1),
         test_utils:cleanup_tmp_dir(TestDir)
      end}].

encrypt_decrypt_double_layer_poc_v4_test_() ->
    [{"no blockhash entropy", fun() ->
        TestDir = test_utils:tmp_dir("encrypt_decrypt_test_1"),
        Ledger = blockchain_ledger_v1:new(TestDir),
        encrypt_decrypt_double_layer(Ledger),
        test_utils:cleanup_tmp_dir(TestDir)
      end},
     {"added blockhash entropy", fun() ->
         TestDir = test_utils:tmp_dir("encrypt_decrypt_test_2"),
         Ledger = blockchain_ledger_v1:new(TestDir),
         Ledger1 = blockchain_ledger_v1:new_context(Ledger),
         blockchain_ledger_v1:vars(#{?poc_version => 4}, [], Ledger1),
         encrypt_decrypt_double_layer(Ledger1),
         test_utils:cleanup_tmp_dir(TestDir)
      end}].

encrypt_decrypt_test_() ->
    [{"no blockhash entropy", fun() ->
        TestDir = test_utils:tmp_dir("encrypt_decrypt_test_1"),
        Ledger = blockchain_ledger_v1:new(TestDir),
        encrypt_decrypt(Ledger),
        test_utils:cleanup_tmp_dir(TestDir)
      end},
     {"added blockhash entropy", fun() ->
         TestDir = test_utils:tmp_dir("encrypt_decrypt_test_2"),
         Ledger = blockchain_ledger_v1:new(TestDir),
         Ledger1 = blockchain_ledger_v1:new_context(Ledger),
         blockchain_ledger_v1:vars(#{?poc_version => 2}, [], Ledger1),
         encrypt_decrypt(Ledger1),
         test_utils:cleanup_tmp_dir(TestDir)
      end}].

encrypt_decrypt_single_layer(Ledger) ->
    #{secret := PrivKey1, public := PubKey1} = libp2p_crypto:generate_keys(ecc_compact),

    OnionKey = libp2p_crypto:generate_keys(ecc_compact),

    PubKeys = [PubKey1],
    PrivKeys = [PrivKey1],

    LayerData = [<<"abc">>],

    KeysAndData = lists:zip(PubKeys, LayerData),

    IV = rand:uniform(16384),
    BlockHash = crypto:strong_rand_bytes(32),
    {OuterPacket, Rows} = build(OnionKey, IV, KeysAndData, BlockHash, Ledger),
    %% make sure it's deterministic
    {OuterPacket, Rows} = build(OnionKey, IV, KeysAndData, BlockHash, Ledger),

    #{secret := PrivOnionKey, public := PubOnionKey} = OnionKey,

    ECDHFun1 = libp2p_crypto:mk_ecdh_fun(PrivKey1),
    ECDHFun2 = libp2p_crypto:mk_ecdh_fun(PrivOnionKey),
    SecretKey1 = ECDHFun1(PubOnionKey),
    SecretKey2 = ECDHFun2(PubKey1),
    ?assertEqual(SecretKey1, SecretKey2),
    {<<"abc">>, Remainder1} = decrypt(OuterPacket, libp2p_crypto:mk_ecdh_fun(PrivKey1), BlockHash, Ledger),
    ?assert(lists:all(fun(E) -> E == error end, [ decrypt(OuterPacket, libp2p_crypto:mk_ecdh_fun(PK), BlockHash, Ledger) || PK <- PrivKeys -- [PrivKey1]])),
    OnionCompactKey = libp2p_crypto:pubkey_to_bin(PubOnionKey),
    %ExpectedIV = IV+1,
    <<_IV:16/integer-unsigned-little, OnionCompactKey:33/binary, _Rest/binary>> = Remainder1,
    %% check all packets are the same length
    ?assertEqual(1, length(lists:usort([ byte_size(B) || B <- [OuterPacket, Remainder1]]))),
    %% check all the packets at each decryption layer are as expected, and have the right IV
    {IVs, Layers} = lists:unzip([{ThisIV, Layer} || <<ThisIV:16/integer-unsigned-little, ThisKey:33/binary, Layer/binary>>
                        <- [OuterPacket, Remainder1], ThisKey == OnionCompactKey]),
    ?assertEqual(Layers, Rows),
    ?assertEqual(IVs, compute_ivs(IV, KeysAndData)),
    ok.

encrypt_decrypt_double_layer(Ledger) ->
    #{secret := PrivKey1, public := PubKey1} = libp2p_crypto:generate_keys(ecc_compact),
    #{secret := PrivKey2, public := PubKey2} = libp2p_crypto:generate_keys(ecc_compact),

    OnionKey = libp2p_crypto:generate_keys(ecc_compact),

    PubKeys = [PubKey1, PubKey2],
    PrivKeys = [PrivKey1, PrivKey2],

    LayerData = [<<"abc">>, <<"def">>],

    KeysAndData = lists:zip(PubKeys, LayerData),

    IV = rand:uniform(16384),
    BlockHash = crypto:strong_rand_bytes(32),
    {OuterPacket, Rows} = build(OnionKey, IV, KeysAndData, BlockHash, Ledger),
    %% make sure it's deterministic
    {OuterPacket, Rows} = build(OnionKey, IV, KeysAndData, BlockHash, Ledger),

    #{secret := PrivOnionKey, public := PubOnionKey} = OnionKey,

    ECDHFun1 = libp2p_crypto:mk_ecdh_fun(PrivKey1),
    ECDHFun2 = libp2p_crypto:mk_ecdh_fun(PrivOnionKey),
    SecretKey1 = ECDHFun1(PubOnionKey),
    SecretKey2 = ECDHFun2(PubKey1),
    ?assertEqual(SecretKey1, SecretKey2),
    {<<"abc">>, Remainder1} = decrypt(OuterPacket, libp2p_crypto:mk_ecdh_fun(PrivKey1), BlockHash, Ledger),
    ?assert(lists:all(fun(E) -> E == error end, [ decrypt(OuterPacket, libp2p_crypto:mk_ecdh_fun(PK), BlockHash, Ledger) || PK <- PrivKeys -- [PrivKey1]])),
    OnionCompactKey = libp2p_crypto:pubkey_to_bin(PubOnionKey),
    %ExpectedIV = IV+1,
    <<_IV:16/integer-unsigned-little, OnionCompactKey:33/binary, _Rest/binary>> = Remainder1,
    {<<"def">>, Remainder2} = decrypt(Remainder1, libp2p_crypto:mk_ecdh_fun(PrivKey2), BlockHash, Ledger),
    %% check all packets are the same length
    ?assertEqual(1, length(lists:usort([ byte_size(B) || B <- [OuterPacket, Remainder1]]))),
    %% check all the packets at each decryption layer are as expected, and have the right IV
    {IVs, Layers} = lists:unzip([{ThisIV, Layer} || <<ThisIV:16/integer-unsigned-little, ThisKey:33/binary, Layer/binary>>
                        <- [OuterPacket, Remainder1, Remainder2], ThisKey == OnionCompactKey]),
    ?assertEqual(Layers, Rows),
    ?assertEqual(IVs, compute_ivs(IV, KeysAndData)),
    ok.


encrypt_decrypt(Ledger) ->
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

    IV = rand:uniform(16384),
    BlockHash = crypto:strong_rand_bytes(32),
    {OuterPacket, Rows} = build(OnionKey, IV, KeysAndData, BlockHash, Ledger),
    %% make sure it's deterministic
    {OuterPacket, Rows} = build(OnionKey, IV, KeysAndData, BlockHash, Ledger),

    #{secret := PrivOnionKey, public := PubOnionKey} = OnionKey,

    ECDHFun1 = libp2p_crypto:mk_ecdh_fun(PrivKey1),
    ECDHFun2 = libp2p_crypto:mk_ecdh_fun(PrivOnionKey),
    SecretKey1 = ECDHFun1(PubOnionKey),
    SecretKey2 = ECDHFun2(PubKey1),
    ?assertEqual(SecretKey1, SecretKey2),
    {<<"abc">>, Remainder1} = decrypt(OuterPacket, libp2p_crypto:mk_ecdh_fun(PrivKey1), BlockHash, Ledger),
    ?assert(lists:all(fun(E) -> E == error end, [ decrypt(OuterPacket, libp2p_crypto:mk_ecdh_fun(PK), BlockHash, Ledger) || PK <- PrivKeys -- [PrivKey1]])),
    OnionCompactKey = libp2p_crypto:pubkey_to_bin(PubOnionKey),
    %ExpectedIV = IV+1,
    <<_IV:16/integer-unsigned-little, OnionCompactKey:33/binary, _Rest/binary>> = Remainder1,
    {<<"def">>, Remainder2} = decrypt(Remainder1, libp2p_crypto:mk_ecdh_fun(PrivKey2), BlockHash, Ledger),
    ?assert(lists:all(fun(E) -> E == error end, [ decrypt(Remainder1, libp2p_crypto:mk_ecdh_fun(PK), BlockHash, Ledger) || PK <- PrivKeys -- [PrivKey2]])),
    {<<"ghi">>, Remainder3} = decrypt(Remainder2, libp2p_crypto:mk_ecdh_fun(PrivKey3), BlockHash, Ledger),
    ?assert(lists:all(fun(E) -> E == error end, [ decrypt(Remainder2, libp2p_crypto:mk_ecdh_fun(PK), BlockHash, Ledger) || PK <- PrivKeys -- [PrivKey3]])),
    {<<"jhk">>, Remainder4} = decrypt(Remainder3, libp2p_crypto:mk_ecdh_fun(PrivKey4), BlockHash, Ledger),
    ?assert(lists:all(fun(E) -> E == error end, [ decrypt(Remainder3, libp2p_crypto:mk_ecdh_fun(PK), BlockHash, Ledger) || PK <- PrivKeys -- [PrivKey4]])),
    {<<"lmn">>, Remainder5} = decrypt(Remainder4, libp2p_crypto:mk_ecdh_fun(PrivKey5), BlockHash, Ledger),
    ?assert(lists:all(fun(E) -> E == error end, [ decrypt(Remainder4, libp2p_crypto:mk_ecdh_fun(PK), BlockHash, Ledger) || PK <- PrivKeys -- [PrivKey5]])),
    {<<"opq">>, Remainder6} = decrypt(Remainder5, libp2p_crypto:mk_ecdh_fun(PrivKey6), BlockHash, Ledger),
    ?assert(lists:all(fun(E) -> E == error end, [ decrypt(Remainder5, libp2p_crypto:mk_ecdh_fun(PK), BlockHash, Ledger) || PK <- PrivKeys -- [PrivKey6]])),
    {<<"rst">>, Remainder7} = decrypt(Remainder6, libp2p_crypto:mk_ecdh_fun(PrivKey7), BlockHash, Ledger),
    ?assert(lists:all(fun(E) -> E == error end, [ decrypt(Remainder6, libp2p_crypto:mk_ecdh_fun(PK), BlockHash, Ledger) || PK <- PrivKeys -- [PrivKey7]])),
    {<<"uvw">>, Remainder8} = decrypt(Remainder7, libp2p_crypto:mk_ecdh_fun(PrivKey8), BlockHash, Ledger),
    ?assert(lists:all(fun(E) -> E == error end, [ decrypt(Remainder7, libp2p_crypto:mk_ecdh_fun(PK), BlockHash, Ledger) || PK <- PrivKeys -- [PrivKey8]])),
    %% check all packets are the same length
    ?assertEqual(1, length(lists:usort([ byte_size(B) || B <- [OuterPacket, Remainder1, Remainder2, Remainder3, Remainder4, Remainder5, Remainder6, Remainder7, Remainder8]]))),
    %% check all the packets at each decryption layer are as expected, and have the right IV
    {IVs, Layers} = lists:unzip([{ThisIV, Layer} || <<ThisIV:16/integer-unsigned-little, ThisKey:33/binary, Layer/binary>>
                        <- [OuterPacket, Remainder1, Remainder2, Remainder3, Remainder4, Remainder5, Remainder6, Remainder7, Remainder8], ThisKey == OnionCompactKey]),
    ?assertEqual(Layers, Rows),
    ?assertEqual(IVs, compute_ivs(IV, KeysAndData)),
    ok.

-endif.
