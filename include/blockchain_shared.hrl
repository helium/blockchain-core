-type dialers() :: [dialer()].
-type dialer() :: {pid(), libp2p_crypto:pubkey_bin()}.

-record(txn_data,
        {
            callback :: fun(),
            recv_block_height=undefined :: undefined | integer(),
            acceptions=[] :: [libp2p_crypto:pubkey_bin()],
            rejections=[] :: [libp2p_crypto:pubkey_bin()],
            dialers=[] :: dialers()
        }).
