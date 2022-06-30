-define(ROCKSDB_ITERATOR_CLOSE(IteratorHandle),
    %% Wrapping in fun to avoid scope leakage:
    (fun() ->
        try
            rocksdb:iterator_close(IteratorHandle)
        catch ErrorClass:ErrorReason ->
            case application:get_env(blockchain, log_rocksdb_iterator_close_errors, true) of
                true ->
                    lager:warning("rocksdb:iterator_close error: ~p:~p", [ErrorClass, ErrorReason]);
                false ->
                    ok
            end
        end
    end)()
).
