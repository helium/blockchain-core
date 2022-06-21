-define(ROCKSDB_ITERATOR_CLOSE(IteratorHandle),
    %% Wrapping in fun to avoid scope leakage:
    (fun() ->
        try
            rocksdb:iterator_close(IteratorHandle)
        catch ErrorClass:ErrorReason ->
            lager:error("rocksdb:iterator_close error: ~p:~p", [ErrorClass, ErrorReason])
        end
    end)()
).
