%% TODO Find a better home for this set of macros. Maybe in data_contract?
-define(DEFINE_RECORD_TO_KVL(RECORD_NAME),
record_to_kvl(RECORD_NAME, RecordValue) ->
    Keys = record_info(fields, RECORD_NAME),
    [_ | Vals] = tuple_to_list(RecordValue),
    lists:zip(Keys, Vals)
).

-define(RECORD_TO_KVL(RECORD_NAME, RECORD_VALUE), lists:zip(record_info(fields, RECORD_NAME), tl(tuple_to_list(RECORD_VALUE)))).
