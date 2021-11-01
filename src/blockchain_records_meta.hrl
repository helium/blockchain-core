-define(DEFINE_RECORD_TO_KVL(RECORD_NAME),
record_to_kvl(RECORD_NAME, RecordValue) ->
    Keys = record_info(fields, RECORD_NAME),
    [_ | Vals] = tuple_to_list(RecordValue),
    lists:zip(Keys, Vals)
).
