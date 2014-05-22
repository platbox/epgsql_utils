-module(epgsql_utils_record).

%% model record helper

-export([get_record_keys/2]).

-spec get_record_keys(atom(), tuple()) -> list({atom(), term()}).
get_record_keys(Mod, Record) ->
    RecordName = element(1, Record),
    Fields = Mod:struct_info({RecordName, fields}),
    Keys = Mod:struct_info({RecordName, keys}),
    lists:map(
        fun({FieldName, N}) ->
            RecordFieldN = N + 1,    %Record = {RecordName, Field1, Field2, Field3,..., FieldN}.
            {FieldName, element(RecordFieldN, Record)}
        end,
        match_struct_info(1, Keys, Fields)
    ).

-spec match_struct_info(integer(), list(), list({_, _})) -> list({_, _}).
match_struct_info(N, [F | Rest], [{F, _} | RestFields]) ->
    [{F, N} | match_struct_info(N + 1, Rest, RestFields)];

match_struct_info(N, IDFieldNames, [_ | RestFields]) ->
    match_struct_info(N + 1, IDFieldNames, RestFields);

match_struct_info(_, [], _Fields) ->
    [].
