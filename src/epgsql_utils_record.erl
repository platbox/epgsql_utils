-module(epgsql_utils_record).

%% model record helper

-export([get_keys/2]).

-spec get_keys(atom(), tuple()) -> list({atom(), term()}).
get_keys({Mod, LocalType}, Record) when LocalType =:= element(1, Record)->
    Fields = Mod:struct_info({LocalType, fields}),
    Keys = Mod:struct_info({LocalType, keys}),
    % Record = {RecordName, Field1, Field2, Field3,..., FieldN}
    [element(N + 1, Record) || {_FieldName, N} <- match_struct_info(1, Keys, Fields)].

-spec match_struct_info(integer(), list(), list({_, _})) -> list({_, _}).
match_struct_info(N, [F | Rest], [{F, _} | RestFields]) ->
    [{F, N} | match_struct_info(N + 1, Rest, RestFields)];

match_struct_info(N, IDFieldNames, [_ | RestFields]) ->
    match_struct_info(N + 1, IDFieldNames, RestFields);

match_struct_info(_, [], _Fields) ->
    [].
