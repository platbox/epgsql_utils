%%% модуль для генерации sql запросов из DSL
-module(epgsql_utils_sql).

-export([select    /4]).
-export([update    /3]).
-export([insert    /2]).
-export([minsert   /2]).
-export([delete    /2]).
-export([make_where/1]).
-export([quote     /1]).


%% SQL API
%% {select, Fields, from, Table, where, Conds, Args}
%% {select, [id], from, users, where, [{id, '=', 42}], []}
%% {select, [id], from, users, where, [{id, '=', '$1'}], [42]}
select(Table, Fields, Cond, Opts) ->
    {Where, Args, _} = make_where(Cond),
    Q = [append_extra_clauses(Opts, [
            <<"SELECT ">>, join_iolist(<<", ">>, prepare_fields(Fields)),
            <<" FROM ">>, prepare_table_name(Table),
            Where
        ]), <<";">>
    ],
    {Q, Args}.

update(Table, FieldsValues, Cond) ->
    {Values, Args0, N0} = make_update_values(FieldsValues),
    {Where , Args1, _ } = make_where(Cond, N0),
    Q = [
        <<"UPDATE ">>, prepare_table_name(Table),
        <<" SET ">>, Values,
        Where,
    <<";">>],
    {Q, Args0 ++ Args1}.

insert(Table, FieldValues) ->
    minsert(Table, [FieldValues]).

minsert(Table, RowsValues = [FieldValues | _]) ->
    {Values, Args} = make_minsert_values(RowsValues),
    Q = [
        <<"INSERT INTO ">>, prepare_table_name(Table),
        <<"(">>, prepare_field_names(FieldValues), <<") ">>,
        Values,
    <<";">>],
    {Q, Args}.

delete(Table, Cond) ->
    {Where, Args, _} = make_where(Cond),
    Q = [
        <<"DELETE FROM ">>, prepare_table_name(Table),
        Where,
    <<";">>],
    {Q, Args}.

append_extra_clauses([], Q) ->
    Q;
append_extra_clauses(Opts, Q) ->
    Clauses = lists:foldr(fun (Key, Acc) ->
        case lists:keyfind(Key, 1, Opts) of
            false -> Acc;
            Value -> [Value | Acc]
        end
    end, [], [order, limit]),
    lists:foldl(fun append_extra_clause/2, Q, Clauses).

append_extra_clause({limit, N}, Q) ->
    _ = N > 0 orelse error(badarg),
    [Q, <<" LIMIT ">>, integer_to_binary(N)];
append_extra_clause({order, FieldName}, Q) ->
    [Q, <<" ORDER BY ">>, to_binary(FieldName), <<" ">>].


prepare_table_name({Schema, Table}) ->
    <<(to_binary(Schema))/binary, ".", (to_binary(Table))/binary>>;
prepare_table_name(Table) ->
    to_binary(Table).

prepare_fields(Fields) ->
    lists:map(fun to_binary/1, Fields).

prepare_field_names(FieldsValues) ->
    join_iolist(<<",">>, [to_binary(FieldName) || {FieldName, _} <- FieldsValues]).

make_minsert_values(RowsValues) ->
    {Values, Args, _} = lists:foldl(fun make_insert_values/2, {<<>>, [], 1}, RowsValues),
    {Values, Args}.

make_insert_values(RowValue, {ValAcc, ArgAcc, ArgNum}) ->
    {ValuesQ, Args, N} = lists:foldl(fun append_insert_value/2, {<<>>, [], ArgNum}, RowValue),
    ValAccNext = append_binary_part(ValAcc, <<", ">>, <<"VALUES ">>),
    {<<ValAccNext/binary, $(, ValuesQ/binary, $)>>, Args ++ ArgAcc, N}.

append_insert_value({_, null}, {Q, Args, NAcc}) ->
    QNext = append_binary_part(Q, <<",">>, <<>>),
    {<<QNext/binary, "DEFAULT">>, Args, NAcc};
append_insert_value({_, FValue}, {Q, Args, NAcc}) ->
    QNext = append_binary_part(Q, <<",">>, <<>>),
    {<<QNext/binary, "$", (integer_to_binary(NAcc))/binary>>, Args ++ [FValue], NAcc + 1}.

append_binary_part(<<>>, _, InitialPart) ->
    InitialPart;
append_binary_part(Acc, Part, _) ->
    <<Acc/binary, Part/binary>>.

make_update_values(FiledValues) ->
    make_update_values(FiledValues, 1).
make_update_values([], N) ->
    {[], [], N};
make_update_values(FiledValues, N) ->
    F = fun({FName, FValue}, {Q, Args, NAcc}) ->
            {Q ++ [[to_binary(FName), <<"=$">>, integer_to_binary(NAcc)]], Args++[FValue], NAcc + 1}
        end,
    {ValuesQ, Args, N1} = lists:foldl(F, {[], [], N}, FiledValues),
    {join_iolist(<<", ">>, ValuesQ), Args, N1}.

make_where(Conds) ->
    make_where(Conds, 1).
make_where([], N) ->
    {[], [], N};
make_where(Conds, N) ->
    F = fun ({V1, Op, V2}, {Q, Args, NAcc}) ->
                {Q ++ [[to_binary(V1), to_binary(Op), <<"$">>, integer_to_binary(NAcc)]], Args ++ [V2], NAcc + 1};
            (V, {Q, Args, NAcc}) ->
                {Q ++ [V], Args, NAcc}
        end,
    {CondsQ, Args, N1} = lists:foldl(F, {[], [], N}, Conds),
    {[<<" WHERE ">>, join_iolist(<<" AND ">>, CondsQ)], Args, N1}.


quote(String) when is_list(String) ->
    [$' | lists:reverse([$' | quote(String, [])])];
quote(Bin) when is_binary(Bin) ->
    list_to_binary(quote(binary_to_list(Bin))).

quote([], Acc) ->
    Acc;
quote([$\0 | Rest], Acc) ->
    quote(Rest, [$0, $\\ | Acc]);
quote([$\n | Rest], Acc) ->
    quote(Rest, [$n, $\\ | Acc]);
quote([$\r | Rest], Acc) ->
    quote(Rest, [$r, $\\ | Acc]);
quote([$\\ | Rest], Acc) ->
    quote(Rest, [$\\ , $\\ | Acc]);
quote([$' | Rest], Acc) ->
    quote(Rest, [$', $\\ | Acc]);
quote([$" | Rest], Acc) ->
    quote(Rest, [$", $\\ | Acc]);
quote([$\^Z | Rest], Acc) ->
    quote(Rest, [$Z, $\\ | Acc]);
quote([C | Rest], Acc) ->
    quote(Rest, [C | Acc]).


to_binary(V) when is_atom(V)   -> atom_to_binary(V, latin1);
to_binary(V) when is_list(V)   -> iolist_to_binary(V);
to_binary(V) when is_binary(V) -> V.

join_iolist(_    , [])    -> [];
join_iolist(_    , [H])   -> [H];
join_iolist(Delim, [H|T]) -> [H, Delim | join_iolist(Delim, T)].
