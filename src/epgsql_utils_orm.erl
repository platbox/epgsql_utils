-module(epgsql_utils_orm).

%% standart objects operations (CRUD, etc...)
-export([get_fields           /3]).
-export([get_fields           /4]).
-export([get_fields_by_cond   /3]).
-export([get_fields_by_cond   /4]).
-export([update_fields        /3]).
-export([update_fields_by_cond/3]).
-export([find                 /2]).
-export([is_exist             /2]).
-export([create               /2]).
-export([package_create       /2]).
-export([package_create       /3]).
-export([create_or_update     /3]).
-export([get                  /2]).
-export([get_by_cond          /2]).
-export([get_by_cond          /3]).
-export([get_by_fields        /3]).
-export([update_by_cond       /3]).
-export([update               /2]).
-export([delete_by_cond       /2]).
-export([delete               /2]).
-export([package_delete       /2]).
-export([package_delete       /3]).
-export([get_last_create_id   /0]).
-export([count                /1]).
-export([count_by_cond        /2]).

-export([pack_record        /2]).
-export([pack_record_field  /2]).
-export([unpack_record      /2]).
-export([unpack_record_field/2]).

-export([pack  /2]).
-export([unpack/2]).

%% TODO
%%  - сделать маппинг стр

%% standart objects operations (CRUD, etc...)
%% operations with object fields
get_fields(Type, ID, Fields) ->
    get_fields(Type, ID, Fields, []).

get_fields(Type, ID, Fields, Opts) ->
    get_fields_by_cond(Type, make_fields_cond(struct_info(keys, Type), ID), Fields, Opts).

get_fields_by_cond(Type, Conds, Fields) ->
    get_fields_by_cond(Type, Conds, Fields, []).

get_fields_by_cond(Type, Conds, Fields, Opts) ->
    case Fields of
        [] -> [];
        _  ->
            check_fields(Type, Fields),
            Rows = q(make_select(Type, Fields, Conds, Opts)),
            lists:map(
                fun(Row) ->
                    lists:map(
                        fun({F, V}) ->
                            {F, unpack({Type, F}, V)}
                        end,
                        lists:zip(Fields, tuple_to_list(Row))
                    )
                end,
                Rows
            )
    end.


update_fields(Type, ID, FieldsValues) ->
    update_fields_by_cond(Type, make_fields_cond(struct_info(keys, Type), ID), FieldsValues).

update_fields_by_cond(Type, Conds, FieldsValues) ->
    {Fields, _} = lists:unzip(FieldsValues),
    check_fields(Type, Fields),
    case FieldsValues of
        [] -> ok;
        _  ->
            q(make_update_proplist(Type, FieldsValues, Conds))
    end.

check_fields(Type, RequestedFields) ->
    PresentedFields = get_struct_fields_names(Type),
    lists:foreach(fun(Field) ->
        case lists:member(Field, PresentedFields) of
            true  -> ok;
            false -> error({unknown_field, Type, Field})
        end
    end, RequestedFields).

%%package functions
package_create(Type, ObjectList) ->
    package_create(Type, ObjectList, 20000).

package_create(Type, ObjectList, PackageSize) ->
    package_create(Type, ObjectList, PackageSize, [], 0).

package_create(Type, ObjectList, PackageSize, Package, PackageSize) ->
    Query = epgsql_utils_sql:minsert(struct_info(table, Type), Package),
    q(Query),
    package_create(Type, ObjectList, PackageSize, [], 0);
package_create(Type, [H | T], PackageSize, Acc, CurrQuery) ->
    package_create(Type, T, PackageSize, [pack(Type, H) | Acc], CurrQuery + 1);
package_create(_, [], _, [], _) ->
    0;
package_create(Type, [], _, Package, _) ->
    Query = epgsql_utils_sql:minsert(struct_info(table, Type), Package),
    q(Query).


package_delete(Type, IdList) ->
    package_delete(Type, IdList, 1000).

package_delete(Type, IdList, PackageSize) ->
    package_delete(Type, IdList, PackageSize, [], 0).

package_delete(Type, [H | T], PackageSize, QueryIOList, PackageSize) ->
    DeleteQuery = epgsql_utils_sql:delete(struct_info(table, Type),
                                          make_fields_cond(struct_info(keys, Type), H)
                                         ),
    q(QueryIOList),
    package_delete(Type, T, PackageSize, [DeleteQuery], 1);
package_delete(Type, [H | T], PackageSize, QueryIOList, CurrQuery) ->
    DeleteQuery = epgsql_utils_sql:delete(struct_info(table, Type),
                                          make_fields_cond(struct_info(keys, Type), H)
                                         ),
    package_delete(Type, T, PackageSize, [DeleteQuery, QueryIOList], CurrQuery + 1);
package_delete(_, [], _, QueryIOList, _) ->
    q(QueryIOList).



%% find
find(Type, Conds) ->
    q(make_select(Type, get_struct_fields_names(Type), Conds, [])).

is_exist(Type, ID) ->
    case find(Type, make_fields_cond(struct_info(keys, Type), ID)) of
        [ ] -> false;
        [_] -> true
    end.

%% insert
create(Type, Object) ->
    1 = q(epgsql_utils_sql:insert(struct_info(table, Type), pack(Type, Object))).

create_or_update(Type, ID, Object) ->
    case is_exist(Type, ID) of
        true  -> update(Type, Object);
        false -> create(Type, Object)
    end.

%% get
get_by_cond(Type, Conds) ->
    get_by_cond(Type, Conds, []).

get_by_cond(Type, Conds, Opts) ->
    unpack_objects(Type, q(make_select(Type, get_struct_fields_names(Type), Conds, Opts))).

unpack_objects(Type, Data) ->
    map_selected(
        fun(PL) ->
            unpack(Type, lists:zip(get_struct_fields_names(Type), tuple_to_list(PL)))
        end,
        Data
    ).

get(Type, ID) ->
    get_by_cond(Type, make_fields_cond(struct_info(keys, Type), ID)).


get_by_fields(Type, Field, Value) ->
    get_by_cond(Type, make_fields_cond(Field, Value)).


%% count
count_by_cond(Type, Conds) ->
    [{N}] = q(make_select(Type, ["count(*)"], Conds, [])),
    N.


count(Type) ->
    [{N}] = q(make_select(Type, ["count(*)"], [], [])),
    N.

%% update
update_by_cond(Type, Object, Cond) ->
    q(make_update_object(Type, Object, Cond)).

update(Type, Object) ->
    1 = q(make_update_object(Type, Object)).

%% delete
delete_by_cond(Type, Cond) ->
    q(make_delete(Type, Cond)).

delete(Type, ID) ->
    q(epgsql_utils_sql:delete(struct_info(table, Type),
        make_fields_cond(struct_info(keys, Type), ID)
    )).

get_last_create_id() ->
    [{ID}] = epgsql_utils_querying:do_query(<<"SELECT LASTVAL();">>),
    ID.

%% utils
make_fields_cond(Fields, Values) when is_list(Values), is_list(Fields) ->
    case length(Fields) =:= length(Values) of
        false ->
            error(badarg, [Fields, Values]);
        true  ->
            lists:map(
                fun({KeyElement, IDElement}) ->
                    {KeyElement, '=', IDElement}
                end,
                lists:zip(Fields, Values)
            )
    end;
make_fields_cond([Field], Value) ->
    make_fields_cond([Field], [Value]);
make_fields_cond(Field, Value) ->
    make_fields_cond([Field], [Value]).

pack_conds(Type, Conds) when is_list(Conds) ->
    pack_conds(Type, Conds, []);
pack_conds(Type, Cond) ->
    pack_conds(Type, [Cond], []).

pack_conds(Type, [{Field, Predicate, Value} | T], Acc) ->
    pack_conds(Type, T, [{Field, Predicate, pack({Type, Field}, Value)} | Acc]);
pack_conds(_, [], Acc) ->
    Acc.


%% wrappers
make_select(Type, Fields, Conds, Opts) ->
    PackedConds = pack_conds(Type, Conds),
    epgsql_utils_sql:select(struct_info(table, Type), Fields, PackedConds, Opts).

make_update_object(Type, Object) ->
    PackedObject = pack(Type, Object),
    PackedConds  = pack_conds(Type, lists:map(
        fun(KeyPart) -> {KeyPart, '=', proplists:get_value(KeyPart, PackedObject)} end,
        struct_info(keys, Type)
    )),
    epgsql_utils_sql:update(struct_info(table, Type), PackedObject, PackedConds).

make_update_object(Type, Object, Conds) ->
    PackedObject = pack(Type, Object),
    PackedConds  = pack_conds(Type, Conds),
    epgsql_utils_sql:update(struct_info(table, Type), PackedObject, PackedConds).

make_update_proplist(Type, FieldsValues, Conds) ->
    PackedConds = pack_conds(Type, Conds),
    PackedFieldsValues = lists:map( fun({F, V}) -> {F, pack({Type, F}, V)} end,
                                    FieldsValues
                                  ),
    epgsql_utils_sql:update(struct_info(table, Type), PackedFieldsValues, PackedConds).

make_delete(Type, Conds) ->
    PackedConds = pack_conds(Type, Conds),
    epgsql_utils_sql:delete(struct_info(table, Type), PackedConds).


map_selected(MapF, L) ->
    lists:foldr(
        fun(E, Acc) ->
                [MapF(E)|Acc]
        end,
        [],
        L
    ).

pack_record_field({Type, FieldName}, V) ->
    FieldType = proplists:get_value(FieldName, struct_info(fields, Type)),
    pack(FieldType, V).

pack_record(Type={Mod, LocalType}, V) ->
    FieldsTypes = struct_info(fields, Type),
    [_|FieldsValues] = tuple_to_list(V),
    lists:map(
        fun({{FieldName, _}, FieldValue}) ->
            {FieldName, pack({Mod, {LocalType, FieldName}}, FieldValue)}
        end,
        lists:zip(FieldsTypes, FieldsValues)
    ).

unpack_record_field({Type, FieldName}, V) ->
    FieldType = proplists:get_value(FieldName, struct_info(fields, Type)),
    unpack(FieldType, V).

unpack_record(Type={Mod, LocalType}, V) ->
    FieldsTypes = struct_info(fields, Type),
    FieldsValues =
        lists:map(
            fun({FieldName, _}) ->
                unpack({Mod, {LocalType, FieldName}}, proplists:get_value(FieldName, V, null))
            end,
            FieldsTypes
        ),
    list_to_tuple([LocalType|FieldsValues]).

q({Q, A}) ->
    epgsql_utils_querying:do_query(Q, A).

get_struct_fields_names(Type) ->
    element(1, lists:unzip(struct_info(fields, Type))).

struct_info(Attr, {Mod, LocalType}) ->
    Mod:struct_info({LocalType, Attr}).

%% packer
pack({?MODULE, T}, V) ->
    pack_(T, V);
pack({{Mod, T}, F}, V) ->
    Mod:pack({T, F}, V);
pack({Mod, T}, V) ->
    Mod:pack(T, V);
pack(Type, Data) ->
    error(badarg, [Type, Data]).

pack_(_, undefined) -> null;
pack_(boolean, Boolean) when is_boolean(Boolean) -> Boolean;
pack_(integer, Int) when is_integer(Int) -> Int;
pack_(float, Float) when is_float(Float) or is_integer(Float) -> Float;
pack_(string, Str) when is_binary(Str) -> Str;
pack_(string, IOList) when is_list(IOList) -> iolist_to_binary(IOList);
pack_(datetime, DateTime) -> DateTime;
pack_({list, T}, List) when is_list(List) -> [pack(T, E) || E <- List];
pack_(binary, Binary) when is_binary(Binary) -> Binary;
pack_(binary, IOList) when is_list(IOList) -> iolist_to_binary(IOList);
pack_(term, Term) -> term_to_binary(Term);
pack_(date, Date={_, _, _}) ->
    Date;
pack_(json, Json) when is_atom(Json)-> pack_(atom, Json);
pack_(json, Json) when is_binary(Json)-> pack_(binary, Json);
pack_(json, Json) when is_number(Json)-> pack_(number, Json);
pack_(json, Json={List}) when is_list(List) -> jiffy:encode(Json);
pack_(json_map, Json) when is_atom(Json)-> pack_(atom, Json);
pack_(json_map, Json) when is_binary(Json)-> pack_(binary, Json);
pack_(json_map, Json) when is_number(Json)-> pack_(number, Json);
pack_(json_map, Json) when is_map(Json) -> jiffy:encode(Json);
pack_(atom, Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8);
pack_(pos_integer, PosInteger) when PosInteger > 0 -> pack_(integer, PosInteger);
pack_(neg_integer, NegInteger) when NegInteger < 0 -> pack_(integer, NegInteger);
pack_(non_neg_integer, NonNegInteger) when NonNegInteger >= 0 -> pack_(integer, NonNegInteger);
pack_(number, Number) when is_float(Number); is_integer(Number) -> Number;
pack_(byte, Byte) when Byte >= 0, Byte =< 255 -> pack_(integer, Byte);
pack_(char, Char) when Char >= 0, Char =< 16#10ffff -> pack_(integer, Char);
pack_(iodata, IoData) -> pack_(binary, IoData);
pack_(iolist, IoList) -> pack_(binary, IoList);

pack_(Type, Data) ->
    error(badarg, [Type, Data]).


unpack({?MODULE, T}, V) ->
    unpack_(T, V);
unpack({{Mod, T}, F}, V) ->
    Mod:unpack({T, F}, V);
unpack({Mod, T}, V) ->
    Mod:unpack(T, V);
unpack(Type, Data) ->
    error(badarg, [Type, Data]).

unpack_(_, null) -> undefined;
unpack_(boolean, Boolean) when is_boolean(Boolean) -> Boolean;
unpack_(integer, Int) when is_integer(Int) -> Int;
unpack_(float, Float) when is_float(Float) or is_integer(Float) -> Float;
unpack_(string, Str) when is_binary(Str) -> Str;
unpack_(datetime, {Date, {H,M,S}}) -> {Date, {H,M,erlang:round(S)}};
unpack_({list, T}, List) when is_list(List) -> [unpack(T, E) || E <- List];
unpack_(binary, Binary) when is_binary(Binary) -> Binary;
unpack_(term, Term) -> binary_to_term(Term);
unpack_(date, Date={_, _, _}) ->
    Date;
unpack_(json, Json) -> jiffy:decode(Json);
unpack_(json_map, Json) -> jiffy:decode(Json, [return_maps]);
unpack_(atom, Atom) when is_binary(Atom) -> binary_to_atom(Atom, utf8) ;
unpack_(pos_integer, PosInteger) when PosInteger > 0 -> unpack_(integer, PosInteger);
unpack_(neg_integer, NegInteger) when NegInteger < 0 -> unpack_(integer, NegInteger);
unpack_(non_neg_integer, NonNegInteger) when NonNegInteger >= 0 -> unpack_(integer, NonNegInteger);
unpack_(number, Number) when is_float(Number); is_integer(Number) -> Number;
unpack_(byte, Byte) when Byte >= 0, Byte =< 255 -> unpack_(integer, Byte);
unpack_(char, Char) when Char >= 0, Char =< 16#10ffff -> unpack_(integer, Char);
unpack_(iodata, IoData) -> unpack_(binary, IoData);
unpack_(iolist, IoList) -> [unpack_(binary, IoList)];

unpack_(Type, Data) ->
    error(badarg, [Type, Data]).
