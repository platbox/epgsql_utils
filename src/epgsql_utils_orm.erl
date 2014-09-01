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
-export([batch_create         /2]).
-export([batch_create         /3]).
-export([create_or_update     /3]).
-export([get                  /2]).
-export([get_by_cond          /2]).
-export([get_by_cond          /3]).
-export([get_by_fields        /3]).
-export([update_by_cond       /3]).
-export([update               /2]).
-export([delete_by_cond       /2]).
-export([delete               /2]).
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
            [
                [{F, unpack({Type, F}, V)} || {F, V} <- lists:zip(Fields, tuple_to_list(Row))]
                    || Row <- Rows
            ]
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

batch_create(Type, ObjectList) ->
    batch_create(Type, ObjectList, 10000).

batch_create(Type, ObjectList, BatchSize) ->
    batch_create_(Type, ObjectList, BatchSize, [], 0, 0).

batch_create_(Type, ObjectList, BatchSize, Batch, BatchSize, Total) ->
    _ = q(make_minsert(Type, Batch)),
    batch_create_(Type, ObjectList, BatchSize, [], 0, Total);
batch_create_(Type, [H | T], BatchSize, Acc, CurrSize, Total) ->
    batch_create_(Type, T, BatchSize, [pack(Type, H) | Acc], CurrSize + 1, Total + 1);
batch_create_(_, [], _, [], _, Total) ->
    Total;
batch_create_(Type, [], _, Batch, _, Total) ->
    _ = q(make_minsert(Type, Batch)),
    Total.

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
    [IDTuple] = q(make_insert(Type, Object)),
    FieldList = lists:zip(struct_info(keys, Type), tuple_to_list(IDTuple)),
    case [unpack({Type, F}, V) || {F, V} <- FieldList] of
        [ID] -> ID;
        IDList -> IDList
    end.

create_or_update(Type, ID, Object) ->
    case is_exist(Type, ID) of
        true  -> update(Type, Object), ID;
        false -> create(Type, Object)
    end.

%% get
get_by_cond(Type, Conds) ->
    get_by_cond(Type, Conds, []).

get_by_cond(Type, Conds, Opts) ->
    unpack_objects(Type, q(make_select(Type, get_struct_fields_names(Type), Conds, Opts))).

unpack_objects(Type, Data) ->
    FieldNames = get_struct_fields_names(Type),
    [unpack(Type, lists:zip(FieldNames, tuple_to_list(PL))) || PL <- Data].

get(Type, ID) ->
    get_by_cond(Type, make_fields_cond(struct_info(keys, Type), ID)).


get_by_fields(Type, Field, Value) ->
    get_by_cond(Type, make_fields_cond(Field, Value)).


%% count
count_by_cond(Type, Conds) ->
    [{N}] = q(make_select(Type, ["count(*)"], Conds, [])),
    N.


count(Type) ->
    count_by_cond(Type, []).

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
    PackedConds  = pack_conds(Type, [
        {KeyPart, '=', proplists:get_value(KeyPart, PackedObject)} || KeyPart <- struct_info(keys, Type)
    ]),
    epgsql_utils_sql:update(struct_info(table, Type), PackedObject, PackedConds).

make_update_object(Type, Object, Conds) ->
    PackedObject = pack(Type, Object),
    PackedConds  = pack_conds(Type, Conds),
    epgsql_utils_sql:update(struct_info(table, Type), PackedObject, PackedConds).

make_update_proplist(Type, FieldsValues, Conds) ->
    PackedConds = pack_conds(Type, Conds),
    PackedFieldsValues = [{F, pack({Type, F}, V)} || {F, V} <- FieldsValues],
    epgsql_utils_sql:update(struct_info(table, Type), PackedFieldsValues, PackedConds).

make_insert(Type, Object) ->
    epgsql_utils_sql:insert(struct_info(table, Type), struct_info(keys, Type), pack(Type, Object)).

make_minsert(Type, Batch) ->
    epgsql_utils_sql:minsert(struct_info(table, Type), struct_info(keys, Type), Batch).

make_delete(Type, Conds) ->
    PackedConds = pack_conds(Type, Conds),
    epgsql_utils_sql:delete(struct_info(table, Type), PackedConds).

%%

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
    FieldsValues = [
        unpack({Mod, {LocalType, FieldName}}, proplists:get_value(FieldName, V, null))
            || {FieldName, _} <- FieldsTypes
    ],
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
pack_(json, Json) when is_map(Json)-> jiffy:encode(Json);
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
unpack_(json, Json)-> jiffy:decode(Json, [return_maps]);
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
