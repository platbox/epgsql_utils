-module(epgsql_utils_orm).

%% standart objects operations (CRUD, etc...)
-export([get_fields           /4]).
-export([get_fields           /5]).
-export([get_fields_by_cond   /4]).
-export([get_fields_by_cond   /5]).
-export([update_fields        /4]).
-export([update_fields_by_cond/4]).
-export([find                 /3]).
-export([is_exist             /3]).
-export([create               /3]).
-export([batch_create         /3]).
-export([batch_create         /4]).
-export([create_or_update     /4]).
-export([get                  /3]).
-export([get_by_cond          /3]).
-export([get_by_cond          /4]).
-export([get_by_fields        /4]).
-export([update_by_cond       /4]).
-export([update               /3]).
-export([delete_by_cond       /3]).
-export([delete               /3]).
-export([batch_delete         /3]).
-export([batch_delete         /4]).
-export([get_last_create_id   /0]).
-export([count                /1]).
-export([count_by_cond        /2]).

-export([pack_record        /3]).
-export([pack_record_field  /3]).
-export([unpack_record      /3]).
-export([unpack_record_field/3]).

-export([pack  /3]).
-export([unpack/3]).

%% TODO
%%  - сделать маппинг стр

%% standart objects operations (CRUD, etc...)
%% operations with object fields
get_fields(Type, ID, Fields, Ctx) ->
    get_fields(Type, ID, Fields, Ctx, []).

get_fields(Type, ID, Fields, Ctx, Opts) ->
    get_fields_by_cond(Type, make_fields_cond(struct_info(keys, Type), ID, Ctx), Fields, Ctx, Opts).

get_fields_by_cond(Type, Conds, Fields, Ctx) ->
    get_fields_by_cond(Type, Conds, Fields, Ctx, []).

get_fields_by_cond(Type, Conds, Fields, Ctx, Opts) ->
    case Fields of
        [] -> [];
        _  ->
            check_fields(Type, Fields),
            Rows = q(make_select(Type, Fields, Conds, Ctx, Opts)),
            lists:map(
                fun(Row) ->
                    lists:map(
                        fun({F, V}) ->
                            {F, unpack({Type, F}, V, Ctx)}
                        end,
                        lists:zip(Fields, tuple_to_list(Row))
                    )
                end,
                Rows
            )
    end.


update_fields(Type, ID, FieldsValues, Ctx) ->
    update_fields_by_cond(Type, make_fields_cond(struct_info(keys, Type), ID, Ctx), FieldsValues, Ctx).

update_fields_by_cond(Type, Conds, FieldsValues, Ctx) ->
    {Fields, _} = lists:unzip(FieldsValues),
    check_fields(Type, Fields),
    case FieldsValues of
        [] -> ok;
        _  ->
            q(make_update_proplist(Type, FieldsValues, Conds, Ctx))
    end.

check_fields(Type, RequestedFields) ->
    PresentedFields = get_struct_fields_names(Type),
    lists:foreach(fun(Field) ->
        case lists:member(Field, PresentedFields) of
            true  -> ok;
            false -> error({unknown_field, Type, Field})
        end
    end, RequestedFields).

%% package functions
batch_create(Type, ObjectList, Ctx) ->
    batch_create(Type, ObjectList, Ctx, 20000).

batch_create(Type, ObjectList, Ctx, BatchSize) ->
    batch_create_impl(Type, ObjectList, BatchSize, [], 0, Ctx).

batch_create_impl(Type, ObjectList, BatchSize, Batch, BatchSize, Ctx) ->
    Query = epgsql_utils_sql:minsert(struct_info(table, Type), Batch),
    q(Query),
    batch_create_impl(Type, ObjectList, BatchSize, [], 0, Ctx);
batch_create_impl(Type, [H | T], BatchSize, Acc, CurrQuery, Ctx) ->
    batch_create_impl(Type, T, BatchSize, [pack(Type, H, Ctx) | Acc], CurrQuery + 1, Ctx);
batch_create_impl(_, [], _, [], _, _) ->
    0;
batch_create_impl(Type, [], _, Batch, _, _) ->
    Query = epgsql_utils_sql:minsert(struct_info(table, Type), Batch),
    q(Query).


batch_delete(Type, IdList, Ctx) ->
    batch_delete(Type, IdList, Ctx, 1000).

batch_delete(Type, IdList, Ctx, BatchSize) ->
    batch_delete_impl(Type, IdList, BatchSize, [], 0, Ctx).

batch_delete_impl(Type, [H | T], BatchSize, QueryIOList, BatchSize, Ctx) ->
    DeleteQuery = epgsql_utils_sql:delete(struct_info(table, Type),
                                          make_fields_cond(struct_info(keys, Type), H, Ctx)
                                         ),
    q(QueryIOList),
    batch_delete_impl(Type, T, BatchSize, [DeleteQuery], 1, Ctx);
batch_delete_impl(Type, [H | T], BatchSize, QueryIOList, CurrQuery, Ctx) ->
    DeleteQuery = epgsql_utils_sql:delete(struct_info(table, Type),
                                          make_fields_cond(struct_info(keys, Type), H, Ctx)
                                         ),
    batch_delete_impl(Type, T, BatchSize, [DeleteQuery, QueryIOList], CurrQuery + 1, Ctx);
batch_delete_impl(_, [], _, QueryIOList, _, _) ->
    q(QueryIOList).



%% find
find(Type, Conds, Ctx) ->
    q(make_select(Type, get_struct_fields_names(Type), Conds, [], Ctx)).

is_exist(Type, ID, Ctx) ->
    case find(Type, make_fields_cond(struct_info(keys, Type), ID, Ctx), Ctx) of
        [ ] -> false;
        [_] -> true
    end.

%% insert
create(Type, Object, Ctx) ->
    1 = q(epgsql_utils_sql:insert(struct_info(table, Type), pack(Type, Object, Ctx))).

create_or_update(Type, ID, Object, Ctx) ->
    case is_exist(Type, ID, Ctx) of
        true  -> update(Type, Object, Ctx);
        false -> create(Type, Object, Ctx)
    end.

%% get
get_by_cond(Type, Conds, Ctx) ->
    get_by_cond(Type, Conds, Ctx, []).

get_by_cond(Type, Conds, Ctx, Opts) ->
    unpack_objects(Type, q(make_select(Type, get_struct_fields_names(Type), Conds, Ctx, Opts)), Ctx).

unpack_objects(Type, Data, Ctx) ->
    map_selected(
        fun(PL) ->
            unpack(Type, lists:zip(get_struct_fields_names(Type), tuple_to_list(PL)), Ctx)
        end,
        Data
    ).

get(Type, ID, Ctx) ->
    get_by_cond(Type, make_fields_cond(struct_info(keys, Type), ID, Ctx), Ctx).


get_by_fields(Type, Field, Value, Ctx) ->
    get_by_cond(Type, make_fields_cond(Field, Value, Ctx), Ctx).


%% count
count(Type) ->
    [{N}] = q(make_select(Type, ["count(*)"], [], [], undefined)),
    N.

count_by_cond(Type, Conds) ->
    [{N}] = q(make_select(Type, ["count(*)"], Conds, [], undefined)),
    N.

%% update
update_by_cond(Type, Object, Cond, Ctx) ->
    q(make_update_object(Type, Object, Cond, Ctx)).

update(Type, Object, Ctx) ->
    1 = q(make_update_object(Type, Object, Ctx)).

%% delete
delete_by_cond(Type, Cond, Ctx) ->
    q(make_delete(Type, Cond, Ctx)).

delete(Type, ID, Ctx) ->
    q(epgsql_utils_sql:delete(struct_info(table, Type),
        make_fields_cond(struct_info(keys, Type), ID, Ctx)
    )).

get_last_create_id() ->
    [{ID}] = epgsql_utils_querying:do_query(<<"SELECT LASTVAL();">>),
    ID.

%% utils
make_fields_cond(Fields, Values, _Ctx) when is_list(Values), is_list(Fields) ->
    case length(Fields) =:= length(Values) of
        false ->
            error(badarg, [Fields, Values]);
        true  ->
            lists:map(
                fun({KeyElement, IDElement}) ->
                    {KeyElement, '=', IDElement} %% TODO pack
                end,
                lists:zip(Fields, Values)
            )
    end;
make_fields_cond([Field], Value, Ctx) ->
    make_fields_cond([Field], [Value], Ctx);
make_fields_cond(Field, Value, Ctx) ->
    make_fields_cond([Field], [Value], Ctx).

pack_conds(Type, Conds, Ctx) when is_list(Conds) ->
    pack_conds(Type, Conds, [], Ctx);
pack_conds(Type, Cond, Ctx) ->
    pack_conds(Type, [Cond], [], Ctx).

pack_conds(Type, [{Field, Predicate, Value} | T], Acc, Ctx) ->
    pack_conds(Type, T, [{Field, Predicate, pack({Type, Field}, Value, Ctx)} | Acc]);
pack_conds(_, [], Acc, _) ->
    Acc.


%% wrappers
make_select(Type, Fields, Conds, Ctx, Opts) ->
    PackedConds = pack_conds(Type, Conds, Ctx),
    epgsql_utils_sql:select(struct_info(table, Type), Fields, PackedConds, Opts).

make_update_object(Type, Object, Ctx) ->
    PackedObject = pack(Type, Object, Ctx),
    PackedConds  = pack_conds(Type, lists:map(
        fun(KeyPart) -> {KeyPart, '=', proplists:get_value(KeyPart, PackedObject)} end,
        struct_info(keys, Type)
    ), Ctx),
    epgsql_utils_sql:update(struct_info(table, Type), PackedObject, PackedConds).

make_update_object(Type, Object, Conds, Ctx) ->
    PackedObject = pack(Type, Object, Ctx),
    PackedConds  = pack_conds(Type, Conds, Ctx),
    epgsql_utils_sql:update(struct_info(table, Type), PackedObject, PackedConds).

make_update_proplist(Type, FieldsValues, Conds, Ctx) ->
    PackedConds = pack_conds(Type, Conds, Ctx),
    PackedFieldsValues = lists:map( fun({F, V}) -> {F, pack({Type, F}, V, Ctx)} end,
                                    FieldsValues
                                  ),
    epgsql_utils_sql:update(struct_info(table, Type), PackedFieldsValues, PackedConds).

make_delete(Type, Conds, Ctx) ->
    PackedConds = pack_conds(Type, Conds, Ctx),
    epgsql_utils_sql:delete(struct_info(table, Type), PackedConds).


map_selected(MapF, L) ->
    lists:foldr(
        fun(E, Acc) ->
                [MapF(E)|Acc]
        end,
        [],
        L
    ).

pack_record_field({Type, FieldName}, V, Ctx) ->
    FieldType = proplists:get_value(FieldName, struct_info(fields, Type)),
    pack(FieldType, V, Ctx).

pack_record(Type={Mod, LocalType}, V, Ctx) ->
    FieldsTypes = struct_info(fields, Type),
    [_|FieldsValues] = tuple_to_list(V),
    lists:map(
        fun({{FieldName, _}, FieldValue}) ->
            {FieldName, pack({Mod, {LocalType, FieldName}}, FieldValue, Ctx)}
        end,
        lists:zip(FieldsTypes, FieldsValues)
    ).

unpack_record_field({Type, FieldName}, V, Ctx) ->
    FieldType = proplists:get_value(FieldName, struct_info(fields, Type)),
    unpack(FieldType, V, Ctx).

unpack_record(Type={Mod, LocalType}, V, Ctx) ->
    FieldsTypes = struct_info(fields, Type),
    FieldsValues =
        lists:map(
            fun({FieldName, _}) ->
                unpack({Mod, {LocalType, FieldName}}, proplists:get_value(FieldName, V, null), Ctx)
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
pack({?MODULE, T},  V, Ctx) -> pack_(T, V, Ctx);
pack({{Mod, T}, F}, V, Ctx) -> Mod:pack({T, F}, V, Ctx);
pack({Mod, T}, V     , Ctx) -> Mod:pack(T, V, Ctx);
pack(Type, Data      , Ctx) -> error(badarg, [Type, Data, Ctx]).

pack_(_              , undefined     , _  )                                           -> null;
pack_(boolean        , Boolean       , _  ) when is_boolean(Boolean)                  -> Boolean;
pack_(integer        , Int           , _  ) when is_integer(Int)                      -> Int;
pack_(float          , Float         , _  ) when is_float(Float) or is_integer(Float) -> Float;
pack_(string         , Str           , _  ) when is_binary(Str)                       -> Str;
pack_(string         , IOList        , _  ) when is_list(IOList)                      -> iolist_to_binary(IOList);
pack_(datetime       , DateTime      , _  )                                           -> DateTime;
pack_({list          , T}, List      , Ctx) when is_list(List)                        -> [pack(T, E, Ctx) || E <- List];
pack_(binary         , Binary        , _  ) when is_binary(Binary)                    -> Binary;
pack_(binary         , IOList        , _  ) when is_list(IOList)                      -> iolist_to_binary(IOList);
pack_(term           , Term          , _  )                                           -> term_to_binary(Term);
pack_(date           , Date={_, _, _}, _  )                                           -> Date;
pack_(json           , Json          , Ctx) when is_atom(Json)                        -> pack_(atom, Json, Ctx);
pack_(json           , Json          , Ctx) when is_binary(Json)                      -> pack_(binary, Json, Ctx);
pack_(json           , Json          , Ctx) when is_number(Json)                      -> pack_(number, Json, Ctx);
pack_(json           , Json          , _  ) when is_map(Json)                         -> jiffy:encode(Json);
pack_(atom           , Atom          , _  ) when is_atom(Atom)                        -> atom_to_binary(Atom, utf8);
pack_(pos_integer    , PosInteger    , Ctx) when PosInteger > 0                       -> pack_(integer, PosInteger, Ctx);
pack_(neg_integer    , NegInteger    , Ctx) when NegInteger < 0                       -> pack_(integer, NegInteger, Ctx);
pack_(non_neg_integer, NonNegInteger , Ctx) when NonNegInteger >= 0                   -> pack_(integer, NonNegInteger, Ctx);
pack_(number         , Number        , _  ) when is_float(Number); is_integer(Number) -> Number;
pack_(byte           , Byte          , Ctx) when Byte >= 0, Byte =< 255               -> pack_(integer, Byte, Ctx);
pack_(char           , Char          , Ctx) when Char >= 0, Char =< 16#10ffff         -> pack_(integer, Char, Ctx);
pack_(iodata         , IoData        , Ctx)                                           -> pack_(binary, IoData, Ctx);
pack_(iolist         , IoList        , Ctx)                                           -> pack_(binary, IoList, Ctx);

pack_(Type           , Data          , Ctx)                                           -> error(badarg, [Type, Data, Ctx]).


unpack({?MODULE, T},  V, Ctx) -> unpack_(T, V, Ctx);
unpack({{Mod, T}, F}, V, Ctx) -> Mod:unpack({T, F}, V, Ctx);
unpack({Mod, T}, V     , Ctx) -> Mod:unpack(T, V, Ctx);
unpack(Type, Data      , Ctx) -> error(badarg, [Type, Data, Ctx]).

unpack_(_              , null           , _  )                                           -> undefined;
unpack_(boolean        , Boolean        , _  ) when is_boolean(Boolean)                  -> Boolean;
unpack_(integer        , Int            , _  ) when is_integer(Int)                      -> Int;
unpack_(float          , Float          , _  ) when is_float(Float) or is_integer(Float) -> Float;
unpack_(string         , Str            , _  ) when is_binary(Str)                       -> Str;
unpack_(datetime       , {Date, {H,M,S}}, _  )                                           -> {Date, {H,M,erlang:round(S)}};
unpack_({list          , T}, List       , Ctx) when is_list(List)                        -> [unpack(T, E, Ctx) || E <- List];
unpack_(binary         , Binary         , _  ) when is_binary(Binary)                    -> Binary;
unpack_(term           , Term           , _  )                                           -> binary_to_term(Term);
unpack_(date           , Date={_, _, _} , _  )                                           -> Date;
unpack_(json           , Json           , _  )                                           -> jiffy:decode(Json, [return_maps]);
unpack_(atom           , Atom           , _  ) when is_binary(Atom)                      -> binary_to_atom(Atom, utf8) ;
unpack_(pos_integer    , PosInteger     , Ctx) when PosInteger > 0                       -> unpack_(integer, PosInteger, Ctx);
unpack_(neg_integer    , NegInteger     , Ctx) when NegInteger < 0                       -> unpack_(integer, NegInteger, Ctx);
unpack_(non_neg_integer, NonNegInteger  , Ctx) when NonNegInteger >= 0                   -> unpack_(integer, NonNegInteger, Ctx);
unpack_(number         , Number         , _  ) when is_float(Number); is_integer(Number) -> Number;
unpack_(byte           , Byte           , Ctx) when Byte >= 0, Byte =< 255               -> unpack_(integer, Byte, Ctx);
unpack_(char           , Char           , Ctx) when Char >= 0, Char =< 16#10ffff         -> unpack_(integer, Char, Ctx);
unpack_(iodata         , IoData         , Ctx)                                           -> unpack_(binary, IoData, Ctx);
unpack_(iolist         , IoList         , Ctx)                                           -> [unpack_(binary, IoList, Ctx)];

unpack_(Type           , Data           , Ctx)                                           -> error(badarg, [Type, Data, Ctx]).
