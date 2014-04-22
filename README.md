Epgsql utils
============

A collection of tools and helpers built around epgsql.

Epgsql utils model
------------------
Generates code to simplify database records handling.

Here is a short tutorial:

* Include basic model types:

    ```erl
    -module(some_module).
    -include_lib("epgsql_utils/include/epgsql_utils_model.hrl").
    ```
* Declare parse transform:

    ```erl
    -compile({parse_transform, epgsql_utils_model}).
    ```
* Define or include records which is supposed transformed. Type "key" is reserved for primary keys declarations.  Types are taken from **epgsql_utils_orm** by default. Any other module can be specified explicitly.

    ```erl
    -record(example, {
        id::key(pos_integer()),
        name::string(),
        info::some_module:info()
    }).
    ```
* Delcare corresponding table for each record which is supposed to be transformed

    ```erl
    -export_record_model([
    {example, [{table, {db_shema, table_name}}]}
    ]).
    ```
* Declare **pack/2** and **unpack/2** functions. Make sure that all the custom types can be processed too.

    ```erl
    pack(example, V) ->
        Result = some_packing_function();
    pack(T, V) ->
        pack_record(T, V).

    unpack(example, V) ->
        Result = some_unpacking_function();
    unpack(T, V) ->
        unpack_record(T, V).
    ```

* Fin. The records will be parsed and can be used with **epgsql_utils_orm**:

    ```erl
        epgsql_utils_orm:get_by_cond({some_module, example}, [])
    ```
