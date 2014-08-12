-ifndef(_epgsql_utils_model_included).
-define(_epgsql_utils_model_included, yeah).

-type key(T) :: T.

-type date() :: calendar:date().
-type datetime() :: calendar:datetime().

-type json_value() :: 'null' | binary() | boolean() | number() | [json_value()] | json_object().
-type json_object() :: #{atom() | binary() => json_value()}.
-type json() :: json_value().

-endif.
