-ifndef(_epgsql_utils_model_included).
-define(_epgsql_utils_model_included, yeah).

-type key(T) :: T.
-type date() :: calendar:date().
-type datetime() :: calendar:datetime().
-type value () :: 'null' | binary() | boolean() | number() | [value()] | object().
-type object() :: {[{binary(), value()}]}.
-type json  () :: [value()] | object().

-endif.