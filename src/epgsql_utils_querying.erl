-module(epgsql_utils_querying).

%% API
-export([do_query      /1]).
-export([do_query      /2]).
-export([do            /2]).
-export([do_transaction/2]).
-export([do_transaction/4]).

-define(RETRYING_DELAY, 250).

%%
%% API
%%
do_query(Q) ->
    do_query(Q, []).
do_query(Q, Args) when is_list(Q) ->
    do_query(iolist_to_binary(Q), Args);
do_query(Q, Args) when is_binary(Q) ->
    do_equery_(get_and_check_context(), Q, Args).


do(PoolName, Fun) ->
    do_transaction(PoolName, Fun).

do_transaction(PoolName, Fun) ->
    do_transaction(PoolName, Fun, 2 * 3600, 1000).

do_transaction(_PoolName, _Fun, 0, _Delay) ->
    throw({epgsql_error, 'transaction retrying attempts limit hit'});
do_transaction(PoolName, Fun, Retries, Delay) ->
    C = acquire_conn(PoolName),
    try
        transaction('begin'),
        R = Fun(),
        transaction(commit),
        release_conn(PoolName, C),
        R
    catch
        throw:{epgsql_error, {unexpected_error, _}} ->
            release_conn(PoolName, C),
            timer:sleep(Delay),
            do_transaction(PoolName, Fun, Retries-1, Delay);
        T:E ->
            catch transaction(rollback),
            release_conn(PoolName, C),
            erlang:raise(T, E, erlang:get_stacktrace())
    end.

transaction(Action) ->
    case Action of
        'begin' ->
            do_query("BEGIN");
        commit ->
            do_query("COMMIT");
        rollback ->
            do_query("ROLLBACK")
    end.

%% query
do_equery_(C, Q, A) ->
    R = try pgsql:equery(C, Q, A) of
            {ok, _, R_} ->
                R_;
            {ok, R_} ->
                R_;
            {error, {error, _Error, Code, Desc, _Extra}} when
                Code =:= <<"40P01">>;
                Code =:= <<"40001">>;
                Code =:= <<"40003">>
                ->
                throw({epgsql_error, {recoverable_sql_error, Code, Desc}});
            {error, {error, _Error, Code, Desc, _Extra}} ->
                throw({epgsql_error, {unrecoverable_sql_error, Code, Desc}})
        catch
            Type:Error ->
               throw({epgsql_error, {unexpected_error, Error}})
        end,
    R.


%% local
get_and_check_context() ->
    get_conn().

acquire_conn(PoolName) ->
    try
        C = epgsql_utils_conn_pool:acquire_conn(PoolName),
        put_conn(C),
        C
    catch
        exit:{Reason,_} when Reason =:= noproc orelse Reason =:= timeout->
            timer:sleep(?RETRYING_DELAY),
            acquire_conn(PoolName)
    end.

release_conn(PoolName, C) ->
    try
        pop_conn(),
        epgsql_utils_conn_pool:release_conn(PoolName, C)
    catch
        exit:{Reason,_} when Reason =:= noproc orelse Reason =:= timeout -> ok
    end.

%% connection level
get_conn() ->
    case get_context() of
        [C|_] -> C;
        _       -> error('epgsql context is undefined')
    end.

put_conn(C) ->
    put_context(
        case get_context() of
            undefined -> [C];
            Ctx       -> [C|Ctx]
        end
    ).

pop_conn() ->
    put_context(
        case get_context() of
            [_|Ctx] -> Ctx;
            _       -> error('epgsql context is undefined')
        end
    ).

%% context level
get_context() ->
    get(epgsql_utils_querying_context).

put_context(C) ->
    put(epgsql_utils_querying_context, C).
