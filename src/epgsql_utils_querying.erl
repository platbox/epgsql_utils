-module(epgsql_utils_querying).

%% API
-export([do_query      /1]).
-export([do_query      /2]).
-export([do            /2]).
-export([do_transaction/2]).
-export([do_transaction/4]).

-define(RETRY_DELAY       , 1000).
-define(RETRY_SWEAR_AFTER , 3).

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
    check_context_is_empty(),
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
            lager:warning("epgsql connection error, retrying after ~p ms...", [Delay]),
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
    lager:debug("DB equery: \"~s\", ~p", [Q, A]),
    R = try pgsql:equery(C, Q, A) of
            {ok, _N, _Cols, R_} ->
                R_;
            {ok, _Cols, R_} ->
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
                lager:warning("epgsql query unexpected error ~p:~p", [Type, Error]),
                lager:debug("epgsql query unexpected error ~p:~p ~n ** ~p", [Type, Error, erlang:get_stacktrace()]),
                throw({epgsql_error, {unexpected_error, Error}})
        end,
    lager:debug("DB equery results: ~p", [R]),
    R.


%% local
get_and_check_context() ->
    case get_context() of
        undefined -> error('epgsql query runs without do context');
        V         -> V
    end.

check_context_is_empty() ->
    case get_context() of
        undefined -> ok;
        C         -> exit({context_is_not_empty, C})
    end.

get_context() ->
    get(epgsql_utils_querying_context).

put_context(C) ->
    put(epgsql_utils_querying_context, C).

acquire_conn(PoolName) ->
    acquire_conn(PoolName, 1).

acquire_conn(PoolName, N) ->
    try
        C = epgsql_utils_conn_pool:acquire_conn(PoolName),
        put_context(C),
        C
    catch
        exit:{Reason,_} when Reason =:= noproc orelse Reason =:= timeout ->
            N > ?RETRY_SWEAR_AFTER andalso
                lager:error("could not acquire connection after ~B attempts, keep trying ...", [N]),
            timer:sleep(?RETRY_DELAY),
            acquire_conn(PoolName, N + 1)
    end.

release_conn(PoolName, C) ->
    try
        put_context(undefined),
        epgsql_utils_conn_pool:release_conn(PoolName, C)
    catch
        exit:{Reason,_} when Reason =:= noproc orelse Reason =:= timeout -> ok
    end.

