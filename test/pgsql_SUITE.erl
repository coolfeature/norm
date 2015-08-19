-module(pgsql_SUITE).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

-define(TEST_MODULE,norm_pgsql).

begin_test() ->
  norm_app:ensure_started(norm).

%% @private Init creates schemas and tables if they do not exist.

init_test() ->
  ?assertMatch({ok,_},norm_pgsql:init()).

%% @private Insert test 

insert_new_test() ->  
  User = norm_pgsql:new('user'),
  User1 = maps:update('id',1,User),
  User2 = maps:update('email',<<"szymon.czaja@kfis.co.uk">>,User1),
  User3 = maps:update('password',<<"Password">>,User2),
  ?assertMatch({ok,_},norm_pgsql:insert(User3)).

insert_existing_test() ->  
  User = norm_pgsql:new('user'),
  User1 = maps:update('id',1,User),
  User2 = maps:update('email',<<"szymon.czaja@kfis.co.uk">>,User1),
  User3 = maps:update('password',<<"Password">>,User2),
  ?assertMatch({error,_},norm_pgsql:insert(User3)).

%% @private Select test

select_by_id_test() ->
  ?assertMatch([_User],norm_pgsql:select(user,1)).

select_by_where_test() ->
  ?assertMatch([_User],norm_pgsql:select(user,#{ where => [{'password','LIKE',"%ass%"}]})).

%% @private Update test

update_test() ->
  User = norm_pgsql:new('user'),
  User1 = maps:update('id',1,User),
  User2 = maps:update('password',<<"NewPassword">>,User1),
  {ok,_Id} = norm_pgsql:update(User2),
  [Model] = norm_pgsql:select(user,1),
  ?assertMatch(<<"NewPassword">>,maps:get(password,Model)).

%% @private Delete test 

delete_test() ->
  ?assertMatch({ok,1},norm_pgsql:delete(user,1)).

%% @private Drop tables

drop_tables_test() ->
  {_Result,TableResults} = norm_pgsql:drop_tables(),
  ?assertMatch({ok,TableResults},norm_pgsql:drop_tables()),
  lists:foldl(fun(TableDropResult,_Acc) -> 
    ?assertMatch({ok,{_Table,dropped}},TableDropResult)
  end,[],TableResults).

%% @private Drop schema

drop_schema_test() ->
  Schema = norm_pgsql:get_schema(),
  ?debugFmt("Dropping schema ~p~n", [Schema]),
  ?assertMatch({ok,{Schema,dropped}},norm_pgsql:drop_schema()).

