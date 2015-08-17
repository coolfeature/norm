-module(norm_test).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

-define(TEST_MODULE,norm_pgsql).

begin_test() ->
  ok = application:start(norm).

%% ----------------------------------------------------------------------------
%% ----------------------------- INIT -----------------------------------------
%% ----------------------------------------------------------------------------

init_test() ->
%%  Dbs = norm_utls:get_config(dbs),
%%  Pgsql = norm_utls:get_value(pgsql,Dbs,[]),
%%  Schema = norm_utls:get_value(tablespace,Pgsql,norm_test),
%%  ?debugFmt("Old Schema: ~p~n",[Schema]),
%%  norm_utls:set_db_config(pgsql,tablespace,norm_pgsql_test),
%%  Dbs2 = norm_utls:get_config(dbs),
%%  Pgsql2 = norm_utls:get_value(pgsql,Dbs2,[]),
%%  Schema2 = norm_utls:get_value(tablespace,Pgsql2,norm_test),
%%  ?debugFmt("New Schema: ~p~n",[Schema2]),
  ?assertMatch({ok,_},norm_pgsql:init()).

%% ----------------------------------------------------------------------------
%% ---------------------------- INSERT ----------------------------------------
%% ----------------------------------------------------------------------------

insert_test() ->  

insert_test() ->  
  User = norm_pgsql:new('user'),
  User1 = maps:update('id',1,User),
  User2 = maps:update('email',<<"szymon.czaja@kfis.co.uk">>,User1),
  User3 = maps:update('password',<<"Password">>,User2),
  ?assertMatch({ok,_},norm_pgsql:insert(User3)).

