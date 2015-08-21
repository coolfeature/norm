-module(norm_SUITE).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

begin_test() ->
  norm_app:ensure_started(norm).

%% @private Init creates schemas and tables if they do not exist.

init_default_test() ->
  ?assertMatch({ok,_},norm:init()).

init_pgsql_test() ->
  ?assertMatch({ok,_},norm:init(pgsql)).

init_mnesia_test() ->
  ?assertMatch({ok,_},norm:init(mnesia)).

%% @private Save tests 

save_pgsql_new_test() ->  
  User = norm:new('pgsql',<<"user">>),
  User1 = maps:update(<<"id">>,1,User),
  User2 = maps:update(<<"email">>,<<"szymon.czaja@kfis.co.uk">>,User1),
  User3 = maps:update(<<"password">>,<<"Password">>,User2),
  ?assertMatch({ok,_},norm:save('pgsql',User3)).

save_pgsql_existing_test() ->  
  User = norm:new('pgsql',<<"user">>),
  User1 = maps:update(<<"id">>,1,User),
  User2 = maps:update(<<"email">>,<<"szymon.czaja@kfis.co.uk">>,User1),
  User3 = maps:update(<<"password">>,<<"Password">>,User2),
  ?assertMatch({ok,_},norm:save('pgsql',User3)).

%% @private Delete tests 

remove_pgsql_test() ->
  User = norm:new('pgsql',<<"user">>),
  User1 = maps:update(<<"id">>,1,User),
  ?assertMatch({ok,1},norm:remove('pgsql',User1)).


