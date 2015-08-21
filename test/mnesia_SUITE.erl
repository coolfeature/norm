-module(mnesia_SUITE).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

-define(TEST_MODULE,norm_mnesia).

begin_test() ->
  norm_app:ensure_started(norm).

%% @private Init creates schemas and tables if they do not exist.

init_test() ->
  ?assertMatch({ok,_},norm_mnesia:init()).

%% @private Insert test 

write_new_test() ->  
  Visit = norm_mnesia:new(<<"views">>),
  Visit1 = maps:update(<<"id">>,1,Visit),
  Visit2 = maps:update(<<"visits">>,23,Visit1),
  Visit3 = maps:update(<<"reviews">>,[#{}],Visit2),
  Visit4 = maps:update(<<"purchases">>,[#{}],Visit3),
  ?assertMatch({ok,_},norm_mnesia:write(Visit4)).

%% @private Select test

match_by_id_test() ->
  ?assertMatch([_Visit],norm_mnesia:match(views,1)).

select_by_where_test() ->
  ?assertMatch([_Visit],norm_mnesia:select(views,#{ where => [{'$1',[],['$1']}]})).

%% @private Delete test 

delete_test() ->
  ?assertMatch({ok,1},norm_mnesia:delete(views,1)).

