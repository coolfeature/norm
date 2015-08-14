-module(norm).

-export([
  init/0
  ,init/1
]).

init() ->
  init(norm_utls:get_module()).

init(DbName) ->
  M = norm_utls:get_module(DbName),
  M:init().
