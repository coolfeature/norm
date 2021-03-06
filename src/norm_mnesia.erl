-module(norm_mnesia).

-compile(export_all).

-behaviour(norm_behaviour).

-define(MODELS,norm_utls:models(mnesia)).

%% ----------------------------------------------------------------------------
%% -------------------------- BEHAVIOUR CALLBACKS -----------------------------
%% ----------------------------------------------------------------------------

init() ->
  create_tables([node()]).

new(Name) ->
  ModelSpec = maps:get(Name,?MODELS,undefined),
  if ModelSpec =:= undefined -> undefined; true ->
  ModelFields = maps:get(<<"fields">>,ModelSpec,#{}),
  NullMap = lists:foldl(fun(Key,Map) -> 
    maps:put(Key, <<"NULL">>, Map)
  end,ModelFields,maps:keys(ModelFields)),
  ModelSpecName = maps:put(<<"name">>,Name,ModelSpec),
  maps:put(<<"__meta__">>,ModelSpecName,NullMap) end.

save(Model) ->
  write(Model).

find(Name,Predicates) when is_map(Predicates) ->
  select(Name,Predicates);
find(Name,Id) ->
  match(Name,Id).

remove(Model) ->
  Name = norm_utls:model_name(Model),
  Id = lists:nth(1,mnesia:table_info(Name,attributes)),
  delete(Name,Id).

models() ->
  Models = ?MODELS,
  if Models=:= #{} -> undefined; true -> Models end.

%% -------------------------------- CREATE ------------------------------------

create_tables(Nodes) ->
  application:load(mnesia),

  %% Set mnesia dir
  MnesiaDir = case norm_utls:get_db_config(mnesia,dir) of
    [] ->
      filename:join(norm_utls:root_dir(),"mnesia_db");
    undefined ->
      filename:join(norm_utls:root_dir(),"mnesia_db");
    Dir ->
      Dir
  end,
  application:set_env(mnesia,dir,MnesiaDir),

  %% Set table persistance
  {Copies,LogMsg} = case norm_utls:get_db_config(mnesia,store) of
    disc_copies ->
      {disc_copies,"on DISC"};
    _ ->
      {ram_copies,"in RAM"}
  end,
  
  %% Create schema
  case Copies of
    disc_copies ->
      case mnesia:create_schema(Nodes) of
	ok ->
	  log:debug("~p~n", ["Schema has been created " ++ LogMsg]);
	{error, {_,{already_exists,_Node}}} ->
	  log:debug("~p~n", ["Schema already exists on node."]);
	{error, Error} ->
	  log:error("~p~n",[{?MODULE, ?LINE, Error}])
      end;
    _ -> ok
  end,
  {ok,MnesiaPath} = application:get_env(mnesia,dir),
  log:info("~p~n", ["Mnesia DIR is: " ++ MnesiaPath]),
  log:info("~p~n", ["Mnesia tables reside " ++ LogMsg]),

  %% Start Mnesia
  application:start(mnesia),

  %% Create tables
  Models = ?MODELS,
  Tables = lists:foldl(fun(TableNameBin,Acc) ->
    TableSpec = maps:get(TableNameBin,Models),
    Type = norm_utls:bin_to_atom(maps:get(<<"type">>,TableSpec,<<"set">>)),
    TableName = norm_utls:bin_to_atom(TableNameBin),
    Fields = fields(TableNameBin),
    Acc ++ [{TableName,[{attributes,Fields}
      ,{Copies,Nodes},{type,Type}]}]
  end,[],maps:keys(Models)),
  create_mnesia_table(Tables,[]).

%% @private {@link create_tables/1}. helper. 

create_mnesia_table([{Name,Atts}|Specs],Created) ->
  TableName = atom_to_list(Name),
  case mnesia:create_table(Name,Atts) of
    {aborted,{already_exists,Table}} ->
      Result = Created ++ [{ok,{already_exists,Table}}],
      log:info("~p~n",[TableName ++ " already exists."]);
    {atomic,ok} -> 
      Result = Created ++ [{ok,{atomic,ok}}],
      log:info("~p~n",["Table " ++ TableName ++ " created."]);
    Error -> 
      Result = Created ++ [{error,Error}],
      log:debug("~p~n",[{"Could not create " ++ TableName,Error}])
  end,
  create_mnesia_table(Specs,Result);
create_mnesia_table([],Created) ->
  mnesia:wait_for_tables(model_names(),5000),
  {ok_error(Created),Created}.

%% ------------------------------- WRITE --------------------------------------

write(Map) when is_map(Map) ->
  Table = norm_utls:bin_to_atom(norm_utls:model_name(Map)),
  RecordTuple = model_to_tuple(Map),  
  Fun = fun() -> mnesia:write(Table,RecordTuple,write) end,
  case mnesia:transaction(Fun) of
    {atomic,ok} -> {ok,RecordTuple};
    {aborted,Reason} -> {error,{aborted,Reason}}
  end;
write(Maps) when is_list(Maps) ->
  Fun = fun() -> 
    [mnesia:write(norm_utls:bin_to_atom(norm_utls:model_name(Map))
      ,model_to_tuple(Map),write) || 
      Map <- Maps] 
  end,
  case mnesia:transaction(Fun) of
    {atomic,Results} -> {ok,Results};
    {aborted,Reason} -> {error,{aborted,Reason}}
  end.


%% ------------------------------- FIND ---------------------------------------

match(Name,Id) ->
  NameAtom = norm_utls:bin_to_atom(Name),
  Fun = fun() -> mnesia:read(NameAtom,Id) end,
  case mnesia:transaction(Fun) of
    {atomic,Result} -> Result;
    {aborted, Reason} -> {error, Reason}
  end.

%% @doc Predicates is a map with with 'where', 'order_by', 'limit' and 'offset'
%% keys. Use match specification [{MatchHead, [Guard], [Result]}] in where 
%% eg: [{'$1',[],['$1']}] to select all records.
%%
%% @todo Add logging.

select(Name,Predicates) ->
  Where = maps:get(where,Predicates,all),
  OrderBy = maps:get(order_by,Predicates,undefined),
  Limit = maps:get(limit,Predicates,undefined),
  Offset = maps:get(offset,Predicates,undefined),
  MatchSpec = if Where =:= [all] -> [{'$1',[],['$1']}]; true -> Where end,
  NameAtom = norm_utls:bin_to_atom(Name),
  RawList = mnesia:dirty_select(NameAtom,MatchSpec),
  SortedList = case RawList of
    [H|_T] ->
      Key = element(1,H),
      ModelList = tuple_to_model(RawList),
      Order = if OrderBy =:= undefined -> [{Key,asc}]; true -> OrderBy end,
      order_by(ModelList,Order);
    _Empty -> []
  end,
  SkippedList = offset(SortedList,Offset),
  limit(SkippedList,Limit).

%% @doc Applies sorting criteria one after another.
%% @todo Make sorting more sophisticated.

order_by(Ordered,[{Field,AscDesc}|OrdersBy]) ->
  case AscDesc of
    asc ->
      Fun = fun (A,B) -> maps:get(Field,A) =< maps:get(Field,B) end,
      Sorted = lists:sort(Fun,Ordered);
    desc ->
      Fun = fun (A,B) -> maps:get(Field,A) >= maps:get(Field,B) end,
      Sorted = lists:sort(Fun,Ordered);
    _ ->
      Sorted = Ordered
    end,
  order_by(Sorted,OrdersBy);
order_by(Ordered,[]) ->
  Ordered.
  
offset(List,undefined) -> 
  List;
offset(List,Skip) when Skip >= length(List) -> 
  [];
offset(List,Skip) -> 
  lists:nthtail(Skip,List).

limit(List,Max) when is_integer(Max) ->
  lists:sublist(List,Max);
limit(List,undeifned) ->
  List;
limit(List,_Limit) ->
  log:debug("~p~n",["Limit expects a number."]),
  List.

%% ------------------------------ DELETE --------------------------------------

%% @doc Delete by key.
%% @todo Add delete by query match.

delete(Name,Id) ->
  NameAtom = norm_utls:bin_to_atom(Name),
  Result = mnesia:dirty_delete(NameAtom,Id),
  {Result,Id}.

%% ----------------------------------------------------------------------------
%% ---------------------------- CONVERTERS ------------------------------------
%% ----------------------------------------------------------------------------

model_to_tuple(Map) ->
  Name = norm_utls:bin_to_atom(norm_utls:model_name(Map)),
  Attributes = mnesia:table_info(Name,attributes),
  Fields = lists:foldl(fun(Key,Acc) ->
    KeyBin = norm_utls:atom_to_bin(Key),
    Acc ++ [maps:get(KeyBin,Map)]
  end,[Name],Attributes),
  list_to_tuple(Fields).

%% @doc This depends on whether the RecordTuple values are in exactly the same 
%% order as fields from mnesia:table_info(Name,attributes).

tuple_to_model(RecordTuples) when is_list(RecordTuples) ->
  lists:foldl(fun(Tuple,Acc) -> Acc ++ [tuple_to_model(Tuple)] 
  end,[],RecordTuples);
tuple_to_model(RecordTuple) ->
  [Name|Fields] = tuple_to_list(RecordTuple),
  Attributes = mnesia:table_info(Name,attributes),
  Results = lists:zip(Attributes,Fields),
  Model = new(norm_utls:atom_to_bin(Name)),
  lists:foldl(fun({Field,Value},Map) ->
    FieldBin = norm_utls:atom_to_bin(Field),
    maps:update(FieldBin,Value,Map)
  end,Model,Results).

%% ----------------------------------------------------------------------------
%% ----------------------------- UTILITIES ------------------------------------
%% ----------------------------------------------------------------------------

fields(Name) ->
  Map = maps:get(Name,?MODELS),
  Fields = maps:get(<<"fields">>,Map),
  FieldsBin = maps:keys(Fields),
  Keys = lists:foldl(fun(FBin,Acc) -> 
    Acc ++ [norm_utls:bin_to_atom(FBin)] end,[],FieldsBin),
  Key = maps:get(<<"key">>,Map,undefined),
  if Key =:= undefined -> Keys; 
  true -> 
    [H|_T] = Keys,
    KeyAtom = norm_utls:bin_to_atom(Key),
    if H =:= KeyAtom -> Keys; 
    true -> 
      RemList = lists:delete(KeyAtom,Keys),
      [KeyAtom] ++ RemList
    end 
  end.

ok_error([{ok,_}|Results]) ->
  ok_error(Results);
ok_error([]) ->
  ok;
ok_error([{error,_}|_Results]) ->
  error.

model_names() ->
  lists:foldl(fun(Bin,Acc) -> Acc ++ [norm_utls:bin_to_atom(Bin)] end,[],
    maps:keys(?MODELS)).

