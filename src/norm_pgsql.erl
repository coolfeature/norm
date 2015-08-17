-module(norm_pgsql).

%-export([
%  init/0
%  ,models/0
%  ,save/1
%  ,delete/1
%]).
%
%-export([
%  squery/1
%  ,squery/2
%  ,equery/2
%  ,equery/3
%]).
%
%-export([
%  transaction/0
%  ,commit/0
%  ,rollback/0
%]).
%
%-export([
%  get_schema/0
%  ,get_pool/0
%]).

-compile(export_all).
-include("include/pgsql.hrl").

-define(SQUERY(Sql),squery(Sql)).
-define(EQUERY(Sql,Args),equery(Sql,Args)).
-define(SCHEMA,get_schema()).
-define(POOL,get_pool()).
-define(LOG(Level,Term),norm_log:log_term(Level,Term)).


%% ----------------------------------------------------------------------------
%% --------------------------------- API --------------------------------------
%% ----------------------------------------------------------------------------

new(Name) ->
  PgSql = #'pgsql'{},
  ModelSpec = maps:get(Name,PgSql#'pgsql'.'models',#{}),
  ModelFields = maps:get('fields',ModelSpec,#{}),
  NullMap = lists:foldl(fun(Key,Map) -> 
    maps:put(Key, <<"NULL">>, Map)
  end,ModelFields,maps:keys(ModelFields)),
  ModelSpecName = maps:put('name',Name,ModelSpec),
  maps:put('__meta__',ModelSpecName,NullMap).

save(Model) ->  
  Model.

delete(Model) ->
  Model.

init() ->
  try
    {ok,start} = transaction()
    ,ensure_schema_exists()
    ,{ok,commit} = commit()
    ,{ok,start} = transaction()
    ,ensure_tables_exist()
    ,{ok,commit} = commit()
  catch Error:Reason ->
    Rollback = rollback(),
    ?LOG(info,[Error,Reason,Rollback])
  end.

models() -> 
  PgSql = #'pgsql'{},
  Models = PgSql#'pgsql'.'models',
  create_rank(Models).

%% ---------------------------- TABLESPACE ------------------------------------

%% @doc Drop tablespace schema.
drop_schema() ->
  drop_schema(get_schema()).

drop_schema(Schema) ->
  drop_schema(Schema,#{cascade => true}).

drop_schema(Schema,Ops) ->
  case schema_exists(Schema) of {ok,true} -> 
    case ?SQUERY(sql_drop_schema(Schema,Ops)) of
      {ok,_,_} -> {ok,{Schema,dropped}}; 
      Error ->
        ?LOG(debug,Error),
        {error,{Schema,Error}}
    end; 
  {ok,false} -> {ok,{Schema,not_exists}} end.

sql_drop_schema(Schema,Opts) ->
  norm_utls:concat_bin([<<"DROP SCHEMA ">>,options_to_sql(ifexists,Opts),
  <<" ">>,norm_utls:atom_to_bin(Schema),<<" ">>,options_to_sql(cascade,Opts),
  <<";">>]).

%% @doc Create tablespace schema.

create_schema() ->
  create_schema(get_schema()).

create_schema(Schema) ->
  case schema_exists(Schema) of {ok,false} -> 
    case ?SQUERY(sql_create_schema(Schema)) of
      {ok,_,_} -> {ok,{Schema,created}}; 
      Error -> 
        ?LOG(debug,Error),
        {error,{Schema,Error}}
    end; 
  {ok,true} -> {ok,{Schema,exists}} end.
    
sql_create_schema(Schema) ->
  sql_create_schema(Schema,#{ ifexists => false }).
sql_create_schema(Schema,Op) ->
  norm_utls:concat_bin([ <<"CREATE SCHEMA ">>,options_to_sql(ifexists,Op)
  ,<<" ">>,norm_utls:atom_to_bin(Schema),<<";">> ]).

schema_exists(Schema) ->
  case ?SQUERY(sql_schema_exists(Schema)) of
    {ok,_ColInfo,[{SchemaBin}]} when is_binary(SchemaBin) -> {ok,true};
    {ok,_ColInfo,[]} -> {ok,false};
    Error -> {error,Error}
  end.

sql_schema_exists(Schema) ->
  norm_utls:concat_bin([ <<"SELECT schema_name FROM information_schema.schemat"
  "a WHERE schema_name = '">>,norm_utls:atom_to_bin(Schema),<<"';">> ]).

ensure_schema_exists() ->
  case schema_exists(get_schema()) of
    {ok,true} -> {ok,exists};
    {ok,false} -> create_schema(get_schema())
  end.

%% ---------------------------------- CREATE ----------------------------------

create_tables() ->
  PgSql = #'pgsql'{}, 
  ModelMap = PgSql#'pgsql'.'models',
  Keys = create_rank(ModelMap),
  Results = lists:foldl(fun(Key,Acc) -> 
    Acc ++ [create_table(Key)]
  end,[],Keys),
  {ok_error(Results),Results}.

create_table(Name) ->
  PgSql = #'pgsql'{}, 
  ModelMap = PgSql#'pgsql'.'models',
  create_table(Name,maps:get(Name,ModelMap)).

create_table(Name,Spec) when is_map(Spec) ->
  case ?SQUERY(sql_create_table(Name,Spec)) of
    {ok,_,_} -> {ok,{Name,created}};
    Error -> 
      ?LOG(debug,Error),
      {error,{Name,Error}}
  end.

sql_create_table(Name,Spec) ->
  sql_create_table(Name,Spec,#{ifexists => false}).

sql_create_table(Name,Spec,Options) ->
  TableName = table_name(Name),
  FieldSpec = maps:get('fields',Spec), 
  Fields = field_to_sql(FieldSpec),
  Constraints = constraint_to_sql(Spec),
  FieldsConstraints = strip_comma(norm_utls:concat_bin([Fields,Constraints])),
  norm_utls:concat_bin([<<"CREATE TABLE ">>,options_to_sql(ifexists,Options),<<" ">>
    ,TableName,<<" ( ">>,FieldsConstraints,<<" )">>]).
 
field_to_sql(FieldsSpec) ->
  Keys = maps:keys(FieldsSpec),
  lists:foldl(fun(Key,Acc) ->
    FieldSpec = maps:get(Key,FieldsSpec),
    FldSqlLine = field_to_sql(Key,FieldSpec),     
    norm_utls:concat_bin([ <<",">>, FldSqlLine, Acc ]) 
  end,<<"">>,Keys).

 %% @doc Get something like 'customer varchar(50) not null'

field_to_sql(Name,FldSpec) ->
  Type = maps:get('type',FldSpec,undefined),
  TypeLine = case Type of
    'varchar' ->
      Length = maps:get('length',FldSpec,50),
      norm_utls:concat_bin([<<" VARCHAR(">>,norm_utls:num_to_bin(Length),
        <<")">>]);
    'bigserial' ->
      <<" BIGSERIAL ">>;
    'date' ->
      <<" DATE ">>;
    'timestamp' -> 
      <<" TIMESTAMP ">>;
    'bigint' -> 
      <<" BIGINT ">>;
    undefined -> 
      <<"">>
  end,
  Null = case maps:get('null',FldSpec,undefined) of
    undefined -> <<" NULL ">>;
    false -> <<" NOT NULL ">>;
    true -> <<" NULL ">>
  end,
  norm_utls:concat_bin([norm_utls:atom_to_bin(Name),<<" ">>,TypeLine,Null]).

%% @doc

constraint_to_sql(TableSpec) ->
  TableConstraints = maps:get('constraints',TableSpec,#{}),
  lists:foldl(fun(Key,Acc) ->
    ConstraintSpec = maps:get(Key,TableConstraints,undefined),
    ConstraintSql = constraint_to_sql(Key,ConstraintSpec),
    norm_utls:concat_bin([ Acc,ConstraintSql ])
  end,<<"">>,maps:keys(TableConstraints)).

%% @doc There can only be one PK defined. The Constraint Spec is a Map.

constraint_to_sql('pk',ConstraintSpec) ->
  Name = maps:get('name',ConstraintSpec,undefined),
  ConName = if Name =:= undefined -> constraint_name(['pk']); 
    true -> norm_utls:atom_to_bin(Name) end,
  Fields = lists:foldl(fun(Field,AccF) ->
    norm_utls:concat_bin([ <<",">>,norm_utls:atom_to_bin(Field),AccF])
  end,<<"">>,maps:get('fields',ConstraintSpec,[])),
  FieldsS = strip_comma(Fields),
  norm_utls:concat_bin([ <<",">>,<<"CONSTRAINT ">>,ConName,<<" PRIMARY KEY (">>
    ,FieldsS,<<")">> ]);

constraint_to_sql('fk',ConstraintSpecList) ->
  lists:foldl(fun(ConstraintSpec,Acc) ->
    Fields = lists:foldl(fun(Field,AccF) ->
        norm_utls:concat_bin([ AccF,norm_utls:atom_to_bin(Field),<<",">>])
      end,<<"">>,maps:get('fields',ConstraintSpec,[])),
    FieldsS = strip_comma(Fields),
    References = maps:get('references',ConstraintSpec,[]),
    RefFieldsS = fields_sql(References),
    RefTable = maps:get('table',References),
    RefTableName = table_name(RefTable),
    Name = maps:get('name',ConstraintSpec,undefined),
    ConName = if Name =:= undefined -> constraint_name(['fk',RefTable]); 
      true -> norm_utls:atom_to_bin(Name) end,
    Options = maps:get('options',ConstraintSpec,<<"">>),
    norm_utls:concat_bin([ <<",">>,<<"CONSTRAINT ">>,ConName,
      <<" FOREIGN KEY (">>,FieldsS,<<") REFERENCES ">>,RefTableName,<<" (">>,
      RefFieldsS,<<") ">>,Options,Acc ])
  end,<<"">>,ConstraintSpecList).

drop_tables() ->
  PgSql = #'pgsql'{}, 
  ModelMap = PgSql#'pgsql'.'models',
  Keys = lists:reverse(create_rank(ModelMap)),
  Results = lists:foldl(fun(Key,Acc) -> 
    Acc ++ [drop_table(Key)]
  end,[],Keys),
  {ok_error(Results),Results}.
 
drop_table(Table) ->
  Sql = sql_drop_table(Table,#{ifexists => true,cascade => true}),
  case ?SQUERY(Sql) of
    {ok,_,_} -> {ok,{Table,dropped}};
    Error -> 
      ?LOG(debug,Error),
      {error,{Table,Error}}
  end. 

sql_drop_table(Table,Op) ->
  norm_utls:concat_bin([<<"DROP TABLE ">>
    ,options_to_sql(ifexists,Op),table_name(Table)
    ,options_to_sql(cascade,Op),<<";">>]).

table_exists(Name) ->
  case ?SQUERY(sql_table_exists(Name)) of
    {ok,_ColInfo,[{<<"t">>}]} -> {ok,true};
    {ok,_ColInfo,[{<<"f">>}]} -> {ok,false};
    Error -> {error,Error}
  end.

sql_table_exists(Table) ->
  norm_utls:concat_bin([ <<"select exists ( select 1 from "
    "information_schema.tables where table_schema = '">>,
    norm_utls:atom_to_bin(get_schema()),<<"' and table_name = '">>,
    norm_utls:atom_to_bin(Table),<<"');">> ]).

ensure_tables_exist() ->
  lists:foldl(fun(Table,Acc) ->
    Result = case table_exists(Table) of
      {ok,true} -> {ok,exists};
      {ok,false} -> create_table(Table)
    end,
    Acc ++ [Result]
  end,[],models()).

%% ------------------------------- INSERT -------------------------------------

insert(ModelMap) ->
  insert(ModelMap,#{ returning => id }).

insert(ModelMap,Ops) ->
  Sql = sql_insert(ModelMap,Ops), 
  case ?SQUERY(Sql) of
    {ok,_Count,_Cols,[{Id}]} -> {ok,norm_utls:bin_to_num(Id)};
    Error -> {error,Error}
  end. 
 
sql_insert(ModelMap,Ops) -> 
  ModelSpec = maps:get('__meta__',ModelMap,#{}),
  ModelName = maps:get('name',ModelSpec),
  {Fields,Values} = lists:foldl(fun(Key,{Fs,Vs}) -> 
    case Key of
      '__meta__' -> {Fs,Vs};
      _ -> 
        Fs2 = norm_utls:concat_bin([ Fs,norm_utls:atom_to_bin(Key),<<",">>]),
        %%
        MapVal = maps:get(Key,ModelMap),
        MetaFields = maps:get('fields',ModelSpec),
        MetaVal = maps:get(Key,MetaFields),
        MetaValType = maps:get('type',MetaVal),
        Quoted = if MapVal =:= <<"NULL">> -> false; true -> quoted(MetaValType) end, 
        Formatted = norm_utls:val_to_bin(MapVal),
        Vs2 = case Quoted of
          true ->
            norm_utls:concat_bin([ Vs,<<"'">>,Formatted,<<"'">>,<<",">>]);
          false ->
            norm_utls:concat_bin([ Vs,Formatted,<<",">>]) 
        end,
        {Fs2,Vs2}
    end
  end,{<<"">>,<<"">>},maps:keys(ModelMap)),
  FieldsS = strip_comma(Fields),
  ValuesS = strip_comma(Values),
  norm_utls:concat_bin([<<"INSERT INTO ">>,table_name(ModelName),<<" ( ">>
    ,FieldsS,<<" ) VALUES ( ">>,ValuesS,<<" ) ">>
    ,options_to_sql(returning,Ops)]).
 
%% ------------------------------- SELECT -------------------------------------

select(Name,Id) when is_integer(Id) ->
  select(Name,#{ where => [{'id','=',Id}],order => any,limit => 50,offset => 0}).

select(Name,Where) when is_map(Where) ->
  Sql = select_sql(Name,Where),
  case ?SQUERY(Sql) of
    {ok,Cols,Vals} ->
      select_to_model(Name,Cols,Vals);
    Error -> {error,Error}
  end.

select_sql(Name,Where) ->
  norm_utls:concat_bin([<<"SELECT * FROM ">>,table_name(Name), where(Name,Where) ++ order_by(OrderBy)
  ++ case Limit of all -> ""; _ -> " LIMIT " ++ value_to_string(Limit) end 
  ++ case Offset of 0 -> ""; _ -> " OFFSET " ++ value_to_string(Offset) end.]).

select_to_model(Name,Cols,Vals) ->
  Model = new(Name).

%% ----------------------------------------------------------------------------
%% ----------------------------------------------------------------------------
%% ----------------------------------------------------------------------------

squery(Sql) ->
  squery(?POOL,Sql).
squery(PoolName,Sql) ->
  ?LOG(debug,Sql),
  poolboy:transaction(PoolName,fun(Worker) ->
    gen_server:call(Worker,{squery,Sql})
  end).

equery(Sql,Params) ->
  equery(?POOL,Sql,Params).
equery(PoolName,Sql,Params) ->
  ?LOG(debug,[Sql,Params]),
  poolboy:transaction(PoolName,fun(Worker) ->
    gen_server:call(Worker,{equery,Sql,Params})
  end).


%% ----------------------------------------------------------------------------
%% ----------------------------------------------------------------------------
%% ----------------------------------------------------------------------------

transaction() ->
  case ?SQUERY(sql_transaction()) of
    {ok,[],[]} -> {ok,start};
    Error -> {error,Error}
  end.

sql_transaction() ->
  <<"BEGIN;\n">>.

commit() ->
  case ?SQUERY(sql_commit()) of
    {ok,[],[]} -> {ok,commit};
    Error -> {error,Error}
  end.

sql_commit() ->
  <<"COMMIT;">>.

rollback() ->
  case ?SQUERY(sql_rollback()) of
    {ok,[],[]} -> {ok,rollback};
    Error -> {error,Error}
  end.

sql_rollback() ->
  <<"ROLLBACK;">>.

%% ----------------------------------------------------------------------------

option_to_sql({returning,What}) ->
  norm_utls:concat_bin([<<" RETURNING ">>,norm_utls:atom_to_bin(What),<<" ">>]);
option_to_sql({cascade,true}) -> 
  <<" CASCADE ">>;
option_to_sql({ifexists,true}) ->
  <<" IF EXISTS ">>; 
option_to_sql({ifexists,false}) ->
  <<" IF NOT EXISTS ">>;
option_to_sql({_Key,undefined}) ->
  <<"">>.

options_to_sql(Key,OptionMap) ->
 Val = maps:get(Key,OptionMap,undefined),
 option_to_sql({Key,Val}). 

%% ----------------------------------------------------------------------------
%% ----------------------------- UTILITIES ------------------------------------
%% ----------------------------------------------------------------------------

get_schema() ->
  case norm_utls:get_db_config(pgsql,tablespace) of
    undefined -> norm;
    Schema -> Schema
  end.

constraint_name(NameTokens) when is_list(NameTokens) ->
  NamePart = lists:foldl(fun(E,Acc) -> 
    Name = case is_atom(E) of 
      true -> norm_utls:atom_to_bin(E);
      false -> E
    end,
    norm_utls:concat_bin([ Acc,Name,<<"_">> ])
  end,<<"">>,NameTokens),
  {_,_,Secs} = os:timestamp(),
  norm_utls:concat_bin([NamePart,norm_utls:num_to_bin(Secs)]).  
  
create_rank(Map) ->
  Keys = maps:keys(Map),
  List = lists:foldl(fun(E,Acc) -> 
    Table = maps:get(E,Map),
    RankVal = maps:get(create_rank,Table,undefined),
    Rank = if RankVal =:= undefined -> 0; true -> RankVal end,
    Acc ++ [{Rank,E}]
  end, [], Keys),
  Sorted = lists:sort(List),
  [Val || {_,Val} <- Sorted].

fields_sql(Map) ->
  RefFields = lists:foldl(fun(Field,AccRF) ->
    norm_utls:concat_bin([ AccRF,norm_utls:atom_to_bin(Field),<<",">>]) 
  end,<<"">>,maps:get('fields',Map)),
  strip_comma(RefFields).

quoted(Val) ->
  lists:member(Val,['varchar','timestamp','date']).

table_name(TableName) when is_atom(TableName) ->
  Name = norm_utls:atom_to_bin(TableName),
  Schema = norm_utls:atom_to_bin(get_schema()),
  norm_utls:concat_bin([ Schema, <<".">>, Name ]).

get_pool() ->
  Pools = norm_utls:get_db_config(pgsql,pools),
  {Name,_} = lists:nth(1,Pools),
  Name.

strip_comma(Binary) ->
  String = binary_to_list(Binary),
  Stripped = string:strip(string:strip(String,both,$ ),both,$,),
  list_to_binary(Stripped).

ok_error([{ok,_}|Results]) ->
  ok_error(Results);
ok_error([]) ->
  ok;
ok_error([{error,_}|_Results]) ->
  error.


