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

-behaviour(norm_behaviour).

-compile(export_all).

-define(SQUERY(Sql),squery(Sql)).
-define(EQUERY(Sql,Args),equery(Sql,Args)).
-define(SCHEMA,get_schema()).
-define(POOL,get_pool()).
-define(LOG(Level,Term),norm_log:log_term(Level,Term)).
-define(MODELS,norm_utls:models(pgsql)).

%% ----------------------------------------------------------------------------
%% ------------------------- BEHAVIOUR CALLBACKS ------------------------------
%% ----------------------------------------------------------------------------

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
    ?LOG(info,[Error,Reason,Rollback]),
    Rollback
  end.

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
  %% @todo id is hardcoded
  Id = maps:get(<<"id">>,Model,undefined),
  Name = norm_utls:model_name(Model),
  case no_value(Id) of 
    false ->
      case select(Name,Id) of
        [_H|_T] -> update(Model);
        [] -> insert(Model)
      end;
    true ->
      insert(Model)
  end.

find(Name,Predicates) when is_map(Predicates) ->
  select(Name,Predicates);
find(Name,Id) ->
  select(Name,Id).
  
remove(Model) ->
  Name = norm_utls:model_name(Model),
  Id = maps:get(<<"id">>,Model),
  delete(Name,Id).

models() -> 
  create_rank(?MODELS).

%% ---------------------------- TABLESPACE ------------------------------------

%% @doc Drop tablespace schema.
drop_schema() ->
  drop_schema(get_schema()).

drop_schema(Schema) ->
  drop_schema(Schema,#{ <<"cascade">> => <<"true">>}).

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
  norm_utls:concat_bin([<<"DROP SCHEMA ">>,options_to_sql(<<"ifexists">>,Opts),
  <<" ">>,Schema,<<" ">>,options_to_sql(<<"cascade">>,Opts),
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
  Op = case server_version() of
    {ok,Version} -> 
      if Version < 9.3 -> #{}; 
      true -> #{ <<"ifexists">> => <<"false">> } end; 
    _Error -> #{}
  end,
  sql_create_schema(Schema,Op).
sql_create_schema(Schema,Op) ->
  norm_utls:concat_bin([ <<"CREATE SCHEMA ">>,options_to_sql(<<"ifexists">>,Op)
  ,<<" ">>,Schema,<<";">> ]).

schema_exists(Schema) ->
  case ?SQUERY(sql_schema_exists(Schema)) of
    {ok,_ColInfo,[{SchemaBin}]} when is_binary(SchemaBin) -> {ok,true};
    {ok,_ColInfo,[]} -> {ok,false};
    Error -> {error,Error}
  end.

sql_schema_exists(Schema) ->
  norm_utls:concat_bin([ <<"SELECT schema_name FROM information_schema.schemat"
  "a WHERE schema_name = '">>,Schema,<<"';">> ]).

ensure_schema_exists() ->
  case schema_exists(get_schema()) of
    {ok,true} -> {ok,exists};
    {ok,false} -> create_schema(get_schema())
  end.

%% ---------------------------------- CREATE ----------------------------------

create_tables() ->
  Keys = create_rank(?MODELS),
  Results = lists:foldl(fun(Key,Acc) -> 
    Acc ++ [create_table(Key)]
  end,[],Keys),
  {ok_error(Results),Results}.

create_table(Name) ->
  create_table(Name,maps:get(Name,?MODELS)).

create_table(Name,Spec) when is_map(Spec) ->
  case ?SQUERY(sql_create_table(Name,Spec)) of
    {ok,_,_} -> {ok,{Name,created}};
    Error -> 
      ?LOG(debug,Error),
      {error,{Name,Error}}
  end.

sql_create_table(Name,Spec) ->
  sql_create_table(Name,Spec,#{ <<"ifexists">> => <<"false">> }).

sql_create_table(Name,Spec,Options) ->
  TableName = table_name(Name),
  FieldSpec = maps:get(<<"fields">>,Spec), 
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
  Type = maps:get(<<"type">>,FldSpec,undefined),
  TypeLine = case Type of
    <<"varchar">> ->
      Length = maps:get(<<"length">>,FldSpec,<<"50">>),
      norm_utls:concat_bin([<<" VARCHAR(">>,Length,<<")">>]);
    <<"bigserial">> ->
      <<" BIGSERIAL ">>;
    <<"date">> ->
      <<" DATE ">>;
    <<"timestamp">> -> 
      <<" TIMESTAMP ">>;
    <<"bigint">> -> 
      <<" BIGINT ">>;
    undefined -> 
      <<"">>
  end,
  Null = case maps:get(<<"null">>,FldSpec,undefined) of
    undefined -> <<" NULL ">>;
    <<"false">> -> <<" NOT NULL ">>;
    <<"true">> -> <<" NULL ">>
  end,
  norm_utls:concat_bin([Name,<<" ">>,TypeLine,Null]).

%% @doc

constraint_to_sql(TableSpec) ->
  TableConstraints = maps:get(<<"constraints">>,TableSpec,#{}),
  lists:foldl(fun(Key,Acc) ->
    ConstraintSpec = maps:get(Key,TableConstraints,undefined),
    ConstraintSql = constraint_to_sql(Key,ConstraintSpec),
    norm_utls:concat_bin([ Acc,ConstraintSql ])
  end,<<"">>,maps:keys(TableConstraints)).

%% @doc There can only be one PK defined. The Constraint Spec is a Map.

constraint_to_sql(<<"pk">>,ConstraintSpec) ->
  Name = maps:get(<<"name">>,ConstraintSpec,undefined),
  ConName = if Name =:= undefined -> constraint_name([<<"pk">>]); true -> Name end,
  Fields = lists:foldl(fun(Field,AccF) ->
    norm_utls:concat_bin([ <<",">>,Field,AccF])
  end,<<"">>,maps:get(<<"fields">>,ConstraintSpec,[])),
  FieldsS = strip_comma(Fields),
  norm_utls:concat_bin([ <<",">>,<<"CONSTRAINT ">>,ConName,<<" PRIMARY KEY (">>
    ,FieldsS,<<")">> ]);

%% @doc There may be multiple FKs defined.

constraint_to_sql(<<"fk">>,ConstraintSpecList) ->
  lists:foldl(fun(ConstraintSpec,Acc) ->
    FieldsS = fields_sql(ConstraintSpec),
    %Fields = lists:foldl(fun(Field,AccF) ->
    %    norm_utls:concat_bin([ AccF,Field,<<",">>])
    %  end,<<"">>,maps:get(<<"fields">>,ConstraintSpec,[])),
    %FieldsS = strip_comma(Fields),
    References = maps:get(<<"references">>,ConstraintSpec,[]),
    RefFieldsS = fields_sql(References),
    RefTable = maps:get(<<"table">>,References),
    RefTableName = table_name(RefTable),
    Name = maps:get(<<"name">>,ConstraintSpec,undefined),
    ConName = if Name =:= undefined -> constraint_name([<<"fk">>,RefTable]); 
      true -> norm_utls:atom_to_bin(Name) end,
    Options = maps:get(<<"options">>,ConstraintSpec,<<"">>),
    norm_utls:concat_bin([ <<",">>,<<"CONSTRAINT ">>,ConName,
      <<" FOREIGN KEY (">>,FieldsS,<<") REFERENCES ">>,RefTableName,<<" (">>,
      RefFieldsS,<<") ">>,Options,Acc ])
  end,<<"">>,ConstraintSpecList);

%% @doc There may be multiple UNIQUEs defined.

constraint_to_sql(<<"unique">>,ConstraintSpecList) ->
  lists:foldl(fun(ConstraintSpec,Acc) ->
    %Fields = lists:foldl(fun(Field,AccF) ->
    %    norm_utls:concat_bin([ AccF,Field,<<",">>])
    %  end,<<"">>,maps:get(<<"fields">>,ConstraintSpec,[])),
    %FieldsS = strip_comma(Fields),
    FieldsS = fields_sql(ConstraintSpec),
    Name = maps:get(<<"name">>,ConstraintSpec,undefined),
    ConName = if Name =:= undefined -> constraint_name([<<"unique">>]); 
      true -> norm_utls:atom_to_bin(Name) end,
    norm_utls:concat_bin([ <<",">>,<<"CONSTRAINT ">>,ConName,
      <<" UNIQUE (">>,FieldsS,<<") ">>,Acc ])
  end,<<"">>,ConstraintSpecList).

drop_tables() ->
  Keys = lists:reverse(create_rank(?MODELS)),
  Results = lists:foldl(fun(Key,Acc) -> 
    Acc ++ [drop_table(Key)]
  end,[],Keys),
  {ok_error(Results),Results}.
 
drop_table(Table) ->
  Sql = sql_drop_table(Table,#{ <<"ifexists">> => <<"true">>,<<"cascade">> => <<"true">>}),
  case ?SQUERY(Sql) of
    {ok,_,_} -> {ok,{Table,dropped}};
    Error -> 
      ?LOG(debug,Error),
      {error,{Table,Error}}
  end. 

sql_drop_table(Table,Op) ->
  norm_utls:concat_bin([<<"DROP TABLE ">>
    ,options_to_sql(<<"ifexists">>,Op),table_name(Table)
    ,options_to_sql(<<"cascade">>,Op),<<";">>]).

table_exists(Name) ->
  case ?SQUERY(sql_table_exists(Name)) of
    {ok,_ColInfo,[{<<"t">>}]} -> {ok,true};
    {ok,_ColInfo,[{<<"f">>}]} -> {ok,false};
    Error -> {error,Error}
  end.

sql_table_exists(Table) ->
  norm_utls:concat_bin([ <<"select exists ( select 1 from "
    "information_schema.tables where table_schema = '">>,
    get_schema(),<<"' and table_name = '">>,
    Table,<<"');">> ]).

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
  %% @todo id is hardcoded
  Id = maps:get(<<"id">>,ModelMap,undefined),
  if Id =:= undefined -> insert(ModelMap,#{});
  true -> insert(ModelMap,#{ <<"returning">> => <<"id">> }) end.

insert(ModelMap,Ops) ->
  Sql = sql_insert(ModelMap,Ops), 
  case ?SQUERY(Sql) of
    {ok,_Count,_Cols,[{Id}]} -> {ok,norm_utls:bin_to_num(Id)};
    {ok,Id} -> {ok,Id};
    {error,{error,error,<<"23505">>,Info,Detail}} -> {error,{<<"23505">>,Info,Detail}};
    Error -> {error,{<<"Unmatched Result">>,Error}}
  end. 

%% @doc Insert should insert id value only when it is specified, otherwise 
%% if the id field is a primary key, postgress should automatically 
%% update it.
 
sql_insert(ModelMap,Ops) -> 
  ModelSpec = maps:get(<<"__meta__">>,ModelMap,#{}),
  ModelName = maps:get(<<"name">>,ModelSpec),
  ModelFullSpec = maps:get(ModelName,?MODELS),
  ModelConstraints = maps:get(<<"constraints">>,ModelFullSpec),
  Pk = case maps:get(<<"pk">>,ModelConstraints,undefined) of
    undefined -> undefined; Map -> maps:get(<<"fields">>,Map,undefined) end,
  {Fields,Values} = lists:foldl(fun(Key,{Fs,Vs}) -> 
    case Key of
      <<"__meta__">> -> {Fs,Vs};
      _ ->
        MapVal = maps:get(Key,ModelMap,undefined),
        Fs2 = norm_utls:concat_bin([ Fs,Key,<<",">>]),
        MetaFields = maps:get(<<"fields">>,ModelSpec),
        MetaVal = maps:get(Key,MetaFields),
        MetaValType = maps:get(<<"type">>,MetaVal),
        %%
       %%
        Vs2 = norm_utls:concat_bin([Vs
          ,type_to_sql(MetaValType,MapVal),<<",">>]),
        case lists:member(Key,Pk) of
          true ->        
            case no_value(MapVal) of
              true -> {Fs,Vs};
              false -> 
                %% @todo add id value vs sequence value check 
                %% SELECT MAX(the_primary_key) FROM the_table;
                %%                   v              
                %% SELECT nextval('the_primary_key_sequence');
                %%
                case lists:member(MetaValType,[<<"serial">>,<<"bigserial">>]) of 
                  true -> norm_log:log_term(warning,"Ensure PK matches value r"
                    "eturned by nextval('seq_name').");
                  false -> []
                end,
               {Fs2,Vs2} 
            end;
          false ->
            {Fs2,Vs2}
        end
    end
  end,{<<"">>,<<"">>},maps:keys(ModelMap)),
  FieldsS = strip_comma(Fields),
  ValuesS = strip_comma(Values),
  norm_utls:concat_bin([<<"INSERT INTO ">>,table_name(ModelName),<<" ( ">>
    ,FieldsS,<<" ) VALUES ( ">>,ValuesS,<<" ) ">>
    ,options_to_sql(<<"returning">>,Ops)]).
 
%% ------------------------------- SELECT -------------------------------------

select(Model) when is_map(Model) ->
  Name = norm_utls:model_name(Model),
  Where = lists:foldl(fun(Key,Acc) ->
    Val = maps:get(Key,Model),
    Acc ++ [{Key,'=',Val}]
  end,[],norm_utls:model_keys(Model)),
  select(Name,#{ where => Where }).

select(Name,Id) when is_binary(Name), is_integer(Id) ->
  select(Name,
    #{ where => [{<<"id">>,'=',Id}],order => any,limit => 50,offset => 0});

select(Name,Predicates) when is_binary(Name), is_map(Predicates) ->
  Sql = select_sql(Name,Predicates),
  case ?SQUERY(Sql) of
    {ok,Cols,Vals} ->
      select_to_model(Name,Cols,Vals);
    Error -> {error,Error}
  end.

select_sql(Name,Predicates) ->
  Where = maps:get(where,Predicates,undefined),
  OrderBy = maps:get(order_by,Predicates,undefined),
  Limit = maps:get(limit,Predicates,undefined),
  Offset = maps:get(offset,Predicates,undefined),
  norm_utls:concat_bin([<<"SELECT * FROM ">>,table_name(Name),
    where(Name,Where),order_by(OrderBy),limit(Limit)
    ,offset(Offset)]).

%% @doc Convert SELECT result to model Map.

select_to_model(Name,Cols,Vals) ->
  lists:foldl(fun(ResultTuple,Acc) -> 
    Map = new(Name),
    Fields = [X || {_,X,_,_,_,_} <- Cols],
    Results = tuple_to_list(ResultTuple),
    PropList = lists:zip(Fields,Results),
    Model = lists:foldl(fun({FieldBin,ValueBin},ModelMap) ->
      FieldType = norm_utls:model_type(FieldBin,ModelMap),
      Value = sql_to_type(FieldType,ValueBin),
      maps:update(FieldBin,Value,ModelMap)
    end,Map,PropList),
    Acc ++ [Model]
  end,[],Vals).

%% @todo Allow for nested WHERE predicates 

where(_,undefined) ->
  <<"">>;
where(Name,Where) ->
  WhereSql = lists:foldl(fun(Tuple,Acc) -> 
    norm_utls:concat_bin([Acc,
      case Tuple of
        {Field,Op,Val} when is_binary(Field)->
          Model = new(Name),
          FieldType = norm_utls:model_type(Field,Model),
          norm_utls:concat_bin([ 
            Field,<<" ">>,  
            norm_utls:atom_to_bin(Op),<<" ">>,  
            type_to_sql(FieldType,Val)]);
        AndEtc when is_atom(AndEtc) ->
          norm_utls:val_to_bin(AndEtc);
        {{Y,M,D},undefined} ->
          type_to_sql({<<"date">>,[]},{Y,M,D});
        {undefined,{H,M,S}} ->
          type_to_sql({<<"time">>,[]},{H,M,S});
        {{Y,Mt,D},{H,M,S}} ->
          type_to_sql({<<"timestamp">>,[]},{{Y,Mt,D},{H,M,S}})
      end,
      <<" ">>]) 
  end,<<"">>,Where),
  norm_utls:concat_bin([<<" WHERE ">>,strip_comma(WhereSql)]).  

order_by(undefined) ->
  <<"">>;
order_by(OrderBy) ->
  OrderSql = lists:foldl(fun({Field,Sort},Acc) ->
    norm_utls:concat_bin([Acc,norm_utls:val_to_bin(Field),<<" ">>,
    norm_utls:val_to_bin(Sort),<<", ">>])
  end,<<"">>,OrderBy),
  norm_utls:concat_bin([<<" ORDER BY ">>,strip_comma(OrderSql)]).  

limit(undefined) ->
  <<"">>;
limit(Limit) ->
  case Limit of all -> <<"">>; _ -> 
    norm_utls:concat_bin([<<" LIMIT ">>,norm_utls:num_to_bin(Limit)]) end. 

offset(undefined) ->
  <<"">>;
offset(Offset) ->
  case Offset of 0 -> <<"">>; _ ->  
    norm_utls:concat_bin([<<" OFFSET ">>, norm_utls:num_to_bin(Offset)]) end.


%% ----------------------------- DELETE ---------------------------------------

delete(Name,Id) when is_integer(Id) ->
  delete(Name,#{ where => [{<<"id">>,'=',Id}]});
delete(Name,Conditions) when is_map(Conditions) ->
  case ?SQUERY(delete_sql(Name,Conditions)) of
    {ok,Count} -> {ok,Count};
    {error,Error} -> {error,Error}
  end.

delete_sql(Name,Id) when is_integer(Id) ->
  delete_sql(Name,[{<<"id">>,'=',Id}]);

delete_sql(Name,Conditions) when is_map(Conditions) ->
  Where = maps:get(where,Conditions,undefined),
  norm_utls:concat_bin([<<"DELETE FROM ">>,
    table_name(Name),where(Name,Where)]).
 
%% ----------------------------- UPDATE ---------------------------------------

%% @doc Requires id filed for update.
%% @todo Add more flexibility specifying update criteria.

update(Model) ->
  case ?SQUERY(sql_update(Model)) of
    {ok,Count} -> {ok,Count};
    Error -> {error,Error}
  end. 

sql_update(Model) ->
  Updates = lists:foldl(fun(Key,Acc) ->
    case Key of
      <<"id">> -> <<"">>;
      Field -> 
        Value = maps:get(Key,Model),
        Type = norm_utls:model_type(Field,Model),
        norm_utls:concat_bin([Acc,<<" ">>,
          Field,
          <<" = ">>,type_to_sql(Type,Value),<<",">>])
    end 
  end,<<"">>,norm_utls:model_keys(Model)),
  Table = norm_utls:model_name(Model),
  %% @todo hardcoded id
  Id = maps:get(<<"id">>,Model),
  IdType = norm_utls:model_type(<<"id">>,Model),
  norm_utls:concat_bin([<<"UPDATE ">>,table_name(Table),<<" SET ">>,
    strip_comma(Updates),<<" WHERE id = ">>,type_to_sql(IdType,Id)]).

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

option_to_sql({<<"returning">>,undefined}) ->
  <<"">>;
option_to_sql({<<"returning">>,What}) ->
  norm_utls:concat_bin([<<" RETURNING ">>,What,<<" ">>]);
option_to_sql({<<"cascade">>,<<"true">>}) -> 
  <<" CASCADE ">>;
option_to_sql({<<"ifexists">>,<<"true">>}) ->
  <<" IF EXISTS ">>; 
option_to_sql({<<"ifexists">>,<<"false">>}) ->
  <<" IF NOT EXISTS ">>;
option_to_sql({_Key,undefined}) ->
  <<"">>.

options_to_sql(Key,OptionMap) ->
 Val = maps:get(Key,OptionMap,undefined),
 option_to_sql({Key,Val}). 

%% @doc Used when converting erlang terms to SQL query.

type_to_sql(_Type,<<"NULL">>) ->
  <<"NULL">>;
type_to_sql(_Type,'undefined') ->
  <<"NULL">>;
type_to_sql(_Type,'null') ->
  <<"NULL">>;
type_to_sql(<<"bigserial">>,Value) ->
  norm_utls:val_to_bin(Value);
type_to_sql(<<"bigint">>,Value) ->
  norm_utls:val_to_bin(Value);
type_to_sql(<<"integer">>,Value) ->
  norm_utls:val_to_bin(Value);
type_to_sql({Decimal,Opts},Value) when 
           Decimal =:= <<"decimal">> 
    orelse Decimal =:= <<"float">> 
    orelse Decimal =:= <<"numeric">> ->
  Decimals = norm_utls:get_value(scale,Opts,0),
  norm_utls:val_to_bin({Value,Decimals});
type_to_sql(<<"time">>,Value) ->
  norm_utls:quote([norm_utls:format_time(Value,'iso8601')]);
type_to_sql(<<"date">>,Value) ->
  norm_utls:quote([norm_utls:format_date(Value,'iso8601')]);
type_to_sql(<<"timestamp">>,Value) ->
  norm_utls:quote([norm_utls:format_datetime(Value,'iso8601')]);
type_to_sql(_Quoted,Value) ->
  norm_utls:quote([norm_utls:val_to_bin(Value)]).


%% @doc Used when converting SQL query results to Erlang terms.

sql_to_type(_Type,<<"NULL">>) ->
  <<"NULL">>;
sql_to_type(_Type,'undefined') ->
  <<"NULL">>;
sql_to_type(_Type,'null') ->
  <<"NULL">>;
sql_to_type(<<"bigserial">>,Value) ->
  norm_utls:bin_to_num(Value);
sql_to_type(<<"bigint">>,Value) ->
  norm_utls:bin_to_num(Value);
sql_to_type(<<"integer">>,Value) ->
  norm_utls:bin_to_num(Value);
sql_to_type({Decimal,Opts},Value) when 
           Decimal =:= <<"decimal">> 
    orelse Decimal =:= <<"float">> 
    orelse Decimal =:= <<"numeric">> ->
  Decimals = norm_utls:get_value(scale,Opts,0),
  norm_utls:bin_to_num({Value,Decimals});
sql_to_type(<<"time">>,Value) ->
  norm_utls:time_to_erlang(Value,'iso8601');
sql_to_type(<<"date">>,Value) ->
  norm_utls:date_to_erlang(Value,'iso8601');
sql_to_type(<<"timestamp">>,Value) ->
  norm_utls:datetime_to_erlang(Value,'iso8601');
sql_to_type(_Type,Value) ->
  Value.

%% ----------------------------------------------------------------------------
%% ----------------------------- UTILITIES ------------------------------------
%% ----------------------------------------------------------------------------

server_version() ->
  case ?SQUERY(sql_server_version()) of
    {ok,_Col,[{Version}]} -> {ok,norm_utls:bin_to_num(Version)};
    Error -> {error,Error}
  end.

sql_server_version() ->
  <<"show server_version">>.

get_schema() ->
  case norm_utls:get_db_config(pgsql,tablespace) of
    undefined -> <<"norm">>;
    Schema when is_binary(Schema) -> Schema;
    Schema when is_atom(Schema) -> norm_utls:atom_to_bin(Schema);
    Schema when is_list(Schema) -> list_to_binary(Schema)
  end.

constraint_name(NameTokens) when is_list(NameTokens) ->
  NamePart = lists:foldl(fun(E,Acc) -> 
    Name = case is_binary(E) of 
      true -> E;
      false -> norm_utls:atom_to_bin(E)
    end,
    norm_utls:concat_bin([ Acc,Name,<<"_">> ])
  end,<<"">>,NameTokens),
  {_,_,Secs} = os:timestamp(),
  norm_utls:concat_bin([NamePart,norm_utls:num_to_bin(Secs)]).  
  
create_rank(Map) ->
  Keys = maps:keys(Map),
  List = lists:foldl(fun(E,Acc) -> 
    Table = maps:get(E,Map),
    Rank = norm_utls:bin_to_num(maps:get(<<"create_rank">>,Table,<<"0">>)),
    Acc ++ [{Rank,E}]
  end, [], Keys),
  Sorted = lists:sort(List),
  if Sorted =:= [] -> undefined; true ->
  [Val || {_,Val} <- Sorted] end.

fields_sql(Map) ->
  RefFields = lists:foldl(fun(Field,AccRF) ->
    norm_utls:concat_bin([ AccRF,Field,<<",">>]) 
  end,<<"">>,maps:get(<<"fields">>,Map)),
  strip_comma(RefFields).

table_name(TableName) when is_binary(TableName) ->
  norm_utls:concat_bin([ get_schema(), <<".">>, TableName ]).

get_pool() ->
  Pools = norm_utls:get_db_config(pgsql,pools),
  {Name,_} = lists:nth(1,Pools),
  Name.

strip_comma(Binary) ->
  String = binary_to_list(Binary),
  Stripped = string:strip(string:strip(String,both,$ ),both,$,),
  list_to_binary(Stripped).

no_value(Val) ->
  case Val of 
    undefined -> true;
    <<"">> -> true;
    null -> true;
    <<"NULL">> -> true;
    _ -> false
  end.

ok_error([{ok,_}|Results]) ->
  ok_error(Results);
ok_error([]) ->
  ok;
ok_error([{error,_}|_Results]) ->
  error.


