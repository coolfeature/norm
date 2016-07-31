-module(norm_models).

-compile(export_all).

pgsql() -> 
  #{
    <<"user">> => #{
      <<"create_tank">> => <<"1">>
      ,<<"fields">> => #{
        <<"id">> => #{  <<"type">> => <<"bigserial">>, <<"null">> => <<"false">> }
        ,<<"email">> => #{ <<"type">> => <<"varchar">>, <<"length">> => <<"50">> }
        ,<<"password">> => #{ <<"type">> => <<"varchar">>, <<"length">> => <<"50">> }
        ,<<"date_registered">> => #{ <<"type">> => <<"timestamp">> }
      }
      ,<<"constraints">> => #{
        <<"pk">> => #{ <<"fields">> => [<<"id">>] }
      }
    }
    ,<<"customer">> => #{
      <<"create_rank">> => <<"2">>
      ,<<"fields">> => #{
        <<"id">> => #{ <<"type">> => <<"bigserial">>, <<"null">> => <<"false">> }
        ,<<"factor">> => #{ <<"type">> => <<"decimal">>, <<"scale">> => <<"3,2">>, <<"null">> => <<"false">>, <<"default">> => <<"1.67">> }
        ,<<"fname">> => #{ <<"type">> => <<"varchar">>, <<"length">> => <<"50">> }
        ,<<"mname">> => #{ <<"type">> => <<"varchar">>, <<"length">> => <<"50">> }
        ,<<"lname">> => #{  <<"type">> => <<"varchar">>, <<"length">> => <<"50">> }
        ,<<"dob">> => #{  <<"type">> => <<"date">> }
        ,<<"user_id">> => #{ <<"type">> => <<"bigint">>, <<"null">> => <<"false">> }
      }
      ,<<"constraints">> => #{
        <<"pk">> => #{ <<"name">> => <<"pk_customer">>, <<"fields">> => [<<"id">>] }
        ,<<"fk">> => [ #{ 
            <<"references">> => #{ <<"table">> => <<"user">>, <<"fields">> => [<<"id">>] }
            , <<"fields">> => [<<"user_id">>] 
          }]
      }
    }
  }.

mnesia() ->
  #{
    <<"views">> => #{
      <<"key">> => <<"id">>
      ,<<"type">> => <<"set">>
      ,<<"fields">> => #{
        <<"id">> => #{ <<"type">> => <<"binary">> }
        ,<<"visits">> => #{ <<"type">> => <<"integer">> }
        ,<<"reviews">> => #{ <<"type">> => <<"map">> }
        ,<<"purchases">> => #{ <<"type">> => <<"map">> }
      }
    }
  }.
