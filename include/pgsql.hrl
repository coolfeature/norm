-record(pgsql,{ models = #{
  user => #{ 
    fields => #{
      id => #{  type => 'bigserial', null => 'false' }
      ,email => #{  type => 'varchar', 'length' => 50 }
      ,password => #{  type => 'varchar', 'length' => 50 }
      ,date_registered => #{  type => 'timestamp' }
    }  
    ,constraints => #{
      pk => [ #{ fields => ['id'] }]
    }
  }
  ,customer => #{ 
    fields => #{
      id => #{  type => 'bigserial' }
      ,fname => #{  type => 'varchar', 'length' => 50 }
      ,mname => #{  type => 'varchar', 'length' => 50 }
      ,lname => #{  type => 'varchar', 'length' => 50 }
      ,dob => #{  type => 'date' }
      ,customer_id => #{  type => 'bigint', null => 'false' }
    }  
    ,constraints => #{
      pk => [ #{ 'name' => 'pk_customer', fields => ['id'] } ]
      ,fk => [ #{ references => #{ table => 'user', fields => [id] }, fields => ['customer_id'] }]
    }
  }
}}).

