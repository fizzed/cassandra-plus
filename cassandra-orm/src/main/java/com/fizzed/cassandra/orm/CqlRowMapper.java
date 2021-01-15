package com.fizzed.cassandra.orm;

import com.datastax.driver.core.Row;

public interface CqlRowMapper<T> {

    T apply(Row row);
    
}