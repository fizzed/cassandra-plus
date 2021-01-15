package com.fizzed.cassandra.core;

import com.datastax.driver.core.Row;

public interface CqlRowMapper<T> {

    T apply(Row row);
    
}