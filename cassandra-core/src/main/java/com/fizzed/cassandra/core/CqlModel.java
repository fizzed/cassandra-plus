package com.fizzed.cassandra.core;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import static java.util.stream.Collectors.toSet;

public class CqlModel<T> {
 
    private String tableName;
    private CqlRowMapper<T> rowMapper;
    private Map<String,CqlColMapper> colMappers;
    private Set<String> primaryKeys;

    public String getTableName() {
        return tableName;
    }

    public CqlModel<T> setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public CqlRowMapper<T> getRowMapper() {
        return rowMapper;
    }

    public CqlModel<T> setRowMapper(CqlRowMapper<T> rowMapper) {
        this.rowMapper = rowMapper;
        return this;
    }

    public Map<String, CqlColMapper> getColMappers() {
        return colMappers;
    }

    public CqlModel<T> setColMappers(Map<String, CqlColMapper> colMappers) {
        this.colMappers = colMappers;
        return this;
    }
    
    public CqlModel<T> addColMapper(String columnName, CqlColMapper colMapper) {
        if (this.colMappers == null) {
            this.colMappers = new HashMap<>();
        }
        this.colMappers.put(columnName, colMapper);
        return this;
    }

    public Set<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public CqlModel<T> setPrimaryKeys(Collection<String> primaryKeys) {
        this.primaryKeys = primaryKeys.stream()
            .collect(toSet());
        return this;
    }
    
}