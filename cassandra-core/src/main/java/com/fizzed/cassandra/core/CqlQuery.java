package com.fizzed.cassandra.core;

import com.datastax.driver.core.ResultSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import static java.util.stream.Collectors.toSet;
import java.util.stream.StreamSupport;

public interface CqlQuery<T> {
    
    static public enum Command {
        SELECT,
        INSERT,
        UPDATE,
        DELETE,
        UPSERT
    }
    
    static class Parameter {
        
        private final String name;
        private final Object value;

        public Parameter(String name, Object value) {
            this.name = name;
            this.value = value;
        }

        public String getName() {
            return name;
        }

        public Object getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "Parameter{" + "name=" + name + ", value=" + value + '}';
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 29 * hash + Objects.hashCode(this.name);
            hash = 29 * hash + Objects.hashCode(this.value);
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final Parameter other = (Parameter) obj;
            if (!Objects.equals(this.name, other.name)) {
                return false;
            }
            if (!Objects.equals(this.value, other.value)) {
                return false;
            }
            return true;
        }

    }
    
    <U> CqlQuery<U> type(Class<U> type);
    
    CqlQuery<T> rowMapper(CqlRowMapper<T> rowMapper);
    
    CqlQuery<T> table(String tableName);
    
    default CqlQuery<T> primaryKeys(Iterable<String> primaryKeys) {
        if (primaryKeys != null) {
            this.primaryKeys(StreamSupport.stream(primaryKeys.spliterator(), false)
                .collect(toSet()));
        } else {
            this.primaryKeys((Set)null);
        }
        return this;
    }
    
    CqlQuery<T> primaryKeys(Set<String> primaryKeys);
    
    CqlQuery<T> columns(String columns);
    
    CqlExpressionList<T> where();
    
    CqlQuery<T> val(String name, Object value);
    
    CqlQuery<T> optimisticLock(String name, Object value);
    
    CqlQuery<T> allowFiltering();
    
    CqlQuery<T> setFetchSize(Integer fetchSize);
    
    CqlQuery<T> setPagingState(String pagingState);
    
    
    
    CqlBoundQuery build();
 

    ResultSet execute();

    T findOne();
    
    List<T> findList();
    
    PagedList<T> findPagedList();
    
}