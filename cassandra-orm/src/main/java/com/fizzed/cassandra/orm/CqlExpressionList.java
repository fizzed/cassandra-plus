package com.fizzed.cassandra.orm;

public interface CqlExpressionList<T> extends CqlQuery<T> {
    
    CqlExpressionList<T> eq(String name, Object value);
    
    CqlExpressionList<T> gt(String name, Object value);
    
    CqlExpressionList<T> ge(String name, Object value);
    
    CqlExpressionList<T> lt(String name, Object value);
    
    CqlExpressionList<T> le(String name, Object value);
    
    CqlExpressionList<T> in(String name, Iterable<?> values);
    
    CqlExpressionList<T> groupBy(String groupBy);
    
    CqlExpressionList<T> orderBy(String orderBy);
    
}