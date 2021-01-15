package com.fizzed.cassandra.orm;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.fizzed.cassandra.orm.CqlQuery.Command;
import com.fizzed.cassandra.orm.impl.CqlQueryImpl;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class Cassandra {
    
    static private final CqlModel<Row> ROW_MODEL = new CqlModel<Row>()
        .setRowMapper(row -> row);
    
    private final AtomicLong idSequence;
    private final Session session;
    private final Map<Class<?>,CqlModel<?>> modelTypes;

    public Cassandra(Session session) {
        this.idSequence = new AtomicLong();
        this.session = session;
        this.modelTypes = new HashMap<>();
    }
    
    public Cassandra registerModel(Class<?> type, CqlModel<?> model) {
        this.modelTypes.put(type, model);
        return this;
    }
    
    private <T> CqlModel<T> resolveModel(Class<?> type) {
        final CqlModel<?> model = this.modelTypes.get(type);
        
        if (model == null) {
            throw new IllegalStateException("Model for type " + type.getCanonicalName() + " was not registered!");
        }
        
        return (CqlModel<T>)model;
    }
    
    
    
    public CqlQuery<Row> select(String tableName) {
        return select(ROW_MODEL)
            .table(tableName);
    }
    
    public <T> CqlQuery<T> select(Class<T> type) {
        final CqlModel<T> model = this.resolveModel(type);
        return this.select(model);
    }
    
    private <T> CqlQuery<T> select(CqlModel<T> model) {
        return new CqlQueryImpl<T>(this.idSequence.incrementAndGet(), this.session, Command.SELECT)
            .rowMapper(model.getRowMapper())
            .colMappers(model.getColMappers())
            .primaryKeys(model.getPrimaryKeys())
            .columns("*")
            .table(model.getTableName());
    }

    
    
    public CqlQuery<Row> update(String tableName) {
        return update(ROW_MODEL)
            .table(tableName);
    }
    
    public <T> CqlQuery<T> update(Class<T> type) {
        final CqlModel<T> model = this.resolveModel(type);
        return this.update(model);
    }
    
    public <T> CqlQuery<T> update(CqlModel<T> model) {
        return new CqlQueryImpl<T>(this.idSequence.incrementAndGet(), this.session, Command.UPDATE)
            .rowMapper(model.getRowMapper())
            .colMappers(model.getColMappers())
            .primaryKeys(model.getPrimaryKeys())
            .table(model.getTableName());
    }
    
    
    
    public CqlQuery<Row> insert(String tableName) {
        return insert(ROW_MODEL)
            .table(tableName);
    }
    
    public <T> CqlQuery<T> insert(Class<T> type) {
        final CqlModel<T> model = this.resolveModel(type);
        return this.insert(model);
    }
    
    private <T> CqlQuery<T> insert(CqlModel<T> model) {
        return new CqlQueryImpl<T>(this.idSequence.incrementAndGet(), this.session, Command.INSERT)
            .rowMapper(model.getRowMapper())
            .colMappers(model.getColMappers())
            .primaryKeys(model.getPrimaryKeys())
            .table(model.getTableName());
    }
    
    
    
    public CqlQuery<Row> upsert(String tableName) {
        return upsert(ROW_MODEL)
            .table(tableName);
    }
    
    public <T> CqlQuery<T> upsert(Class<T> type) {
        final CqlModel<T> model = this.resolveModel(type);
        return this.upsert(model);
    }
    
    private <T> CqlQuery<T> upsert(CqlModel<T> model) {
        return new CqlQueryImpl<T>(this.idSequence.incrementAndGet(), this.session, Command.UPSERT)
            .rowMapper(model.getRowMapper())
            .colMappers(model.getColMappers())
            .primaryKeys(model.getPrimaryKeys())
            .table(model.getTableName());
    }
    
    
    
    public CqlQuery<Row> delete(String tableName) {
        return delete(ROW_MODEL)
            .table(tableName);
    }
    
    public <T> CqlQuery<T> delete(Class<T> type) {
        final CqlModel<T> model = this.resolveModel(type);
        return this.delete(model);
    }

    private <T> CqlQuery<T> delete(CqlModel<T> model) {
        return new CqlQueryImpl<T>(this.idSequence.incrementAndGet(), this.session, Command.DELETE)
            .rowMapper(model.getRowMapper())
            .colMappers(model.getColMappers())
            .primaryKeys(model.getPrimaryKeys())
            .table(model.getTableName());
    }

}