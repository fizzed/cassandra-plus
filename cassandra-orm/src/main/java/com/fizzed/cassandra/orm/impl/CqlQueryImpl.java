package com.fizzed.cassandra.orm.impl;

import com.datastax.driver.core.PagingState;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.fizzed.cassandra.orm.CqlBoundQuery;
import com.fizzed.cassandra.orm.CqlColMapper;
import com.fizzed.cassandra.orm.CqlExpressionList;
import com.fizzed.cassandra.orm.CqlQuery;
import com.fizzed.cassandra.orm.CqlRowMapper;
import com.fizzed.cassandra.orm.FindIterator;
import com.fizzed.cassandra.orm.PagedList;
import com.fizzed.cassandra.orm.UnappliedException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import static java.util.Optional.ofNullable;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import javax.persistence.EntityExistsException;
import javax.persistence.OptimisticLockException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CqlQueryImpl<T> implements CqlQuery<T>, CqlExpressionList<T> {
    static private final Logger log = LoggerFactory.getLogger(CqlQuery.class);
    
    static interface Clause {
        
        void appendTo(StringBuilder cql, List<Parameter> parameters);
        
    }

    static class BasicClause implements Clause {
        
        String name;
        String op;
        Object value;

        public BasicClause(String name, String op, Object value) {
            this.op = op;
            this.name = name;
            this.value = value;
        }

        @Override
        public void appendTo(StringBuilder cql, List<Parameter> parameters) {
            cql.append(this.name);
            cql.append(this.op);
            cql.append("?");
            parameters.add(new Parameter(this.name, value));
        }
    }

    static class InClause implements Clause {
        
        String name;
        Object value;

        public InClause(String name, Object value) {
            this.name = name;
            this.value = value;
        }

        @Override
        public void appendTo(StringBuilder cql, List<Parameter> parameters) {
            cql.append(this.name);
            cql.append(" IN ?");
            parameters.add(new Parameter(this.name, value));
        }
    }
    
    private final long id;
    private final Session session;
    private final ConcurrentHashMap<String,PreparedStatement> preparedStatementCache;
    private final Command command;
    private boolean prepared;
    private CqlRowMapper<T> rowMapper;
    private String columns;
    private String tableName;
    private boolean allowFiltering;
    private List<Clause> clauses;
    private List<Parameter> vals;
    private Set<String> primaryKeys;
    private Map<String,CqlColMapper> colMappers;
    private Parameter optimisticLock;
    private String groupBy;
    private String orderBy;
    private Integer fetchSize;
    private PagingState pagingState;
    
    public CqlQueryImpl(long id, Session session, Command command) {
        this.id = id;
        this.session = session;
        this.preparedStatementCache = new ConcurrentHashMap<>();
        this.command = command;
    }

    @Override
    public <U> CqlQuery<U> type(Class<U> type) {
        return (CqlQuery<U>)this;
    }

    @Override
    public CqlQuery<T> rowMapper(CqlRowMapper<T> rowMapper) {
        this.rowMapper = rowMapper;
        return this;
    }
    
    @Override
    public CqlQuery<T> colMappers(Map<String,CqlColMapper> colMappers) {
        this.colMappers = colMappers;
        return this;
    }

    private Object cqlVal(String columnName, Object value) {
        if (this.colMappers != null) {
            CqlColMapper colMapper = this.colMappers.get(columnName);
            if (colMapper != null) {
                return colMapper.apply(value);
            }
        }
        return value;
    }
    
    @Override
    public CqlQuery<T> columns(String columns) {
        this.columns = columns;
        return this;
    }

    @Override
    public CqlQuery<T> table(String tableName) {
        this.tableName = tableName;
        return this;
    }
    
    @Override
    public CqlQuery<T> primaryKeys(Set<String> primaryKeys) {
        this.primaryKeys = primaryKeys;
        return this;
    }
    
    @Override
    public CqlExpressionList<T> where() {
        return this;
    }

    @Override
    public CqlQuery<T> val(String name, Object value) {
        final Object cqlValue = this.cqlVal(name, value);
        if (this.vals == null) {
            this.vals = new ArrayList<>();
        }
        this.vals.add(new Parameter(name, cqlValue));
        return this;
    }
    
    private void addClause(Clause clause) {
        if (this.clauses == null) {
            this.clauses = new ArrayList<>();
        }
        this.clauses.add(clause);
    }
    
    @Override
    public CqlExpressionList<T> eq(String name, Object value) {
        final Object cqlValue = this.cqlVal(name, value);
        this.addClause(new BasicClause(name, "=", cqlValue));
        return this;
    }
    
    @Override
    public CqlExpressionList<T> gt(String name, Object value) {
        final Object cqlValue = this.cqlVal(name, value);
        this.addClause(new BasicClause(name, ">", cqlValue));
        return this;
    }
    
    @Override
    public CqlExpressionList<T> ge(String name, Object value) {
        final Object cqlValue = this.cqlVal(name, value);
        this.addClause(new BasicClause(name, ">=", cqlValue));
        return this;
    }
    
    @Override
    public CqlExpressionList<T> lt(String name, Object value) {
        final Object cqlValue = this.cqlVal(name, value);
        this.addClause(new BasicClause(name, "<", cqlValue));
        return this;
    }
    
    @Override
    public CqlExpressionList<T> le(String name, Object value) {
        final Object cqlValue = this.cqlVal(name, value);
        this.addClause(new BasicClause(name, "<=", cqlValue));
        return this;
    }
    
    @Override
    public CqlExpressionList<T> in(String name, Iterable<?> values) {
        this.addClause(new InClause(name, values));
        return this;
    }
    
    @Override
    public CqlQuery<T> optimisticLock(String name, Object value) {
        final Object cqlValue = this.cqlVal(name, value);
        this.optimisticLock = new Parameter(name, cqlValue);
        return this;
    }
    
    @Override
    public CqlExpressionList<T> groupBy(String groupBy) {
        this.groupBy = groupBy;
        return this;
    }
    
    @Override
    public CqlExpressionList<T> orderBy(String orderBy) {
        this.orderBy = orderBy;
        return this;
    }
    
    @Override
    public CqlQuery<T> setFetchSize(Integer fetchSize) {
        this.fetchSize = fetchSize;
        return this;
    }
    
    @Override
    public CqlQuery<T> setPagingState(String pagingState) {
        if (pagingState != null && !pagingState.isEmpty()) { 
            this.pagingState = PagingState.fromString(pagingState);
        } else {
            this.pagingState = null;
        }
        return this;
    }
    
    @Override
    public CqlQuery<T> setAllowFiltering(boolean allowFiltering) {
        this.allowFiltering = allowFiltering;
        return this;
    }
    
    @Override
    public CqlQuery<T> setPrepared(boolean prepared) {
        this.prepared = prepared;
        return this;
    }
    
    private Command effectiveCommand() {
        if (this.command == Command.UPSERT) {
            if (this.optimisticLock == null || this.optimisticLock.getValue() == null) {
                return Command.INSERT;
            } else {
                return Command.UPDATE;
            }
        } else {
            return this.command;
        }
    }
    
    @Override
    public CqlBoundQuery build() {
        final StringBuilder cql = new StringBuilder();
        final List<Parameter> parameters = new ArrayList();
        final Command cmd = this.effectiveCommand();
        final boolean isUpsert = this.command == Command.UPSERT;
        final boolean isOptimisticLocking = isUpsert && this.optimisticLock != null;

        if (isUpsert && (this.primaryKeys == null || this.primaryKeys.isEmpty())) {
            throw new IllegalStateException("UPSERT not allowed (primary keys not set)");
        }
        
        cql.append(cmd.toString());
        
        switch (cmd) {
            case SELECT:
            case DELETE:
                if (this.columns != null) {
                    cql.append(" ");
                    cql.append(this.columns);
                }
                cql.append(" FROM ");
                break;
            case UPDATE:
                cql.append(" ");
                break;
            case INSERT:
                cql.append(" INTO ");
                break;
            default:
                break;
        }
        
        cql.append(this.tableName);
        
        // insert columns & values
        if (cmd == Command.INSERT) {
            cql.append(" (");
            
            int count = 0;
            for (Parameter p : this.vals) {
                if (count > 0) {
                    cql.append(",");
                }
                
                cql.append(p.getName());
                parameters.add(p);
                count++;
            }
            
            cql.append(") VALUES (");
            
            count = 0;
            for (Parameter p : this.vals) {
                if (count > 0) {
                    cql.append(",");
                }
                
                cql.append("?");
                count++;
            }
            
            cql.append(")");
        }
        
        // update set values
        if (cmd == Command.UPDATE) {
            cql.append(" SET");
            
            int count = 0;
            for (Parameter p : this.vals) {
                // skip primary keys, not part of update
                if (isUpsert && this.primaryKeys.contains(p.getName())) {
                    continue;   // skip it!
                }
                
                if (count > 0) {
                    cql.append(",");
                }
                
                cql.append(" ");
                cql.append(p.getName());
                cql.append("=?");
                parameters.add(p);
                count++;
            }
        }
        
        switch (cmd) {
            case SELECT:
            case DELETE:
            case UPDATE:
                //
                // build a new effective "clauses" array if upsert
                //
                List<Clause> _clauses = this.clauses;
                if (isUpsert) {
                    _clauses = this.vals.stream()
                        .filter(v -> this.primaryKeys.contains(v.getName()))
                        .map(v -> new BasicClause(v.getName(), "=", v.getValue()))
                        .collect(toList());
                }
                
                if (_clauses != null && !_clauses.isEmpty()) {
                    cql.append(" WHERE ");
                    int count = 0;
                    for (Clause clause : _clauses) {
                        if (count > 0) {
                            cql.append(" AND ");
                        }
                        clause.appendTo(cql, parameters);
                        count++;
                    }
                }
                
                break;
            default:
                break;
        }

        
        if (cmd == Command.SELECT) {
            if (this.groupBy != null) {
                cql.append(" GROUP BY ");
                cql.append(this.groupBy);
            }
            
            if (this.orderBy != null) {
                cql.append(" ORDER BY ");
                cql.append(this.orderBy);
            }
        }
        
        if (isOptimisticLocking) {
            if (cmd == Command.INSERT) {
                cql.append(" IF NOT EXISTS");
            } else if (cmd == Command.UPDATE) {
                cql.append(" IF ");
                cql.append(this.optimisticLock.getName());
                cql.append("=?");
                parameters.add(this.optimisticLock);
            }
        }
        
        if (this.allowFiltering) {
            cql.append(" ALLOW FILTERING");
        }

        return new CqlBoundQuery(
            cmd,
            cql.toString(),
            this.tableName,
            parameters,
            this.primaryKeys,
            isOptimisticLocking ? this.optimisticLock : null);
    }
    
    
    @Override
    public ResultSet execute() {
        // a row mapper MUST be set
        if (this.rowMapper == null) {
            throw new IllegalStateException("A rowMapper must be set prior to execute");
        }
        
        final CqlBoundQuery boundQuery = this.build();
        
        final Statement statement;
        
        if (this.prepared) {
            final PreparedStatement preparedStatement = this.preparedStatementCache.computeIfAbsent(
                boundQuery.getCql(), k -> this.session.prepare(k));

            statement = preparedStatement.bind(boundQuery.toValues());
        }
        else {
            statement = new SimpleStatement(boundQuery.getCql(), boundQuery.toValues());
        }
        
        if (this.fetchSize != null) {
            statement.setFetchSize(this.fetchSize);
        }
        
        if (this.pagingState != null) {
            statement.setPagingState(this.pagingState);
        }
        
        if (log.isTraceEnabled()) {
            log.trace("[txn {}] sql {}", this.id, boundQuery.getCql());
            log.trace("[txn {}] val {}", this.id, boundQuery.getParameters());
        }
        
        final long start = System.currentTimeMillis();
        boolean success = false;
        try {
            final ResultSet results = this.session.execute(statement);

            if (!results.wasApplied()) {
                if (boundQuery.getOptimisticLock() != null) {
                    // build a helpful primary key help message
                    final String pkmsg = boundQuery.getParameters().stream()
                        .filter(v -> boundQuery.getPrimaryKeys().contains(v.getName()))
                        .map(v -> v.toString())
                        .collect(joining(", "));

                    if (boundQuery.getOptimisticLock() == null || boundQuery.getOptimisticLock().getValue() == null) {
                        // Error[Duplicate entry '2000000-2-63f10cfb-11f6-4338-bf0c-386cae93d26d' for key 'uk_local_data_accounting_typed_key']
                        throw new EntityExistsException(
                            "Duplicate entry for primary key '" + pkmsg + "' in table " + boundQuery.getTableName(), null);
                    } else {
                        // Data has changed. updated [0] rows sql[update local_data set document=?, updated_at=? where id=? and updated_at=?] bind[null]
                        throw new OptimisticLockException(
                            "Data has changed. Updated [0] rows for primary key '" + pkmsg + "' in table " + boundQuery.getTableName());
                    }
                }

                throw new UnappliedException("Unable to apply " + boundQuery.getCommand());
            }

            success = true;
            
            return results;
        }
        finally {
            if (log.isTraceEnabled()) {
                final long elapsedMillis = System.currentTimeMillis() - start;
                log.trace("[txn {}] execute {} in {} ms", this.id, success ? "success" : "failed", elapsedMillis);
            }
        }
    }

    @Override
    public T findOne() {
        final Row row = this.execute().one();
        return this.rowMapper.apply(row);
    }

    @Override
    public List<T> findList() {
        final ResultSet results = this.execute();
        final int rowSize = results.getAvailableWithoutFetching();
        
        final List<T> v = new ArrayList<>(rowSize);
        
        for (Row row : results) {
            v.add(this.rowMapper.apply(row));
        }
        
        return v;
    }

    @Override
    public FindIterator<T> findIterator() {
        final ResultSet results = this.execute();
        return new FindIterator<>(results, this.rowMapper);
    }
    
    @Override
    public PagedList<T> findPagedList() {
        final ResultSet results = this.execute();
        final int rowSize = results.getAvailableWithoutFetching();
        final String current = ofNullable(this.pagingState)
            .map(v -> v.toString())
            .orElse(null);
        final String next = ofNullable(results.getExecutionInfo())
            .map(v -> v.getPagingState())
            .map(v -> v.toString())
            .orElse(null);
        
        final List<T> v = new ArrayList<>(rowSize);
        
        int count = 0;
        
        for (Row row : results) {
            if (count >= rowSize) {
                // exit early, so we don't trigger another page of data...
                break;
            }
            
            v.add(this.rowMapper.apply(row));
            
            count++;
        }

        return new PagedList<>(v, this.fetchSize, current, next);
    }
    
}