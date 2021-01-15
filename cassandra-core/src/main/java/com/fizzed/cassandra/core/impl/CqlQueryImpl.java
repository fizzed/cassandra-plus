package com.fizzed.cassandra.core.impl;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PagingState;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.fizzed.cassandra.core.CqlBoundQuery;
import com.fizzed.cassandra.core.CqlExpressionList;
import com.fizzed.cassandra.core.CqlQuery;
import com.fizzed.cassandra.core.CqlRowMapper;
import com.fizzed.cassandra.core.PagedList;
import com.fizzed.cassandra.core.UnappliedException;
import java.util.ArrayList;
import java.util.List;
import static java.util.Optional.ofNullable;
import java.util.Set;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import javax.persistence.EntityExistsException;
import javax.persistence.OptimisticLockException;

public class CqlQueryImpl<T> implements CqlQuery<T>, CqlExpressionList<T> {
    
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
    
    private final Session session;
    private final Command command;
    private CqlRowMapper<T> rowMapper;
    private String columns;
    private String tableName;
    private boolean allowFiltering;
    private List<Clause> clauses;
    private List<Parameter> vals;
    private Set<String> primaryKeys;
    private Parameter optimisticLock;
    private String groupBy;
    private String orderBy;
    private Integer fetchSize;
    private PagingState pagingState;
    
    public CqlQueryImpl(Session session, Command command) {
        this.session = session;
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
    public CqlQuery<T> allowFiltering() {
        this.allowFiltering = true;
        return this;
    }
    
    @Override
    public CqlExpressionList<T> where() {
        return this;
    }

    @Override
    public CqlQuery<T> val(String name, Object value) {
        if (this.vals == null) {
            this.vals = new ArrayList<>();
        }
        this.vals.add(new Parameter(name, value));
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
        this.addClause(new BasicClause(name, "=", value));
        return this;
    }
    
    @Override
    public CqlExpressionList<T> gt(String name, Object value) {
        this.addClause(new BasicClause(name, ">", value));
        return this;
    }
    
    @Override
    public CqlExpressionList<T> ge(String name, Object value) {
        this.addClause(new BasicClause(name, ">=", value));
        return this;
    }
    
    @Override
    public CqlExpressionList<T> lt(String name, Object value) {
        this.addClause(new BasicClause(name, "<", value));
        return this;
    }
    
    @Override
    public CqlExpressionList<T> le(String name, Object value) {
        this.addClause(new BasicClause(name, "<=", value));
        return this;
    }
    
    @Override
    public CqlExpressionList<T> in(String name, Iterable<?> values) {
        this.addClause(new InClause(name, values));
        return this;
    }
    
    @Override
    public CqlQuery<T> optimisticLock(String name, Object value) {
        this.optimisticLock = new Parameter(name, value);
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
        
        final BoundStatement statement = this.session.prepare(boundQuery.getCql())
            .bind(boundQuery.toValues());
        
        if (this.fetchSize != null) {
            statement.setFetchSize(this.fetchSize);
        }
        
        if (this.pagingState != null) {
            statement.setPagingState(this.pagingState);
        }
        
        final ResultSet results = this.session.execute(statement);
        
        if (!results.wasApplied()) {
            if (boundQuery.getOptimisticLock() != null) {
                // build a helpful primary key help message
                final String pkmsg = boundQuery.getParameters().stream()
                    .filter(v -> boundQuery.getPrimaryKeys().contains(v.getName()))
                    .map(v -> v.getName() + "=" + v.getValue())
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
        
        return results;
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