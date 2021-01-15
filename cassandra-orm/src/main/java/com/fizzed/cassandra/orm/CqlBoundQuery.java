package com.fizzed.cassandra.orm;

import com.fizzed.cassandra.orm.CqlQuery.Command;
import com.fizzed.cassandra.orm.CqlQuery.Parameter;
import java.util.List;
import java.util.Set;

public class CqlBoundQuery {
    
    private final Command command;
    private final String cql;
    private final String tableName;
    private final List<Parameter> parameters;
    private final Set<String> primaryKeys;
    private final Parameter optimisticLock;

    public CqlBoundQuery(
            Command command,
            String cql,
            String tableName,
            List<Parameter> parameters,
            Set<String> primaryKeys,
            Parameter optimisticLock) {
        
        this.command = command;
        this.cql = cql;
        this.tableName = tableName;
        this.parameters = parameters;
        this.primaryKeys = primaryKeys;
        this.optimisticLock = optimisticLock;
    }

    public Command getCommand() {
        return command;
    }

    public String getCql() {
        return this.cql;
    }

    public String getTableName() {
        return this.tableName;
    }

    public List<CqlQuery.Parameter> getParameters() {
        return this.parameters;
    }

    public Set<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public Parameter getOptimisticLock() {
        return this.optimisticLock;
    }

    public Object[] toValues() {
        if (this.parameters == null || this.parameters.isEmpty()) {
            return new Object[0];
        }
        Object[] values = new Object[this.parameters.size()];
        for (int i = 0; i < values.length; i++) {
            values[i] = this.parameters.get(i).getValue();
        }
        return values;
    }
    
}
