package com.fizzed.cassandra.orm;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

public class FindIterator<T> implements Iterable<T> {

    private final ResultSet results;
    private final CqlRowMapper<T> rowMapper;
    private final AtomicLong counter;
    private final Iterator<Row> it;

    public FindIterator(ResultSet results, CqlRowMapper<T> rowMapper) {
        this.results = results;
        this.rowMapper = rowMapper;
        this.counter = new AtomicLong();
        this.it = this.results.iterator();
    }
    
    public long getCount() {
        return this.counter.get();
    }

    @Override
    public Iterator<T> iterator() {
        return new Iterator<T>() {
            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public T next() {
                Row row = it.next();
                counter.incrementAndGet();
                return rowMapper.apply(row);
            }
        };
    }

}