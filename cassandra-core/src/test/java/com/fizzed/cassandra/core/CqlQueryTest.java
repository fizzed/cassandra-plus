package com.fizzed.cassandra.core;

import com.fizzed.cassandra.core.CqlQuery.Parameter;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import org.junit.Test;

public class CqlQueryTest {
 
    private final Cassandra cassandra = new Cassandra(null);
    
    @Test
    public void select() {
        
        CqlBoundQuery query;
        
        query = this.cassandra.select("test")
            .allowFiltering()
            .where()
            .eq("a", 1)
            .build();

        assertThat(query.getCql(), is("SELECT * FROM test WHERE a=? ALLOW FILTERING"));
        assertThat(query.getParameters().get(0), is(new Parameter("a", 1)));
        
        query = this.cassandra.select("test")
            .columns("a, b")
            .where()
            .eq("a", 1)
            .eq("b", "dude")
            .build();

        assertThat(query.getCql(), is("SELECT a, b FROM test WHERE a=? AND b=?"));
        assertThat(query.getParameters().get(0), is(new Parameter("a", 1)));
        assertThat(query.getParameters().get(1), is(new Parameter("b", "dude")));
        
        query = this.cassandra.select("test")
            .columns("a, b")
            .where()
            .eq("a", 1)
            .eq("b", "dude")
            .in("d", asList(1, 2, 3))
            .build();

        assertThat(query.getCql(), is("SELECT a, b FROM test WHERE a=? AND b=? AND d IN ?"));
        assertThat(query.getParameters().get(0), is(new Parameter("a", 1)));
        assertThat(query.getParameters().get(1), is(new Parameter("b", "dude")));
        assertThat(query.getParameters().get(2), is(new Parameter("d", asList(1, 2, 3))));
        
        query = this.cassandra.select("test")
            .allowFiltering()
            .where()
            .eq("a", 1)
            .groupBy("id")
            .orderBy("a")
            .build();

        assertThat(query.getCql(), is("SELECT * FROM test WHERE a=? GROUP BY id ORDER BY a ALLOW FILTERING"));
        assertThat(query.getParameters().get(0), is(new Parameter("a", 1)));
    }
 
    @Test
    public void update() {
        
        CqlBoundQuery query;
        
        query = this.cassandra.update("test")
            .val("b", "dude")
            .where()
            .eq("a", 1)
            .build();

        assertThat(query.getCql(), is("UPDATE test SET b=? WHERE a=?"));
        assertThat(query.getParameters().get(0), is(new Parameter("b", "dude")));
        assertThat(query.getParameters().get(1), is(new Parameter("a", 1)));
        
        query = this.cassandra.update("test")
            .val("b", "dude")
            .val("c", (byte)5)
            .where()
            .eq("a", 1)
            .build();

        assertThat(query.getCql(), is("UPDATE test SET b=?, c=? WHERE a=?"));
        assertThat(query.getParameters().get(0), is(new Parameter("b", "dude")));
        assertThat(query.getParameters().get(1), is(new Parameter("c", (byte)5)));
        assertThat(query.getParameters().get(2), is(new Parameter("a", 1)));
    }
 
    @Test
    public void insert() {
        
        CqlBoundQuery query;
        
        query = this.cassandra.insert("test")
            .val("b", "dude")
            .build();

        assertThat(query.getCql(), is("INSERT INTO test (b) VALUES (?)"));
        assertThat(query.getParameters().get(0), is(new Parameter("b", "dude")));
        
        query = this.cassandra.insert("test")
            .val("b", "dude")
            .val("c", (byte)5)
            .build();

        assertThat(query.getCql(), is("INSERT INTO test (b,c) VALUES (?,?)"));
        assertThat(query.getParameters().get(0), is(new Parameter("b", "dude")));
        assertThat(query.getParameters().get(1), is(new Parameter("c", (byte)5)));
    }
    
    @Test
    public void delete() {
        
        CqlBoundQuery query;
        
        query = this.cassandra.delete("test")
            .allowFiltering()
            .where()
            .eq("a", 1)
            .build();

        assertThat(query.getCql(), is("DELETE FROM test WHERE a=? ALLOW FILTERING"));
        assertThat(query.getParameters().get(0), is(new Parameter("a", 1)));
        
        query = this.cassandra.delete("test")
            .columns("a, b")
            .where()
            .eq("a", 1)
            .eq("b", "dude")
            .build();

        assertThat(query.getCql(), is("DELETE a, b FROM test WHERE a=? AND b=?"));
        assertThat(query.getParameters().get(0), is(new Parameter("a", 1)));
        assertThat(query.getParameters().get(1), is(new Parameter("b", "dude")));
    }
    
    @Test
    public void upsert() {
        
        CqlBoundQuery query;
        
        query = this.cassandra.upsert("test")
            .primaryKeys(asList("c", "d"))
            .val("a", 1)
            .val("b", 2)
            .val("c", 3)
            .val("d", 4)
            .build();

        assertThat(query.getCql(), is("INSERT INTO test (a,b,c,d) VALUES (?,?,?,?)"));
        assertThat(query.getParameters().get(0), is(new Parameter("a", 1)));
        
        
        query = this.cassandra.upsert("test")
            .primaryKeys(asList("c", "d"))
            .optimisticLock("d", null)
            .val("a", 1)
            .val("b", 2)
            .val("c", 3)
            .val("d", 4)
            .build();

        assertThat(query.getCql(), is("INSERT INTO test (a,b,c,d) VALUES (?,?,?,?) IF NOT EXISTS"));
        assertThat(query.getParameters().get(0), is(new Parameter("a", 1)));
        
        
        query = this.cassandra.upsert("test")
            .primaryKeys(asList("c", "d"))
            .optimisticLock("a", 5)
            .val("a", 1)
            .val("b", 2)
            .val("c", 3)
            .val("d", 4)
            .build();

        assertThat(query.getCql(), is("UPDATE test SET a=?, b=? WHERE c=? AND d=? IF a=?"));
        assertThat(query.getParameters().get(0), is(new Parameter("a", 1)));
        // this is the version part...
        assertThat(query.getParameters().get(4), is(new Parameter("a", 5)));
    }
    
}