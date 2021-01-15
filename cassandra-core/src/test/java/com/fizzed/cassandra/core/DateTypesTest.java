package com.fizzed.cassandra.core;

import com.datastax.driver.core.Row;
import java.util.UUID;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import org.junit.Test;
import static com.fizzed.cassandra.core.DataTypesJoda.jodaDateTime;
import static com.fizzed.cassandra.core.DateTypes.javaString;
import static com.fizzed.cassandra.core.DateTypes.cqlBlob;
import static com.fizzed.cassandra.core.DataTypesJoda.cqlTimestampJoda;

public class DateTypesTest extends BaseTester {

    public void dropCreateStringTable() {
        this.session.execute(
            "DROP TABLE IF EXISTS string_test;");
        
        this.session.execute(
            "CREATE TABLE string_test (\n" +
            "  id uuid,\n" +
            "  vc varchar,\n" +
            "  bl blob,\n" +
            "  primary key ((id))\n" +
            ");");
    }
    
    @Test
    public void string() {
        this.dropCreateStringTable();
        
        final UUID id1 = UUID.randomUUID();
        final String s1 = "\u20AChello";
        final String s2 = "this is a blob";
        
        this.session.execute("INSERT INTO string_test (id,vc,bl) VALUES (?,?,?);", id1, s1, cqlBlob(s2));
        
        // will be present
        final Row row1 = this.session.execute("SELECT * FROM string_test WHERE id=?", id1).one();
        
        assertThat(javaString(row1, "vc"), is(s1));
        assertThat(javaString(row1, "bl"), is(s2));
    }
    
    public void dropCreateJodaTable() {
        this.session.execute(
            "DROP TABLE IF EXISTS joda_test;");
        
        this.session.execute(
            "CREATE TABLE joda_test (\n" +
            "  id uuid,\n" +
            "  ts timestamp,\n" +
            "  bi bigint,\n" +
            "  vc varchar,\n" +
            "  primary key ((id))\n" +
            ");");
    }
    
    @Test
    public void joda() {
        this.dropCreateJodaTable();
        
        final UUID id1 = UUID.randomUUID();
        final UUID id2 = UUID.randomUUID();
        final DateTime dt1 = new DateTime(DateTimeZone.UTC);
        final long bi1 = System.currentTimeMillis();
        
        this.session.execute("INSERT INTO joda_test (id,ts,bi,vc) VALUES (?,?,?,?);", id1, cqlTimestampJoda(dt1), bi1, "a");
        this.session.execute("INSERT INTO joda_test (id,ts,bi,vc) VALUES (?,?,?,?);", id2, cqlTimestampJoda(null), null, "b");
        
        // will be present
        final Row row1 = this.session.execute("SELECT * FROM joda_test WHERE id=?", id1).one();
        
        assertThat(jodaDateTime(row1, "ts"), is(dt1));
        assertThat(jodaDateTime(row1, "bi"), is(new DateTime(bi1, DateTimeZone.UTC)));
        assertThat(jodaDateTime(row1, "notexists", false), is(nullValue()));
        
        // will not be convertible
        try {
            jodaDateTime(row1, "vc");
            fail();
        }
        catch (IllegalArgumentException e) {
            // expected
        }
        
        // will be NULL
        final Row row2 = this.session.execute("SELECT * FROM joda_test WHERE id=?", id2).one();
        
        assertThat(jodaDateTime(row2, "ts"), is(nullValue()));
    }
    
}