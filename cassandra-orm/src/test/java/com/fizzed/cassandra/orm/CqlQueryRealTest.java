package com.fizzed.cassandra.orm;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import static com.fizzed.cassandra.orm.DataTypesJoda.cqlTimestampJoda;
import static java.util.Arrays.asList;
import java.util.List;
import java.util.UUID;
import javax.persistence.EntityExistsException;
import javax.persistence.OptimisticLockException;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import org.junit.Test;

public class CqlQueryRealTest extends BaseTester {
 
    public void dropCreateTable() {
        this.session.execute(
            "DROP TABLE IF EXISTS query_test;");
        
        this.session.execute(
            "CREATE TABLE query_test (\n" +
            "  id uuid,\n" +
            "  vc varchar,\n" +
            "  ts timestamp,\n" +
            "  primary key ((id))\n" +
            ");");
    }
    
    @Test
    public void select() {
        this.dropCreateTable();

        final Row row1 = this.cassandra.select("query_test")
            .where()
            .eq("id", UUID.randomUUID())
            .findOne();

        assertThat(row1, is(nullValue()));
        
        final List<Row> rows1 = this.cassandra.select("query_test")
            .where()
            .in("id", asList(UUID.randomUUID()))
            .findList();

        assertThat(rows1, hasSize(0));
        
        // insert some data now
        final List<String> primaryKeys = asList("id");
        final UUID id1 = UUID.randomUUID();
        final DateTime dt1 = new DateTime(DateTimeZone.UTC);
        final UUID id2 = UUID.randomUUID();
        final DateTime dt2 = new DateTime(DateTimeZone.UTC).plusMillis(1);
        
        this.cassandra.insert("query_test")
            .primaryKeys(primaryKeys)
            .val("id", id1)
            .val("vc", "a")
            .val("ts", cqlTimestampJoda(dt1))
            .execute();
        
        this.cassandra.insert("query_test")
            .primaryKeys(primaryKeys)
            .val("id", id2)
            .val("vc", "b")
            .val("ts", cqlTimestampJoda(dt2))
            .execute();
        
        final Row row2 = this.cassandra.select("query_test")
            .where()
            .eq("id", id1)
            .findOne();

        assertThat(row2, is(not(nullValue())));
        assertThat(row2.getString("vc"), is("a"));
        
        final Row row3 = this.cassandra.select("query_test")
            .where()
            .eq("id", id2)
            .findOne();

        assertThat(row3, is(not(nullValue())));
        assertThat(row3.getString("vc"), is("b"));
        
        this.cassandra.delete("query_test")
            .where()
            .eq("id", id1)
            .execute();
        
        final Row row4 = this.cassandra.select("query_test")
            .where()
            .eq("id", id1)
            .findOne();

        assertThat(row4, is(nullValue()));
    }
    
    @Test
    public void upsert() {
        this.dropCreateTable();

        final List<String> primaryKeys = asList("id");
        final UUID id1 = UUID.randomUUID();
        final DateTime dt1 = new DateTime(DateTimeZone.UTC);
        final UUID id2 = UUID.randomUUID();
        final DateTime dt2 = new DateTime(DateTimeZone.UTC).plusMillis(1);
        
        this.cassandra.upsert("query_test")
            .primaryKeys(primaryKeys)
            .optimisticLock("ts", null)
            .val("id", id1)
            .val("vc", "a")
            .val("ts", cqlTimestampJoda(dt1))
            .execute();
        
        // this should fail
        try {
            this.cassandra.upsert("query_test")
                .primaryKeys(primaryKeys)
                .optimisticLock("ts", null)
                .val("id", id1)
                .val("vc", "b")
                .val("ts", cqlTimestampJoda(dt2))
                .execute();
            fail();
        }
        catch (EntityExistsException e) {
            // expected
        }
        
        this.cassandra.upsert("query_test")
            .primaryKeys(primaryKeys)
            .optimisticLock("ts", cqlTimestampJoda(dt1))
            .val("id", id1)
            .val("vc", "b")
            .val("ts", cqlTimestampJoda(dt2))
            .execute();
        
        // this should fail since "ts" SHOULD be dt2, not dt1
        try {
            this.cassandra.upsert("query_test")
                .primaryKeys(primaryKeys)
                .optimisticLock("ts", cqlTimestampJoda(dt1))
                .val("id", id1)
                .val("vc", "b")
                .val("ts", cqlTimestampJoda(dt2))
                .execute();
            fail();
        }
        catch (OptimisticLockException e) {
            // expected
        }
    }
    
    @Test
    public void consistencyLevelsWithLWT() {
        this.dropCreateTable();


//        ConsistencyLevel consistencyLevel = null;
//        ConsistencyLevel serialConsistencyLevel = null;
        ConsistencyLevel consistencyLevel = ConsistencyLevel.LOCAL_QUORUM;
        ConsistencyLevel serialConsistencyLevel = ConsistencyLevel.LOCAL_SERIAL;

        for (int i = 0; i < 2000; i++) {
            final List<String> primaryKeys = asList("id");
            final UUID id1 = UUID.randomUUID();
            final DateTime dt1 = new DateTime(DateTimeZone.UTC);
            final UUID id2 = UUID.randomUUID();
            final DateTime dt2 = new DateTime(DateTimeZone.UTC).plusMillis(1);

            this.cassandra.upsert("query_test")
                .setConsistencyLevel(consistencyLevel)
                .setSerialConsistencyLevel(serialConsistencyLevel)
                .optimisticLock("ts", null)
                .primaryKeys(primaryKeys)
                .val("id", id1)
                .val("vc", "a")
                .val("ts", cqlTimestampJoda(dt1))
                .execute();

            // use LWT to change the value
            this.cassandra.upsert("query_test")
                .setConsistencyLevel(consistencyLevel)
                .setSerialConsistencyLevel(serialConsistencyLevel)
                .optimisticLock("ts", cqlTimestampJoda(dt1))
                .primaryKeys(primaryKeys)
                .val("id", id1)
                .val("vc", "b")
                .val("ts", cqlTimestampJoda(dt2))
                .execute();
            
            final Row row1 = this.cassandra.select("query_test")
                .setConsistencyLevel(consistencyLevel)
                .setSerialConsistencyLevel(serialConsistencyLevel)
                .where()
                .eq("id", id1)
                .findOne();

            assertThat(row1, is(not(nullValue())));
            assertThat(row1.getString("vc"), is("b"));
            assertThat(row1.getTimestamp("ts"), is(cqlTimestampJoda(dt2)));

            this.cassandra.delete("query_test")
                .setConsistencyLevel(consistencyLevel)
                .setSerialConsistencyLevel(serialConsistencyLevel)
                .optimisticLock("ts", dt2)
                .where()
                .eq("id", id1)
                .execute();

            final Row row2 = this.cassandra.select("query_test")
                .setConsistencyLevel(consistencyLevel)
                .setSerialConsistencyLevel(serialConsistencyLevel)
                .where()
                .eq("id", id1)
                .findOne();

            assertThat(row2, is(nullValue()));
        }
    }
 
}