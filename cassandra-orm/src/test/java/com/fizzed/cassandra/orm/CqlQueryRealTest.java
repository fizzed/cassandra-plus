package com.fizzed.cassandra.orm;

import com.datastax.driver.core.ConsistencyLevel;
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
    }
    
    @Test
    public void upsert() {
        this.dropCreateTable();

        final List<String> primaryKeys = asList("id");
        final UUID id1 = UUID.randomUUID();
        final DateTime dt1 = new DateTime(DateTimeZone.UTC);
        final UUID id2 = UUID.randomUUID();
        final DateTime dt2 = new DateTime(DateTimeZone.UTC).plusMinutes(1);
        
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
        
        // this should fail
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

        final ConsistencyLevel nonLwtForWritesConsistencyLevel = ConsistencyLevel.LOCAL_QUORUM;
        final ConsistencyLevel consistencyLevel = ConsistencyLevel.LOCAL_SERIAL;

        for (int i = 0; i < 2000; i++) {
            final List<String> primaryKeys = asList("id");
            final UUID id1 = UUID.randomUUID();
            final DateTime dt1 = new DateTime(DateTimeZone.UTC);
            final UUID id2 = UUID.randomUUID();
            final DateTime dt2 = new DateTime(DateTimeZone.UTC).plusMillis(1);

            this.cassandra.upsert("query_test")
//                .setConsistencyLevel(nonLwtForWritesConsistencyLevel)
                .setSerialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL)
                .optimisticLock("ts", null)
                .primaryKeys(primaryKeys)
                .val("id", id1)
                .val("vc", "a")
                .val("ts", cqlTimestampJoda(dt1))
                .execute();

            // use LWT to change the value
            this.cassandra.upsert("query_test")
//                .setConsistencyLevel(consistencyLevel)
                .setSerialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL)
                .optimisticLock("ts", cqlTimestampJoda(dt1))
                .primaryKeys(primaryKeys)
                .val("id", id1)
                .val("vc", "b")
                .val("ts", cqlTimestampJoda(dt2))
                .execute();

            final Row row1 = this.cassandra.select("query_test")
                .setConsistencyLevel(consistencyLevel)
                .where()
                .eq("id", id1)
                .findOne();

            assertThat(row1, is(not(nullValue())));
            assertThat(row1.getString("vc"), is("b"));
            assertThat(row1.getTimestamp("ts"), is(cqlTimestampJoda(dt2)));

            this.cassandra.delete("query_test")
                //.setConsistencyLevel(nonLwtForWritesConsistencyLevel)
                .setSerialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL)
                .optimisticLock("ts", dt2)
                .where()
                .eq("id", id1)
                .execute();

            final Row row2 = this.cassandra.select("query_test")
                .setConsistencyLevel(consistencyLevel)
                .where()
                .eq("id", id1)
                .findOne();

            assertThat(row2, is(nullValue()));
        }
    }

}