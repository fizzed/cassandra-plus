package com.fizzed.cassandra.orm;

import com.datastax.driver.core.Row;
import static com.fizzed.cassandra.orm.DateTypes.hasColumn;
import java.util.Date;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class DataTypesJoda {
 
    static public Date cqlTimestampJoda(Object v) {
        if (v == null) {
            return null;
        }
        
        if (v instanceof DateTime) {
            return ((DateTime)v).toDate();
        }
        
        if (v instanceof Date) {
            return (Date)v;
        }
        
        throw new IllegalArgumentException("Unable to convert to DateTime");
    }
 
    static private DateTime jodaDateTime(Object v) {
        if (v == null) {
            return null;
        }
        
        switch (v.getClass().getCanonicalName()) {
            case "java.util.Date":
                Date d = (Date)v;
                return new DateTime(d.getTime(), DateTimeZone.UTC);
            case "java.lang.Long":
                Long l = (Long)v;
                return new DateTime(l, DateTimeZone.UTC);
            default:
                // com.datastax.driver.core.exceptions.CodecNotFoundException: Codec not found for requested operation: [varchar <-> java.util.Date]
                throw new IllegalArgumentException("Unable to convert " + v.getClass() + " <-> org.joda.time.DateTime");
        }
    }
    
    static public DateTime jodaDateTime(Row row, String columnName) {
        return jodaDateTime(row, columnName, true);
    }
    
    static public DateTime jodaDateTime(Row row, String columnName, boolean required) {
        if (hasColumn(row, columnName, required)) {
            final Object v = row.getObject(columnName);
            return jodaDateTime(v);
        }
        return null;
    }
    
}