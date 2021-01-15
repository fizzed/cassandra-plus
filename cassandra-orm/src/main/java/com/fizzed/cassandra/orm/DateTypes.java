package com.fizzed.cassandra.orm;

import com.datastax.driver.core.Row;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import static java.util.stream.Collectors.toList;
import java.util.stream.StreamSupport;

public class DateTypes {

    static public boolean hasColumn(Row row, String columnName, boolean required) {
        boolean exists = row != null
            && columnName != null
            && row.getColumnDefinitions().contains(columnName);
        
        if (!exists && required) {
            throw new IllegalArgumentException("Column " + columnName + " is not present");
        }
        
        return exists;
    }
    
    static public List<?> cqlList(Iterable<?> iterable) {
        if (iterable != null) {
            if (iterable instanceof List) {
                return (List)iterable;
            }
            
            // we MUST adapt it
            return StreamSupport.stream(iterable.spliterator(), false)
                .collect(toList());
        }
        return null;
    }
    
    static public ByteBuffer cqlBlob(String s) {
        if (s != null) {
            return StandardCharsets.UTF_8.encode(s);
        }
        return null;
    }
    
    static private String javaString(Object v) {
        if (v == null) {
            return null;
        }
        
        if (v instanceof String) {
            return (String)v;
        }
        
        if (v instanceof ByteBuffer) {
            ByteBuffer bb = (ByteBuffer)v;
            return StandardCharsets.UTF_8.decode(bb).toString();
        }

        switch (v.getClass().getCanonicalName()) {
//            case "java.nio.ByteBuffer":
//                ByteBuffer bb = (ByteBuffer)v;
//                return StandardCharsets.UTF_8.decode(bb).toString();
            default:
                throw new IllegalArgumentException("Unable to convert " + v.getClass() + " <-> java.lang.String");
        }
    }
    
    static public String javaString(Row row, String columnName) {
        return javaString(row, columnName, true);
    }
    
    static public String javaString(Row row, String columnName, boolean required) {
        if (hasColumn(row, columnName, required)) {
            final Object v = row.getObject(columnName);
            return javaString(v);
        }
        return null;
    }
    
    //
    // Bytes
    //
    
    static public Byte cqlByte(Object v) {
        if (v != null) {
            if (v instanceof Byte) {
                return (Byte)v;
            }
            if (v instanceof Number) {
                return ((Number)v).byteValue();
            }
            throw new IllegalArgumentException("Unable to convert to cqlByte");
        }
        return null;
    }
    
    static private Byte javaByte(Object v) {
        if (v == null) {
            return null;
        }
        
        if (v instanceof Byte) {
            return (Byte)v;
        }
        
        if (v instanceof Number) {
            Number n = (Number)v;
            return n.byteValue();
        }
        
        switch (v.getClass().getCanonicalName()) {
//            case "java.lang.Byte":
//                Byte b = (Byte)v;
//                return b.longValue();
//            case "java.lang.Short":
//                Short s = (Short)v;
//                return s.longValue();
//            case "java.lang.Integer":
//                Integer i = (Integer)v;
//                return i.longValue();
            default:
                throw new IllegalArgumentException("Unable to convert " + v.getClass() + " <-> java.lang.Byte");
        }
    }
    
    static public Byte javaByte(Row row, String columnName) {
        return javaByte(row, columnName, true);
    }
    
    static public Byte javaByte(Row row, String columnName, boolean required) {
        if (hasColumn(row, columnName, required)) {
            final Object v = row.getObject(columnName);
            return javaByte(v);
        }
        return null;
    }
    
    //
    // Shorts
    //
    
    static public Short cqlShort(Number v) {
        if (v != null) {
            if (v instanceof Short) {
                return (Short)v;
            }
            if (v instanceof Number) {
                return ((Number)v).shortValue();
            }
            throw new IllegalArgumentException("Unable to convert to cqlShort");
        }
        return null;
    }
    
    static private Short javaShort(Object v) {
        if (v == null) {
            return null;
        }
        
        if (v instanceof Short) {
            return (Short)v;
        }
        
        if (v instanceof Number) {
            Number n = (Number)v;
            return n.shortValue();
        }
        
        switch (v.getClass().getCanonicalName()) {
//            case "java.lang.Byte":
//                Byte b = (Byte)v;
//                return b.longValue();
//            case "java.lang.Short":
//                Short s = (Short)v;
//                return s.longValue();
//            case "java.lang.Integer":
//                Integer i = (Integer)v;
//                return i.longValue();
            default:
                throw new IllegalArgumentException("Unable to convert " + v.getClass() + " <-> java.lang.Short");
        }
    }
    
    static public Short javaShort(Row row, String columnName) {
        return javaShort(row, columnName, true);
    }
    
    static public Short javaShort(Row row, String columnName, boolean required) {
        if (hasColumn(row, columnName, required)) {
            final Object v = row.getObject(columnName);
            return javaShort(v);
        }
        return null;
    }
    
    //
    // Ints
    //
    
    static public Integer cqlInteger(Number v) {
        if (v != null) {
            if (v instanceof Integer) {
                return (Integer)v;
            }
            if (v instanceof Number) {
                return ((Number)v).intValue();
            }
            throw new IllegalArgumentException("Unable to convert to cqlInteger");
        }
        return null;
    }
    
    static private Integer javaInteger(Object v) {
        if (v == null) {
            return null;
        }
        
        if (v instanceof Integer) {
            return (Integer)v;
        }
        
        if (v instanceof Number) {
            Number n = (Number)v;
            return n.intValue();
        }
        
        switch (v.getClass().getCanonicalName()) {
//            case "java.lang.Byte":
//                Byte b = (Byte)v;
//                return b.intValue();
//            case "java.lang.Short":
//                Short s = (Short)v;
//                return s.intValue();
//            case "java.lang.Long":
//                Long l = (Long)v;
//                return l.intValue();
            default:
                throw new IllegalArgumentException("Unable to convert " + v.getClass() + " <-> java.lang.Integer");
        }
    }
    
    static public Integer javaInteger(Row row, String columnName) {
        return javaInteger(row, columnName, true);
    }
    
    static public Integer javaInteger(Row row, String columnName, boolean required) {
        if (hasColumn(row, columnName, required)) {
            final Object v = row.getObject(columnName);
            return javaInteger(v);
        }
        return null;
    }
    
    //
    // Longs
    //
    
    static public Long cqlLong(Number v) {
        if (v != null) {
            if (v instanceof Long) {
                return (Long)v;
            }
            if (v instanceof Number) {
                return ((Number)v).longValue();
            }
            throw new IllegalArgumentException("Unable to convert to cqlLong");
        }
        return null;
    }
    
    static private Long javaLong(Object v) {
        if (v == null) {
            return null;
        }
        
        if (v instanceof Long) {
            return (Long)v;
        }
        
        if (v instanceof Number) {
            Number n = (Number)v;
            return n.longValue();
        }
        
        switch (v.getClass().getCanonicalName()) {
//            case "java.lang.Byte":
//                Byte b = (Byte)v;
//                return b.longValue();
//            case "java.lang.Short":
//                Short s = (Short)v;
//                return s.longValue();
//            case "java.lang.Integer":
//                Integer i = (Integer)v;
//                return i.longValue();
            default:
                throw new IllegalArgumentException("Unable to convert " + v.getClass() + " <-> java.lang.Long");
        }
    }
    
    static public Long javaLong(Row row, String columnName) {
        return javaLong(row, columnName, true);
    }
    
    static public Long javaLong(Row row, String columnName, boolean required) {
        if (hasColumn(row, columnName, required)) {
            final Object v = row.getObject(columnName);
            return javaLong(v);
        }
        return null;
    }
    
}