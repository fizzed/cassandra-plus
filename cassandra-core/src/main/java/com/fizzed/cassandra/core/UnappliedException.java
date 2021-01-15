package com.fizzed.cassandra.core;

import javax.persistence.PersistenceException;

public class UnappliedException extends PersistenceException {
 
    public UnappliedException(String message) {
        super(message);
    }
 
    public UnappliedException(String message, Throwable cause) {
        super(message, cause);
    }
    
}