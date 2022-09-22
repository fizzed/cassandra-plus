package com.fizzed.cassandra.ninja;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import ninja.lifecycle.Start;
import ninja.lifecycle.Dispose;
import ninja.utils.NinjaProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class NinjaCassandraLifecycle {
    static private final Logger log = LoggerFactory.getLogger(NinjaCassandraLifecycle.class);
    
    private final String name;
    private final Boolean validateAtStart;
    private final Provider<Session> sessionProvider;
    
    @Inject 
    public NinjaCassandraLifecycle(
            NinjaProperties ninjaProperties,
            String name,
            Provider<Session> sessionProvider) {
        
        this.name = name;
        this.validateAtStart = ninjaProperties.getBooleanWithDefault(name + ".validate_at_start", Boolean.TRUE);
        this.sessionProvider = sessionProvider;
    }

    @Start(order = 50)
    public void start() {
        if (!validateAtStart) {
            return;
        }
        
        log.info("Starting {}...", this.name);
        final Session session = sessionProvider.get();  // get but do not close
        ResultSet rs = session.execute("SELECT release_version FROM system.local");
        log.info("Connected to {} {}", this.name, rs.one().getString(0));
    }
    
    @Dispose(order = 50)
    public void stop() {
        log.info("Stopping {}...", this.name);
        try (Session session = sessionProvider.get()) {
            session.getCluster().close();
        }
        log.info("Closed connection to {}", this.name);
    }
    
}