package com.fizzed.cassandra.ninja;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import javax.inject.Provider;
import ninja.utils.NinjaProperties;

public class NinjaCassandraModule extends AbstractModule {
    
    private final NinjaProperties ninjaProperties;
    private final String name;

    public NinjaCassandraModule(NinjaProperties ninjaProperties) {
        this(ninjaProperties, "cassandra");
    }
    
    public NinjaCassandraModule(NinjaProperties ninjaProperties, String name) {
        this.ninjaProperties = ninjaProperties;
        this.name = name;
    }

    @Override
    protected void configure() {
        final Provider<Cluster> clusterProvider = new NinjaCassandraClusterProvider(this.ninjaProperties, this.name);
        final Provider<Session> sessionProvider = new NinjaCassandraSessionProvider(this.ninjaProperties, this.name, clusterProvider);
        
//        bind(Cluster.class).annotatedWith(Names.named(this.name)).toProvider(clusterProvider);
//        bind(Session.class).annotatedWith(Names.named(this.name)).toProvider(sessionProvider);
        bind(Cluster.class).toProvider(clusterProvider);
        bind(Session.class).toProvider(sessionProvider);
        bind(NinjaCassandraLifecycle.class).toInstance(new NinjaCassandraLifecycle(this.ninjaProperties, this.name, sessionProvider));
        bind(NinjaCassandraMigrate.class).toInstance(new NinjaCassandraMigrate(this.ninjaProperties, this.name, sessionProvider));
    }
    
}