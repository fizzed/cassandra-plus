package com.fizzed.cassandra.ninja;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.inject.AbstractModule;

public class NinjaCassandraModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(Cluster.class).toProvider(NinjaCassandraClusterProvider.class);
        bind(Session.class).toProvider(NinjaCassandraSessionProvider.class);
        bind(NinjaCassandraLifecycle.class);
        bind(NinjaCassandraMigrate.class);
    }
    
}