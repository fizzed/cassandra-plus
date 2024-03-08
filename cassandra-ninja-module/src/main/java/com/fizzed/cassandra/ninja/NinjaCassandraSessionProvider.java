package com.fizzed.cassandra.ninja;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import ninja.utils.NinjaProperties;
import static org.apache.commons.lang3.StringUtils.isEmpty;

@Singleton
public class NinjaCassandraSessionProvider implements Provider<Session> {

    private final Supplier<Session> memoizedSupplier;
    
    @Inject
    public NinjaCassandraSessionProvider(
            NinjaProperties ninjaProperties,
            Provider<Cluster> clusterProvider) {
        this.memoizedSupplier = Suppliers.memoize(() -> {
            return this.build(ninjaProperties, clusterProvider);
        });
    }
    
    @Override
    public Session get() {
        return this.memoizedSupplier.get();
    }
    
    public Session build(NinjaProperties ninjaProperties, Provider<Cluster> clusterProvider) {
        final Cluster cluster = clusterProvider.get();
        final String keyspace = ninjaProperties.get("cassandra.keyspace");
        final Session session;
        if (!isEmpty(keyspace)) {
            session = cluster.connect(keyspace);
        } else {
            session = cluster.connect();
        }
        return session;
    }
    
}