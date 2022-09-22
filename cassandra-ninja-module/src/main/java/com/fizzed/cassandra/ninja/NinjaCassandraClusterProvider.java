package com.fizzed.cassandra.ninja;

import com.datastax.driver.core.Cluster;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import java.net.InetSocketAddress;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import ninja.utils.NinjaProperties;

@Singleton
public class NinjaCassandraClusterProvider implements Provider<Cluster> {

    private final NinjaProperties ninjaProperties;
    private final String name;
    private final Supplier<Cluster> memoizedSupplier;
    
    @Inject
    public NinjaCassandraClusterProvider(
            NinjaProperties ninjaProperties,
            String name) {
        
        this.ninjaProperties = ninjaProperties;
        this.name = name;
        this.memoizedSupplier = Suppliers.memoize(() -> {
            return this.build();
        });
    }
    
    @Override
    public Cluster get() {
        return this.memoizedSupplier.get();
    }
    
    public Cluster build() {
        return this.createBuilder().build();
    }
    
    public Cluster.Builder createBuilder() {
        final Cluster.Builder clusterBuilder = Cluster.builder();
        
        final String[] contactPoints = ninjaProperties.getStringArray(this.name + ".contact_points");
        if (contactPoints != null) {
            for (String contactPoint : contactPoints) {
                if (contactPoint.contains(":")) {
                    String address = contactPoint.substring(0, contactPoint.indexOf(":"));
                    int port = Integer.parseInt(contactPoint.substring(contactPoint.indexOf(":")+1));
                    clusterBuilder.addContactPointsWithPorts(new InetSocketAddress(address, port));
                } else {
                    clusterBuilder.addContactPoint(contactPoint);
                }
            }
        }
        
        final String username = ninjaProperties.get(this.name + ".username");
        if (username != null) {
            String password = ninjaProperties.getOrDie(this.name + ".password");
            clusterBuilder.withCredentials(username, password);
        }
        
        // disable jmx reporting?
        if (!ninjaProperties.getBooleanWithDefault(this.name + ".jmx", false)) {
            clusterBuilder.withoutJMXReporting();
        }
        
        return clusterBuilder;
    }

}