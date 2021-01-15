package com.fizzed.cassandra.core;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import java.net.InetSocketAddress;
import org.junit.Before;

public class BaseTester {
    
    protected Cluster cluster;
    protected Session session;
    protected Cassandra cassandra;
    
    @Before
    public void setupSession() {
        this.cluster = new Cluster.Builder()
            .addContactPointsWithPorts(new InetSocketAddress("localhost", 19042))
            .build();
        
        this.session = cluster.connect("cassandra_plus_dev");
        
        this.cassandra = new Cassandra(this.session);
    }
    
}