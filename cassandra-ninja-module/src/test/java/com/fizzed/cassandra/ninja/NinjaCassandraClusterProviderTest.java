package com.fizzed.cassandra.ninja;

import com.datastax.driver.core.Cluster;
import ninja.utils.NinjaProperties;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NinjaCassandraClusterProviderTest {
 
    private NinjaProperties ninjaProperties;
    
    @Before
    public void before() {
        this.ninjaProperties = mock(NinjaProperties.class);
    }
    
    @Test
    public void buildCluster() {
        when(ninjaProperties.getStringArray("cassandra.contact_points"))
            .thenReturn(new String[] { "localhost:9403" });
        
        Cluster.Builder clusterBuilder = new NinjaCassandraClusterProvider(ninjaProperties)
            .createBuilder();
        
        assertThat(clusterBuilder.getContactPoints(), hasSize(1));
        
        Cluster cluster = clusterBuilder.build();
    }
    
}