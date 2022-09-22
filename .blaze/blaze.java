
import com.fizzed.blaze.Contexts;
import static com.fizzed.blaze.Systems.exec;
import com.fizzed.blaze.Task;
import com.fizzed.blaze.util.Streamables;
import java.io.IOException;
import org.slf4j.Logger;

public class blaze {
    private final Logger log = Contexts.logger();
    
    final private int clusterNodeCount = 5;
    
    @Task(order=1, value="Setup depenencies")
    public void setup() throws InterruptedException {
        log.info("Setting up cassandra cluster...");
        
        log.info("Setting up cassandra network...");
        exec("docker", "network", "create", "fizzed-cassandra-net")
            .exitValues(0, 125)
            .run();
        
        // build full "seed" list of servers
        String seeds = "";
        for (int i = 1; i <= clusterNodeCount; i++) {
            if (seeds.length() > 0) {
                seeds += ",";
            }
            seeds += "fizzed-cassandra-" + i;
        }
        
        for (int i = 1; i <= clusterNodeCount; i++) {
            String containerName = "fizzed-cassandra-" + i;
            
            exec("docker", "run", "--name", containerName,
                "-e", "MAX_HEAP_SIZE=512M",
                "-e", "HEAP_NEWSIZE=128M",
                "--net", "fizzed-cassandra-net",
                "-e", "CASSANDRA_SEEDS="+seeds,
                "-e", "CASSANDRA_CLUSTER_NAME=fizzed-cassandra-plus",
                "-e", "CASSANDRA_ENDPOINT_SNITCH=GossipingPropertyFileSnitch",
                "-e", "CASSANDRA_NUM_TOKENS=256",
                "-p", (19042-1+i)+":9042",
                "-d",
                "cassandra:3.11.2")
                .exitValues(0, 125)
                .run();

            // wait for cassandra to start
            int rv = -1;
            for (int j = 0; j < 30 && rv != 0; j++) {
                Thread.sleep(1000L);
                log.info("Waiting for cassandra {}...", containerName);
                rv = (int)exec("docker", "exec", "-i", containerName, "cqlsh")
                    .pipeInput("SELECT * from system.local")
                    .pipeOutput(Streamables.nullOutput())
                    .pipeError(Streamables.nullOutput())
                    .exitValues(0, 1)
                    .run();
            }
        }
        
        String sql = ""
            + "CREATE KEYSPACE IF NOT EXISTS cassandra_plus_dev WITH REPLICATION = {'class':'SimpleStrategy','replication_factor':3};\n"
            + "";
        
        exec("docker", "exec", "-i", "fizzed-cassandra-1", "cqlsh")
            .pipeInput(sql)
            .exitValues(0, 1)
            .run();
    }

    @Task(order=2, value="Nukes all setup. Should get you starting from scratch.")
    public void nuke() {
        log.info("Dropping cassandra nodes...");
        
        for (int i = 1; i <= clusterNodeCount; i++) {
            exec("docker", "rm", "-f", "fizzed-cassandra-" + i)
                .exitValues(0, 1)
                .run();
        }
        
        log.info("Nuking cassandra network...");
        exec("docker", "network", "rm", "fizzed-cassandra-net")
            .exitValues(0, 1)
            .run();
    }

    @Task(order=11, value="Run ninja")
    public void ninja() throws IOException {
        exec("mvn", "-Pninja-run", "process-classes").run();
    }
    
}
