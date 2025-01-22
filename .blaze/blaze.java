
import static com.fizzed.blaze.Systems.exec;

import com.fizzed.blaze.project.PublicBlaze;
import com.fizzed.blaze.util.Streamables;
import java.io.IOException;

public class blaze extends PublicBlaze {

    private final String cassandraVersion = config.value("cassandra.version").orElse("3.11.2");
    private final int cassandraPort = 19042;

    public void setup() throws InterruptedException {
        log.info("Setting up cassandra...");
        
        exec(this.resolveContainerExe(), "run", "--name", "fizzed-cassandra-plus",
            "-d", "-p", cassandraPort+":9042",
            "cassandra:"+cassandraVersion)
            .exitValues(0, 125)
            .run();

        this.waitFor("fizzed-cassandra-plus", 60, 1000L, () -> {
            try {
                exec(this.resolveContainerExe(), "exec", "-i", "fizzed-cassandra-plus", "cqlsh", "-e", "describe keyspaces")
                    .pipeError(Streamables.nullOutput())
                    .pipeOutput(Streamables.nullOutput())
                    .run();
                return true;
            } catch (Exception e) {
                return false;
            }
        });

        log.info("Creating cassandra keyspace...");

        String sql = "CREATE KEYSPACE IF NOT EXISTS cassandra_plus_dev WITH REPLICATION = {'class':'SimpleStrategy','replication_factor':1};";
        
        exec(this.resolveContainerExe(), "exec", "-i", "fizzed-cassandra-plus", "cqlsh")
            .pipeInput(sql)
            .exitValues(0, 1)
            .run();
    }

    public void nuke() {
        log.info("Dropping Cassandra keyspace...");

        this.containerNuke("fizzed-cassandra-plus");
    }

    public void ninja() throws IOException {
        exec("mvn", "-Pninja-run", "process-classes").run();
    }
    
}
