
import com.fizzed.blaze.Contexts;
import static com.fizzed.blaze.Systems.exec;
import com.fizzed.blaze.Task;
import com.fizzed.blaze.util.Streamables;
import java.io.IOException;
import org.slf4j.Logger;

public class blaze {
    private final Logger log = Contexts.logger();
    
    @Task(order=1, value="Setup depenencies")
    public void setup() throws InterruptedException {
        log.info("Setting up cassandra...");
        
        exec("docker", "run", "--name", "fizzed-cassandra-plus",
            "-d", "-p", "19042:9042",
            "cassandra:3.11.2")
            .exitValues(0, 125)
            .run();
        
        // wait for cassandra to start
        int rv = -1;
        for (int i = 0; i < 30 && rv != 0; i++) {
            Thread.sleep(1000L);
            log.info("Waiting for cassandra...");
            rv = (int)exec("docker", "exec", "-i", "fizzed-cassandra-plus", "cqlsh")
                .pipeInput("SELECT * from system.local")
                .pipeOutput(Streamables.nullOutput())
                .pipeError(Streamables.nullOutput())
                .exitValues(0, 1)
                .run();
        }
        
        String sql = ""
            + "CREATE KEYSPACE IF NOT EXISTS cassandra_plus_dev WITH REPLICATION = {'class':'SimpleStrategy','replication_factor':1};\n"
            //+ "CREATE ROLE ninjadev WITH PASSWORD = 'test' AND LOGIN = true;\n"
            //+ "GRANT ALL PERMISSIONS on KEYSPACE ninja_dev to ninjadev;";
            + "";
        
        exec("docker", "exec", "-i", "fizzed-cassandra-plus", "cqlsh")
            .pipeInput(sql)
            .exitValues(0, 1)
            .run();
    }

    @Task(order=2, value="Nukes all setup. Should get you starting from scratch.")
    public void nuke() {
        log.info("Dropping Cassandra keyspace...");
        
        exec("docker", "rm", "-f", "fizzed-cassandra-plus")
            .exitValues(0, 1)
            .run();
    }

    @Task(order=11, value="Run ninja")
    public void ninja() throws IOException {
        exec("mvn", "-Pninja-run", "process-classes").run();
    }
    
}
