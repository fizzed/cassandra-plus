package controllers;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import java.util.UUID;
import javax.inject.Inject;
import javax.inject.Singleton;
import ninja.Result;
import ninja.Results;

@Singleton
public class ApplicationController {

    private final Session session;
    
    @Inject
    public ApplicationController(
            Session session) {
        this.session = session;
    }
    
    public Result home() {
        ResultSet rs = session.execute("SELECT release_version FROM system.local");
        String version = rs.one().getString(0);
        
        Row row = session.execute("SELECT * FROM test LIMIT 1").one();
        UUID uuid = row.getUUID("id");
        String name = row.getString("name");
        
        return Results.html()
            .renderRaw(
                "Cassandra Version: " + version + "<br/>" +
                "Test Row: "+ uuid + ": " + name);
    }
    
}