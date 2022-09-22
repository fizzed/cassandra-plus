package com.fizzed.cassandra.ninja;

import com.datastax.driver.core.Session;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import ninja.lifecycle.Start;
import ninja.utils.NinjaProperties;
import org.cognitor.cassandra.migration.Database;
import org.cognitor.cassandra.migration.MigrationRepository;
import org.cognitor.cassandra.migration.MigrationTask;

@Singleton
public class NinjaCassandraMigrate {
    
    private final NinjaProperties ninjaProperties;
    private final String name;
    private final Provider<Session> sessionProvider;
    
    @Inject
    public NinjaCassandraMigrate(
            NinjaProperties ninjaProperties,
            String name,
            Provider<Session> sessionProvider) {
        
        this.ninjaProperties = ninjaProperties;
        this.name = name;
        this.sessionProvider = sessionProvider;
    }

    @Start(order = 51)
    public void migrate() {
        Boolean migrateEnabled = this.ninjaProperties.getBooleanWithDefault(this.name + ".migrate.enabled", Boolean.FALSE);
        String migrateScriptPath = this.ninjaProperties.getWithDefault(this.name + ".migrate.script_path", "/db/cassandra");
        if (migrateEnabled) {
            // lazily get session only if migrate is enabled
            Session session = this.sessionProvider.get();
            MigrationRepository migrationRepository = new MigrationRepository(migrateScriptPath);
            Database database = new Database(session.getCluster(), session.getLoggedKeyspace());
            MigrationTask migration = new MigrationTask(database, migrationRepository);
            migration.migrate();
        }
    }
    
}