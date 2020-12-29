package conf;

import com.greenback.cassandra.ninja.NinjaCassandraModule;
import ninja.conf.FrameworkModule;
import ninja.conf.NinjaClassicModule;
import ninja.utils.NinjaProperties;

public class Module extends FrameworkModule {

    private final NinjaProperties ninjaProperties;
    
    public Module(NinjaProperties ninjaProperties) {
        this.ninjaProperties = ninjaProperties;
    }
    
    @Override
    protected void configure() {
        install(new NinjaClassicModule(ninjaProperties)
            .freemarker(false)
            .xml(false)
            .jpa(false)
            .cache(false));
        install(new NinjaCassandraModule());
    }

}