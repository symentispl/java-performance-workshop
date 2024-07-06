package pl.symentis.mapreduce.server;

import static io.micrometer.core.instrument.config.MeterFilter.commonTags;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.observation.DefaultMeterObservationHandler;
import io.micrometer.influx.InfluxConfig;
import io.micrometer.influx.InfluxMeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;

public class Observer {
    private static Observer instance;

    private InfluxMeterRegistry registry;
    private ObservationRegistry observationRegistry;

    private Observer() {}

    public static Observer getInstance() {
        if (instance == null) {
            instance = new Observer();
        }
        return instance;
    }

    public Observer setupRegistry(String processName, File configFile) {

        var propertiesConfiguration = new PropertiesConfiguration();
        try {
            propertiesConfiguration.read(new FileReader(configFile));
        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        InfluxConfig influxConfig = new InfluxConfig() {
            @Override
            public String org() {
                return propertiesConfiguration.get(String.class, "influxOrg");
            }

            @Override
            public String bucket() {
                return propertiesConfiguration.get(String.class, "influxBucket");
            }

            @Override
            public String token() {
                return propertiesConfiguration.get(String.class, "influxToken");
            }

            @Override
            public String uri() {
                return propertiesConfiguration.get(String.class, "influxUri");
            }

            @Override
            public Duration step() {
                return Duration.ofSeconds(5);
            }

            @Override
            public String get(String k) {
                return null; // accept the rest of the defaults
            }
        };
        registry = new InfluxMeterRegistry(influxConfig, Clock.SYSTEM);

        registry.config().meterFilter(commonTags(List.of(Tag.of("process", processName))));

        return this;
    }

    public Observer setupObservationRegistry() {
        observationRegistry = ObservationRegistry.create();
        observationRegistry.observationConfig().observationHandler(new DefaultMeterObservationHandler(registry));
        return this;
    }

    public Observer turnOnJvmMetrics() {
        new ClassLoaderMetrics().bindTo(registry);
        new JvmMemoryMetrics().bindTo(registry);
        new JvmGcMetrics().bindTo(registry);
        new ProcessorMetrics().bindTo(registry);
        new JvmThreadMetrics().bindTo(registry);
        return this;
    }

    public InfluxMeterRegistry getRegistry() {
        return registry;
    }

    public ObservationRegistry getObservationRegistry() {
        return observationRegistry;
    }
}
