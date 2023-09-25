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

        var influxOrg = propertiesConfiguration.get(String.class, "influxOrg");
        var influxBucket = propertiesConfiguration.get(String.class, "influxBucket");
        var influxToken = propertiesConfiguration.get(String.class, "influxToken");

        InfluxConfig influxConfig = new InfluxConfig() {
            @Override
            public String org() {
                return influxOrg;
            }

            @Override
            public String bucket() {
                return influxBucket;
            }

            @Override
            public String token() {
                return influxToken;
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
