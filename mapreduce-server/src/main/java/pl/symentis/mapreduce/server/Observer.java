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
import java.util.List;

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

    public Observer setupRegistry(String processNane) {
        InfluxConfig influxConfig = new InfluxConfig() {
            @Override
            public String org() {
                return "symentis";
            }

            @Override
            public String bucket() {
                return "rakiety";
            }

            @Override
            public String token() {
                return "qdP2pk0pLtsFAdahQAa8uhyN_ezdg5X0KXTDhcu6AYOAxK-AnKS9fQHlYWiacDN6jSoGzv8VXCfsmYbzMdUk2A==";
            }

            @Override
            public String get(String k) {
                return null; // accept the rest of the defaults
            }
        };
        registry = new InfluxMeterRegistry(influxConfig, Clock.SYSTEM);

        registry.config().meterFilter(commonTags(List.of(Tag.of("process", processNane))));

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
