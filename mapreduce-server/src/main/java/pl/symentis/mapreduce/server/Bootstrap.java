package pl.symentis.mapreduce.server;

import com.google.gson.Gson;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.influx.InfluxConfig;
import io.micrometer.influx.InfluxMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.symentis.mapreduce.batching.BatchingParallelMapReduce;
import pl.symentis.mapreduce.core.Job;
import pl.symentis.mapreduce.core.JobFactory;
import pl.symentis.mapreduce.core.MapReduce;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Bootstrap
{

    private static final Logger LOG = LoggerFactory.getLogger( Bootstrap.class );
    private final Path jobsDir;
    private final MeterRegistry registry;
    private final WatchService watchService;
    private final WatchKey watchKey;
    private final ScheduledExecutorService executorService;
    private final Gson gson;
    private final MapReduce mapReduce;

    public Bootstrap( Path jobsDir,
                      MeterRegistry registry,
                      WatchService watchService,
                      WatchKey watchKey,
                      ScheduledExecutorService executorService,
                      Gson gson,
                      MapReduce mapReduce )
    {

        this.jobsDir = jobsDir;
        this.registry = registry;
        this.watchService = watchService;
        this.watchKey = watchKey;
        this.executorService = executorService;
        this.gson = gson;
        this.mapReduce = mapReduce;
    }

    public void start()
    {
        executorService.scheduleAtFixedRate( () ->
                                             {
                                                 LOG.debug( "polling watch key {} for filesystem modifications", watchKey.watchable() );
                                                 var watchEvents = watchKey.pollEvents();
                                                 for ( WatchEvent<?> watchEvent : watchEvents )
                                                 {
                                                     Path context = (Path) watchEvent.context();
                                                     LOG.debug( "new file {}", context );
                                                     if(context.getFileName().toString().endsWith( ".json" )){
                                                         try(var reader = Files.newBufferedReader( jobsDir.resolve( context ))){
                                                             var jobDefinition = gson.fromJson( reader, JobDefinition.class );
                                                             var job = loadJob( Paths.get( jobDefinition.getCodeUri() ), jobDefinition.getContext() );
                                                             var hashMap = new HashMap<String,Long>();
                                                             mapReduce.run( job.input(),
                                                                            job.mapper(),
                                                                            job.reducer(),
                                                                            hashMap::put);

                                                         }
                                                         catch ( IOException e )
                                                         {
                                                             throw new RuntimeException( e );
                                                         }
                                                     }
                                                 }
                                             }, 0, 1, TimeUnit.SECONDS );

    }

    public static void main( String[] args ) throws IOException
    {
        var bootstrap = new Builder().jobsDir( Paths.get( "." ) ).build();
        bootstrap.start();
    }

    private static Job loadJob( Path codeUri, Map<String,String> context )
    {
        try
        {
            var absolutePath = codeUri.toAbsolutePath();
            if(!Files.isRegularFile( absolutePath )){
                throw new FileNotFoundException( "file doesn't exist: " + absolutePath );
            }
            var url = absolutePath.toUri().toURL();
            LOG.debug( "loading job from code url {}", url );
            ClassLoader jobClassLoader = URLClassLoader.newInstance( new URL[]{url} );
            var jobServiceLoader = ServiceLoader.load( JobFactory.class, jobClassLoader );
            var first = jobServiceLoader.findFirst();
            if ( first.isPresent() )
            {
                var job = first.get();
                LOG.debug( "loaded job code", job );
                return job.create( context );
            }
            else
            {
                LOG.warn( "job service not found" );
            }
        }
        catch ( Throwable e )
        {
            LOG.error( "job failed", e );
        }
        return null;
    }

    public void shutdown() throws IOException
    {

        if(!executorService.isShutdown()){
            executorService.shutdown();
            try
            {
                executorService.awaitTermination( 1, TimeUnit.MINUTES );
            }
            catch ( InterruptedException e )
            {
                throw new RuntimeException( e );
            }
        }

        watchKey.cancel();
        watchService.close();

        registry.close();
    }

    public Path jobsDir()
    {
        return jobsDir;
    }

    public static class Builder
    {
        private Path jobsDir;

        public Builder jobsDir( Path dir )
        {
            jobsDir = dir;
            return this;
        }

        public Bootstrap build() throws IOException
        {
            LOG.info( "configuring influx metrics registry" );
            InfluxConfig config = new InfluxConfig()
            {

                @Override
                public String org()
                {
                    return "pitradwar";
                }

                @Override
                public String bucket()
                {
                    return "metrics";
                }

                @Override
                public String token()
                {
                    return "GE12DEEoV8UU9Ooak5Br2_5wrq5YnI1FwYw51bpzii4MX0I39Tc07lO9jRnVUaDS2NbLcfIKAtj5YrmD2WgVRQ=="; // FIXME: This should be securely bound rather than hard-coded, of course.
                }

                @Override
                public Duration step()
                {
                    return Duration.ofSeconds( 5 );
                }

                @Override
                public String get( String k )
                {
                    return null; // accept the rest of the defaults
                }
            };
            MeterRegistry registry = new InfluxMeterRegistry( config, Clock.SYSTEM );
            new ClassLoaderMetrics().bindTo( registry );
            new JvmMemoryMetrics().bindTo( registry );
            new JvmGcMetrics().bindTo( registry );
            new ProcessorMetrics().bindTo( registry );
            new JvmThreadMetrics().bindTo( registry );

            // will watch input dir for tasks (which will be jars)
            var watchService = FileSystems.getDefault().newWatchService();
            var watchKey = jobsDir.register( watchService, StandardWatchEventKinds.ENTRY_CREATE );
            var mapReduce = new BatchingParallelMapReduce.Builder()
                    .withBatchSize( 1000 )
                    .withPhaserMaxTasks( 10000 )
                    .withThreadPoolSize( Runtime.getRuntime().availableProcessors() )
                    .build();

            var executorService = Executors.newScheduledThreadPool( 1 );
            Gson gson = new Gson();

            return new Bootstrap(jobsDir, registry, watchService, watchKey, executorService, gson, mapReduce);
        }
    }
}
