package pl.symentis.mapreduce.server;

import com.google.gson.Gson;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;

public class BootstrapIT
{

    private Bootstrap bootstrap;

    @BeforeEach
    public void setUp(@TempDir Path tempDir) throws Exception
    {
        bootstrap = new Bootstrap.Builder().jobsDir( tempDir).build();
        bootstrap.start();
    }

    @AfterEach
    public void tearDown() throws IOException
    {
        bootstrap.shutdown();
    }

    @Test
    public void test() throws IOException, InterruptedException
    {
        Path jobsDir = bootstrap.jobsDir();
        System.out.println(jobsDir);
        var jobDefinition = new JobDefinition( "../mapreduce-wordcount-bundle/target/mapreduce-wordcount-bundle-0.0.1-SNAPSHOT.jar",
                                          Map.of( "filename", "../mapreduce-wordcount/src/test/resources/big.txt" ) );
        var jobDefPath = jobsDir.resolve( "job.json" );
        while(true){
            var tempFile = Files.createTempFile( jobsDir, "job", ".json" );
            try(var writer = Files.newBufferedWriter( tempFile, StandardOpenOption.WRITE )){
            new Gson().toJson( jobDefinition, writer );

        }
            Thread.sleep( 2000 );
        }
//        System.in.read();
    }
}
