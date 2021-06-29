package pl.symentis.mapreduce.ondisk;

import com.google.common.collect.ImmutableList;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;

class OnDiskMapOutputTest {

    @Test
    public void emitSingleKeyValues() throws IOException {
        var onDiskMapOutput = new OnDiskMapOutput<String, Integer>();
        onDiskMapOutput.emit("a", 1);
        onDiskMapOutput.emit("a", 2);
        onDiskMapOutput.emit("a", 3);
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> {
            var keys = onDiskMapOutput.keys();
            var values = ImmutableList.copyOf(onDiskMapOutput.values("a"));
            return keys.contains("a") &&
                    values.contains(1) && values.contains(2) && values.contains(3);
        });
    }
}