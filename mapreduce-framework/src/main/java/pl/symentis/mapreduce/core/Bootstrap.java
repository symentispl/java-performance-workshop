package pl.symentis.mapreduce.core;

import static java.lang.String.format;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;

public class Bootstrap implements AutoCloseable {

    private final ScanResult scanResult;

    private Bootstrap(ScanResult scanResult) {
        this.scanResult = scanResult;
    }

    public static Bootstrap create() {
        return new Bootstrap(new ClassGraph().enableAllInfo().scan());
    }

    public Class<?> findClassByShortName(String shortName, Class<?> interfaceClass) {
        ClassInfoList classInfoList = scanResult.getClassesImplementing(interfaceClass);
        return classInfoList.stream()
                .filter(classInfo -> classInfo.getSimpleName().equals(shortName))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(format(
                        "cannot find class with short name %s implementing %s", shortName, interfaceClass.getName())))
                .loadClass();
    }

    @Override
    public void close() {
        scanResult.close();
    }
}
