package pl.symentis.mapreduce.core;

import java.util.Map;

public interface JobFactory
{

    Job create( Map<String,String> context);

}
