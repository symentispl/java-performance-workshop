package pl.symentis.mapreduce.server;

import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.OptionType;
import com.github.rvesse.airline.annotations.restrictions.File;
import com.github.rvesse.airline.annotations.restrictions.Required;

public class GlobalOptions {

    @Option(type = OptionType.GLOBAL, name = "--config")
    @Required
    @File(mustExist = true)
    protected java.io.File configFile;
}
