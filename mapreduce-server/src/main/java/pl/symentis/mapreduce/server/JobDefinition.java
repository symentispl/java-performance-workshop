package pl.symentis.mapreduce.server;

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.Map;

public class JobDefinition
{
    private String codeUri;
    private Map<String,String> context;

    public JobDefinition( String codeUri, Map<String,String> context )
    {
        this.codeUri = codeUri;
        this.context = context;
    }

    public String getCodeUri()
    {
        return codeUri;
    }

    public void setCodeUri( String codeUri )
    {
        this.codeUri = codeUri;
    }

    public Map<String,String> getContext()
    {
        return context;
    }

    public void setContext( Map<String,String> context )
    {
        this.context = context;
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder( this )
                .append( "codeUri", codeUri )
                .append( "context", context )
                .toString();
    }
}
