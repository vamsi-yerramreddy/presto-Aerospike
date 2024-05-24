package com.facebook.presto.aerospike;

import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class AerospikeTableLayoutHandle implements ConnectorTableLayoutHandle {
    private final AerospikeTableHandle table;

    @JsonCreator
    public AerospikeTableLayoutHandle(@JsonProperty("table") AerospikeTableHandle table)
    {
        this.table=table;
    }
    @JsonProperty
    public AerospikeTableHandle getTablehandle()
    {
        return  table;
    }
    @Override
    public boolean equals(Object o)
    {
        if ( this==o)
            return true;
        if(o==null || getClass() != o.getClass())
        {
            return false;
        }
        AerospikeTableLayoutHandle that = (AerospikeTableLayoutHandle) o;
        return Objects.equals(table,that.table);

    }
    @Override
    public int hashCode()
    {
             return Objects.hash(table);
    }
    @Override
    public String toString(){
             return table.toString();
    }
}
