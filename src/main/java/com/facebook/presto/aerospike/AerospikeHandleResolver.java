package com.facebook.presto.aerospike;

import com.facebook.presto.spi.*;
/*
*   Responsible for resolving various handle types to their corresponding classes.
*  Handles in Presto are objects that encapsulate information about database elements such as tables, columns, and splits
* */
/*
*  a "handle" is an object that acts as an identifier or a reference to a specific element within the data source.
* handles are abstractions allow presto to interact with different parts of data source
* */
public class AerospikeHandleResolver implements ConnectorHandleResolver {
    /*
    * represents specific table in the data sources
    * Enscapsulates the metadata and information neccessary to identify and interact with specific table in data source */
    @Override
    public Class<? extends ConnectorTableHandle> getTableHandleClass() {
        return AerospikeTableHandle.class;
    }

    @Override
    public Class<? extends ConnectorTableLayoutHandle> getTableLayoutHandleClass() {
        return AerospikeTableLayoutHandle.class;
    }

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass() {
        return AerospikeColumnHandle.class;
    }

    @Override
    public Class<? extends ConnectorSplit> getSplitClass() {
        return AerospikeSplit.class;
    }
}
