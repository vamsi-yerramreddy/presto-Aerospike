package com.facebook.presto.aerospike;

import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.inject.Inject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AerospikeSplitManager implements ConnectorSplitManager {
    private final AerospikeClient client;
    @Inject
    public AerospikeSplitManager(AerospikeClient client){
        this.client=client;
    }
    
    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle,
                                          ConnectorSession session, ConnectorTableLayoutHandle layout,
                                          SplitSchedulingContext splitSchedulingContext) {
        AerospikeTableHandle tablehandle = ((AerospikeTableLayoutHandle)layout).getTablehandle();
        AerospikeTable table = client.getTable(tablehandle.getSchemaName(),tablehandle.getTableName()) ;
        if(table==null){
            throw new TableNotFoundException(tablehandle.getSchemaTableName());
        }
        List<ConnectorSplit> splits= new ArrayList<>();
        splits.add(new AerospikeSplit(tablehandle.getSchemaName(),tablehandle.getTableName())) ;
        Collections.shuffle(splits);
        return new FixedSplitSource(splits);
    }
}
