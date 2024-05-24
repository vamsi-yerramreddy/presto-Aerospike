package com.facebook.presto.aerospike;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class AerospikeRecordSetProvider implements ConnectorRecordSetProvider {
    private final String connectorId;
    @Inject
    public AerospikeRecordSetProvider(AerospikeConnectorId connectorId){
        this.connectorId=  requireNonNull(connectorId,"connectorId is null").toString();

    }
    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transactionHandle,
                                  ConnectorSession session, ConnectorSplit split,
                                  List<? extends ColumnHandle> columns) {
        requireNonNull(split,"partitionChuk is null");

        AerospikeSplit aerospikeSplit = (AerospikeSplit)split;

        ImmutableList.Builder<AerospikeColumnHandle> handles= ImmutableList.builder();
        for(ColumnHandle handle : columns){
            handles.add((AerospikeColumnHandle) handle);
        }

        return new AerospikeRecordSet(aerospikeSplit,handles.build());
    }
}
