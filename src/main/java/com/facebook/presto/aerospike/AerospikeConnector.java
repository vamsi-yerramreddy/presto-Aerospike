package com.facebook.presto.aerospike;

import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.google.inject.Inject;

import static com.facebook.presto.spi.transaction.IsolationLevel.READ_UNCOMMITTED;
import static com.facebook.presto.spi.transaction.IsolationLevel.checkConnectorSupports;

public class AerospikeConnector implements Connector {
    private final AerospikeMetadata aerospikeMetadata;
    private final AerospikeSplitManager splitManager;
    private final AerospikeRecordSetProvider recordSetProvider;

    @Inject
    public AerospikeConnector(  AerospikeMetadata metadata,AerospikeSplitManager splitManager,
                                AerospikeRecordSetProvider recordSetProvider)       {
        this.aerospikeMetadata=metadata;
        this.splitManager=splitManager;
        this.recordSetProvider=recordSetProvider;
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly) {
        checkConnectorSupports(READ_UNCOMMITTED,isolationLevel);
        return AerospikeTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle) {
        return aerospikeMetadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        return splitManager;
    }
}
