package com.facebook.presto.aerospike;

import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class AerospikeMetadata implements ConnectorMetadata {
    private final String connectorId;
    private final AerospikeClient aerospikeClient;

    @Inject
    public AerospikeMetadata(AerospikeConnectorId connectorId, AerospikeClient client){
        this.connectorId=requireNonNull(connectorId,"connectorId is null").toString();
        this.aerospikeClient = requireNonNull(client,"client is null");
    }
    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return listSchemaNames();
    }

    public List<String> listSchemaNames(){
        return ImmutableList.copyOf(aerospikeClient.getSchemaNames());
    }
    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
        AerospikeTable table = aerospikeClient.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (table == null) {
            return null;
        }
        return new AerospikeTableHandle(connectorId, tableName.getSchemaName(), tableName.getTableName());
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle) {
        // Implementation will be added later
        return null;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle) {
        AerospikeTableHandle aerospikeTableHandle = (AerospikeTableHandle) tableHandle;
        AerospikeTable table = aerospikeClient.getTable(aerospikeTableHandle.getSchemaName(), aerospikeTableHandle.getTableName());
        if (table == null) {
            return null;
        }
        return new ConnectorTableMetadata(new SchemaTableName(aerospikeTableHandle.getSchemaName(), aerospikeTableHandle.getTableName()), table.getColumnsMetadata());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
        AerospikeTableHandle aerospikeTableHandle = (AerospikeTableHandle) tableHandle;
        AerospikeTable table = aerospikeClient.getTable(aerospikeTableHandle.getSchemaName(), aerospikeTableHandle.getTableName());
        if (table == null) {
            return null;
        }
        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (AerospikeColumn column : table.getColumns()) {
            columnHandles.put(column.getName(), new AerospikeColumnHandle(column.getName(), column.getType()));
        }
        return columnHandles.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
        AerospikeColumnHandle aerospikeColumnHandle = (AerospikeColumnHandle) columnHandle;
        return new ColumnMetadata(aerospikeColumnHandle.getColumnName(), aerospikeColumnHandle.getColumnType());
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix) {
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> builder = ImmutableMap.builder();
        if (prefix.getTableName() == null) {
            for (String schemaName : listSchemaNames()) {
                for (String tableName : aerospikeClient.getTableNames(schemaName)) {
                    AerospikeTable table = aerospikeClient.getTable(schemaName, tableName);
                    builder.put(new SchemaTableName(schemaName, tableName), table.getColumnsMetadata());
                }
            }
        } else {
            AerospikeTable table = aerospikeClient.getTable(prefix.getSchemaName(), prefix.getTableName());
            if (table != null) {
                builder.put(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()), table.getColumnsMetadata());
            }
        }
        return builder.build();
    }
}
