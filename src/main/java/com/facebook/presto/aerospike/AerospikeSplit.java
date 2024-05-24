package com.facebook.presto.aerospike;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeProvider;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;

public class AerospikeSplit implements ConnectorSplit {
    private final String schemaName;
    private final String tableName;
    private final List<HostAddress> addresses;

    @JsonCreator
    public AerospikeSplit(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName
    ) {
        this.schemaName=schemaName;
        this.tableName=tableName;
        this.addresses= ImmutableList.of();
    }

    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy() {
        return NodeSelectionStrategy.NO_PREFERENCE;
    }

    @Override
    public List<HostAddress> getPreferredNodes(NodeProvider nodeProvider) {
        return addresses;
    }

    @Override
    public Object getInfo() {
        return ImmutableMap.builder().
                put("schemaName",schemaName)
                .put("tableName",tableName)
                .put("address",addresses)
                .build();
    }
}
