package com.facebook.presto.aerospike;

import com.facebook.presto.spi.ColumnMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Represents a table in the Aerospike connector.
 * This class is immutable and uses JSON annotations for serialization/deserialization.
 */

public class AerospikeTable {
    private final String name;
    private final List<AerospikeColumn> columns;
    private final List<ColumnMetadata> columnsMetadata;

    @JsonCreator
    public AerospikeTable(@JsonProperty("name") String name
            , @JsonProperty("columns") List<AerospikeColumn> columns) {
        this.name = requireNonNull(name, "name is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns are null"));

        // Generate column metadata from the column definitions
        //NEed to generate the metadata from the columns list provided
        
        ImmutableList.Builder<ColumnMetadata> columnsMetadataBuilder = ImmutableList.builder();
        for (AerospikeColumn column : this.columns) {
            columnsMetadataBuilder.add(new ColumnMetadata(column.getName(), column.getType()));
        }
        this.columnsMetadata = columnsMetadataBuilder.build();
    }
    @JsonProperty
    public List<ColumnMetadata> getColumnsMetadata(){
        return columnsMetadata;
    }
        @JsonProperty
                public String getName(){
            return name;
        }
        @JsonProperty
                public List<AerospikeColumn> getColumns(){
            return columns;
        }



}
