package com.facebook.presto.aerospike;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class AerospikeRecordSet implements RecordSet {

    private final List<AerospikeColumnHandle> columnhandles;
    private final List<Type> columnTypes;

    public AerospikeRecordSet(
            AerospikeSplit split, List<AerospikeColumnHandle> columnhandles)
    {
             requireNonNull(split,"split is null");
             this.columnhandles=requireNonNull(columnhandles,"column handle is null");
             ImmutableList.Builder<Type> types = ImmutableList.builder();

        for(AerospikeColumnHandle column : columnhandles){
            types.add(column.getColumnType());
        }
        this.columnTypes=types.build();

    }

    @Override
    public List<Type> getColumnTypes() {
        return columnTypes;
    }
    @Override
    public RecordCursor cursor() {
        try {
            // Create an AerospikeRecordSet using the column handles and schema
            AerospikeRecordSet recordSet = new AerospikeRecordSet(null,null);
            // Return the cursor from the record set
            return recordSet.cursor();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


}
