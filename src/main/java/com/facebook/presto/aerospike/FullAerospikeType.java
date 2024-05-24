package com.facebook.presto.aerospike;


import java.util.List;

public interface FullAerospikeType {
    AerospikeType getAerospikeType();
    List<AerospikeType> getTypeArguments();

}
