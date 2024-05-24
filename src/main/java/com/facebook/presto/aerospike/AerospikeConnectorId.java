package com.facebook.presto.aerospike;
import java.util.Objects;

import static java.util.Objects.requireNonNull;


public final class AerospikeConnectorId {

    private final String id;

    public AerospikeConnectorId(String id){
        this.id=  requireNonNull(id,"id is null");

    }
    @Override
    public String toString(){
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AerospikeConnectorId that = (AerospikeConnectorId) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
