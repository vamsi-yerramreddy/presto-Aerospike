package com.facebook.presto.aerospike;

import com.facebook.presto.common.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/*
Column with name and type of the column
* */
public class AerospikeColumn {
    private final String name;
    private  final Type type;

    @JsonCreator
    public AerospikeColumn(
            @JsonProperty("name") String name,
            @JsonProperty("type") Type type
    )                                      {
        this.name=name;
        this.type=  requireNonNull(type,"type is null");
    }
      @JsonProperty
    public String getName() {
        return name;
    }
     @JsonProperty
    public Type getType() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AerospikeColumn column = (AerospikeColumn) o;
        return Objects.equals(name, column.name) &&
                Objects.equals(type, column.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type);
    }

    @Override
    public String toString() {
        return "AerospikeColumn{" +
                "name='" + name + '\'' +
                ", type=" + type +
                '}';
    }
}
