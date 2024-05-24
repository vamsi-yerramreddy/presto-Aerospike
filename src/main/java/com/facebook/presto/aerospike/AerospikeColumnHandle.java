
package com.facebook.presto.aerospike;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.common.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;
/*
 AerospikeColumn for metadata management and AerospikeColumnHandle for query execution
Represents a handle or reference to a column when executing queries used bu presto engine during query planning and execution
Used by the Presto engine during query planning and execution.
* */

public class AerospikeColumnHandle implements ColumnHandle {
      private final String columnName;
      private final Type columnType;

      @JsonCreator
      public AerospikeColumnHandle(
              @JsonProperty("columnName") String columnName,
              @JsonProperty("columnType") Type columnType) {
            this.columnName = requireNonNull(columnName, "columnName is null");
            this.columnType = requireNonNull(columnType, "columnType is null");
      }

      @JsonProperty
      public String getColumnName() {
            return columnName;
      }

      @JsonProperty
      public Type getColumnType() {
            return columnType;
      }

      @Override
      public int hashCode() {
            return Objects.hash(columnName, columnType);
      }

      @Override
      public boolean equals(Object obj) {
            if (this == obj) {
                  return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                  return false;
            }
            AerospikeColumnHandle other = (AerospikeColumnHandle) obj;
            return Objects.equals(this.columnName, other.columnName) &&
                    Objects.equals(this.columnType, other.columnType);
      }

      @Override
      public String toString() {
            return "AerospikeColumnHandle{" +
                    "columnName='" + columnName + '\'' +
                    ", columnType=" + columnType +
                    '}';
      }
}
