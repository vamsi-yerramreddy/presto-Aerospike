package com.facebook.presto.aerospike;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;
/*
serves as an identifier for specific table in the aerosike
enscapsulates   necessary metadata(id,name,schema) to interact with the table in source 
* */
public class AerospikeTableHandle
        implements ConnectorTableHandle {
    private final String connectorId;
    private final String schemaName;
    private final String tableName;

    /**
     * Constructor for AerospikeTableHandle.
     *
     * @param connectorId The unique identifier for the connector instance.
     * @param schemaName The schema (namespace) name in Aerospike.
     * @param tableName The table (set) name in Aerospike.
     * @JsonCreator used to deserializing Json to object in distributed systems
     * @JsonProperty maps json property names to class's fields and methods
     */
        
    @JsonCreator
    public AerospikeTableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName")  String schemaName,     //namespace in aerospace (similar to dB)        
            @JsonProperty("tableName") String tableName)
    {
        this.connectorId=requireNonNull(connectorId,"connectorId is null");
        this.schemaName=requireNonNull(schemaName,"schema is null")  ;
        this.tableName=requireNonNull(tableName,"tableName is null");
    }
    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }
    @JsonProperty
    public String getSchemaName(){
        return schemaName;
    }
    @JsonProperty
    public String getTableName()
    {
        return tableName  ;
    }

    public SchemaTableName getSchemaTableName(){
        return new SchemaTableName(schemaName,tableName);
    }
    @Override
    public int hashCode(){
        return Objects.hash(connectorId,schemaName,tableName);
    }
    @Override
    public boolean equals(Object o){
        if(this==null)
            return false;
        if(o==null || getClass() !=o.getClass()){
            return false;
        }
        AerospikeTableHandle other = (AerospikeTableHandle) o;
        return Objects.equals(this.connectorId,other.connectorId) &&
                Objects.equals(this.schemaName,other.schemaName)&&
                Objects.equals(this.tableName,other.tableName);
    }
    @Override
    public String toString(){
        return connectorId+ ": " +schemaName + ":" +tableName;
    }
}

