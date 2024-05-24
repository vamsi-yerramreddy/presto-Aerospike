package com.facebook.presto.aerospike;

import com.facebook.airlift.json.JsonCodec;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
/**
 * AerospikeClient is responsible for managing and providing access to Aerospike schemas and tables.
 */

public class AerospikeClient {
    private final Supplier<Map<String, Map<String,AerospikeTable>>> schemas;

    /**
     * Constructor for AerospikeClient.
     *
     * @param config The Aerospike configuration.
     * @param catalogCode The JSON codec for decoding the catalog.
     */

    @Inject
    public AerospikeClient(AerospikeConfig config, JsonCodec<Map<String, List<AerospikeTable>>>catalogCode){
        schemas= Suppliers.memoize(schemasSupplier(catalogCode,config.getMetadata()));

    }
    public Set<String> getSchemaNames() {
        return schemas.get().keySet();
    }
    /**
     * Gets a specific table by schema and table name.
     *
     * @param schema The schema name.
     * @param tableName The table name.
     * @return The AerospikeTable object, or null if not found.
     */
    public AerospikeTable getTable(String schema, String tableName){
        Map<String,AerospikeTable> tables = schemas.get().get(schema);
        if(tables==null){
            return null;
        }
        return tables.get(tableName);
    }
    
    public Set<String> getTableNames(String schmea){
        Map<String,AerospikeTable> tables =schemas.get().get(schmea);
        if(tables==null){
               return ImmutableSet.of();
        }
        return tables.keySet();
    }
    /**
     * Creates a supplier for the schemas.
     *   memoized the shchemas ensuring that the schemas are loaded only once and cached for future use.
     * @param catalogCodec The JSON codec for decoding the catalog.
     * @param metadata The metadata JSON string.
     * @return The supplier for the schemas.
     */

    private static Supplier<Map<String,Map<String,AerospikeTable>>> schemasSupplier(
            final JsonCodec<Map<String,List<AerospikeTable>>> catalogCodec, final String metadata)
    {
        return ()->{
            try{
                return lookupSchemas(catalogCodec, metadata);
            }catch(IOException e){
                              throw new UncheckedIOException(e);
            }
        }       ;
    }
    /**
     * Looks up the schemas from the provided JSON codec and metadata.
     *   Converts the JSON-encoded catalog to the required map format. This method handles the JSON decoding and conversion logic
     * @param catalogCodec The JSON codec for decoding the catalog.
     * @param metadata The metadata JSON string.
     * @return The map of schemas and their tables.
     * @throws IOException If there is an issue reading the JSON.
     */
    private static Map<String, Map<String, AerospikeTable>> lookupSchemas(
            JsonCodec<Map<String, List<AerospikeTable>>> catalogCodec, String metadata) throws IOException {
        Map<String, List<AerospikeTable>> catalog = catalogCodec.fromJson(metadata);

        // Convert the catalog to the required format
        ImmutableMap.Builder<String, Map<String, AerospikeTable>> schemasBuilder = ImmutableMap.builder();
        for (Map.Entry<String, List<AerospikeTable>> entry : catalog.entrySet()) {
            ImmutableMap.Builder<String, AerospikeTable> tablesBuilder = ImmutableMap.builder();
            for (AerospikeTable table : entry.getValue()) {
                tablesBuilder.put(table.getName(), table);
            }
            schemasBuilder.put(entry.getKey(), tablesBuilder.build());
        }
        return schemasBuilder.build();
    }

    public int getRecordCount() {
        return 0;

    }

    public <E> List<E> getCurrentRecord(int currentPosition) {
        return null;
    }
}
