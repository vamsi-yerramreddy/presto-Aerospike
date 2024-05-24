package com.facebook.presto.aerospike;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Scopes;
import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.json.JsonBinder.jsonBinder;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;

import static java.util.Objects.requireNonNull;

public class AerospikeModule implements Module {
    private final String connectorId;
    private final TypeManager typeManager;

    public AerospikeModule(String connectorId, TypeManager typeManager) {

        this.connectorId=requireNonNull(connectorId,"connecor id is null");
        this.typeManager=requireNonNull(typeManager,"typeManager is null");
    }
    @Override
    public void configure(Binder binder){
        binder.bind(TypeManager.class).toInstance(typeManager);
        binder.bind(AerospikeConnector.class).in(Scopes.SINGLETON);
        binder.bind(AerospikeConnectorId.class).toInstance(new AerospikeConnectorId(connectorId));
        binder.bind(AerospikeMetadata.class).in(Scopes.SINGLETON);
        binder.bind(AerospikeClient.class).in(Scopes.SINGLETON);
        binder.bind(AerospikeSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(AerospikeRecordSetProvider.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(AerospikeConfig.class);

        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
        jsonCodecBinder(binder).bindMapJsonCodec(String.class, JsonCodec.listJsonCodec(AerospikeTable.class));

    }
    public static final class TypeDeserializer extends FromStringDeserializer<Type>{
        private final TypeManager typeManager;

        @Inject
        public TypeDeserializer(TypeManager typeManager){
            super(Type.class);
            this.typeManager=requireNonNull(typeManager,"typeManager is null");
        }
        @Override
        protected Type _deserialize(String value, DeserializationContext context){
            return typeManager.getType(TypeSignature.parseTypeSignature(value));
        }
    }
}
