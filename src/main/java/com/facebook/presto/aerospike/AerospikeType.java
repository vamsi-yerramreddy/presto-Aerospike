package com.facebook.presto.aerospike;

import com.facebook.presto.common.type.*;

import java.nio.ByteBuffer;
import java.util.List;

import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static java.util.Objects.requireNonNull;

public enum AerospikeType implements FullAerospikeType
{

    /*Data Types in the Aerospike*/
    FLOAT(RealType.REAL,Float.class),
    INT(IntegerType.INTEGER, Integer.class)   ,
    BOOLEAN(BooleanType.BOOLEAN,Boolean.class),
    BLOB(VarbinaryType.VARBINARY, ByteBuffer.class),
    ASCII(createUnboundedVarcharType(),String.class),
    DOUBLE(DoubleType.DOUBLE, Double.class),

    /*Collection DATA TYPES*/
    LIST(createUnboundedVarcharType(),null),
    MAP(createUnboundedVarcharType(),null);

    private static class Constants
    {
        private static final int UUID_STRING_MAX_LENGTH=36;
        private static final int IP_ADDRESS_STRING_MAX_LENGTH=45;

    }
    private final Type nativeType;
    private final Class<?> javaType;


    AerospikeType(Type nativeType, Class<?> javaType)
    {
        this.nativeType= requireNonNull(nativeType,"nativeType is null") ;
        this.javaType=javaType;
    }
    public Type getNativeType(){
        return nativeType;
    }
    public int getTypeArgumentSize(){
        switch (this){
            case LIST :
                return 1;
            case MAP:
                return 2;
            default:
                return 0;

        }
    }
    @Override
    public  AerospikeType getAerospikeType() {
       if(getTypeArgumentSize()==0){
                      return this;
       }
       else{
           //we should not dcall for types with typearguements
           throw  new IllegalStateException();
       }
    }

    @Override
    public List<AerospikeType> getTypeArguments() {
        if(getTypeArgumentSize()==0){
            return null;
        }
        else{
            throw  new IllegalStateException();
        }
    }
}
