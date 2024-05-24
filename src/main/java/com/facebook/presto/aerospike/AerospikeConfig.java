package com.facebook.presto.aerospike;
import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;

import javax.validation.constraints.NotNull;

/*
* Necessary config parameters fo connecting to Aerospike cluster.
* */
public class AerospikeConfig {

    private String host;
    private int port;
    private String namespace;
    private String username;
    private String password;
    private String metadata;


    @NotNull
    public String getHost() {
        return host;
    }

    @Config("aerospike.host")
    @ConfigDescription("Aerospike cluster host address")
    public AerospikeConfig setHost(String host) {
        this.host = host;
        return this;
    }

    @NotNull
    public int getPort() {
        return port;
    }


    @Config("aerospike.port")
    @ConfigDescription("Aerospike cluster port")
    public AerospikeConfig setPort(int port) {
        this.port = port;
        return this;
    }

    public String getMetadata() {
        return metadata;
    }

    public void setMetadata(String metadata) {
        this.metadata = metadata;
    }

    @NotNull
    public String getNamespace() {
        return namespace;
    }

    @Config("aerospike.namespace")
    @ConfigDescription("Aerospike namespace")
    public AerospikeConfig setNamespace(String namespace) {
        this.namespace = namespace;
        return this;
    }

    public String getUsername() {
        return username;
    }

    @Config("aerospike.username")
    @ConfigDescription("Aerospike username")
    public AerospikeConfig setUsername(String username) {
        this.username = username;
        return this;
    }

    public String getPassword() {
        return password;
    }

    @Config("aerospike.password")
    @ConfigDescription("Aerospike password")
    public AerospikeConfig setPassword(String password) {
        this.password = password;
        return this;
    }
}
