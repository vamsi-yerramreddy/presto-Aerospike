package com.facebook.presto.aerospike;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.json.JsonModule;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.inject.Injector;
import com.sun.org.slf4j.internal.Logger;
import com.sun.org.slf4j.internal.LoggerFactory;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

/**
 *
 * Factory class for creating instances of AerospikeConnector.
 * This class is responsible fr setting up and configuring the AerospikeConnector using dependency injection.
 * implements ConnectorFactory from prestoSPi
 */
public class AerospikeConnectorFactory implements ConnectorFactory {
    private final String name;
    private static final Logger log = LoggerFactory.getLogger(AerospikeConnectorFactory.class); 

    /**
     * Constructor for AerospikeConnectorFactory.
     *
     * @param name The name of the connector, cannot be null or empty.
     */
    
    public AerospikeConnectorFactory(String name){

        checkArgument(!isNullOrEmpty(name),"name is null or empty");
        this.name=name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public ConnectorHandleResolver getHandleResolver() {
        return new AerospikeHandleResolver();
    }
    /**
     * Creates a new instance of the Aerospike connector.
     *
     * @param catalogName    The name of the catalog for the connector.
     * @param requiredConfig The configuration properties required to set up the connector.
     * @param context        The context in which the connector is created.
     * @return A new instance of AerospikeConnector.
     */

    @Override
    public Connector create(String catalogName, Map<String, String> requiredConfig, ConnectorContext context) {
        requireNonNull(requiredConfig,"Config required in null");
        try{
            /*
         Initializes the dependency injection framework using Guice.
         It sets up necessary modules, JsonModule and a custom AerospikeModule.    * */
            /*info is nt thre need yo chk */

            log.debug("Creating Aerospike connector for catalog: {}", catalogName);
            Bootstrap app = new Bootstrap(
                    new JsonModule(),
                    new AerospikeModule(catalogName,context.getTypeManager())
            ) ;
            // Initialize the Guice injector with the required configuration properties

            Injector injector = app.doNotInitializeLogging().setRequiredConfigurationProperties(requiredConfig).initialize();

            return injector.getInstance(AerospikeConnector.class);
        } catch(Exception e)
        {
            throwIfUnchecked(e);
            throw  new RuntimeException(e);
        }

    }
}
