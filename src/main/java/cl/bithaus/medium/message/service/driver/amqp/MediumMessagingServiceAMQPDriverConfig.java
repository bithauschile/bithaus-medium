/*
 * Copyright (c) BitHaus Software Chile
 * All rights reserved. www.bithaus.cl.
 * 
 * All rights to this product are owned by Bithaus Chile and may only by used 
 * under the terms of its associated license document. 
 * You may NOT copy, modify, sublicense or distribute this source file or 
 * portions of it unless previously authorized by writing by Bithaus Software Chile.
 * In any event, this notice must always be included verbatim with this file.
 */
package cl.bithaus.medium.message.service.driver.amqp;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

/**
 * Kakfa Driver configuration file
 * @author jmakuc
 */
public class MediumMessagingServiceAMQPDriverConfig extends AbstractConfig {
    
    public static final String SERVER_URI_CONFIG = "amqp.server.uri";
    public static final String SERVER_URI_DOC = "Connection string to the AMQP server (ie: ampqs://host:port)";
    public static final String SERVER_USERNAME_CONFIG = "amqp.server.username";
    public static final String SERVER_USERNAME_DOC = "Username to connect to the AMQP server";
    public static final String SERVER_PASSWORD_CONFIG = "amqp.server.password";
    public static final String SERVER_PASSWORD_DOC = "Password to connect to the AMQP server";
    
    
    public static final String PRODUCER_ENABLED_CONFIG = "amqp.producer.enabled";
    public static final String PRODUCER_ENABLED_DOC = "Enables the producer client";
    public static final String PRODUCER_QUEUE_CONFIG = "amqp.producer.queue";
    public static final String PRODUCER_QUEUE_DOC = "Queue name to publibish messages to";
    
    public static final String CONSUMER_ENABLED_CONFIG = "amqp.consumer.enabled";
    public static final String CONSUMER_ENABLED_DOC = "Enables the consumer client";
    public static final String CONSUMER_QUEUE_CONFIG = "amqp.consumer.queue";
    public static final String CONSUMER_QUEUE_DOC = "Queue name to consume from";    


    public MediumMessagingServiceAMQPDriverConfig(Map<String,String> originals) {
        super(conf(), originals, true);
    }
    
    public static ConfigDef conf() {
        
        return new ConfigDef()

                .define(SERVER_URI_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, SERVER_URI_DOC)
                .define(SERVER_USERNAME_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, SERVER_USERNAME_DOC)
                .define(SERVER_PASSWORD_CONFIG, ConfigDef.Type.PASSWORD, null, ConfigDef.Importance.HIGH, SERVER_PASSWORD_DOC)

                .define(PRODUCER_ENABLED_CONFIG, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.HIGH, PRODUCER_ENABLED_DOC)
                .define(PRODUCER_QUEUE_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, PRODUCER_QUEUE_DOC)

                .define(CONSUMER_ENABLED_CONFIG, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.HIGH, CONSUMER_ENABLED_DOC)
                .define(CONSUMER_QUEUE_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONSUMER_QUEUE_DOC)
                ;
         
    }

    public String getServerURI() {
        return this.getString(SERVER_URI_CONFIG);
    }

    public String getServerUsername() {
        return this.getString(SERVER_USERNAME_CONFIG);
    }

    public String getServerPassword() {
        return this.getPassword(SERVER_PASSWORD_CONFIG).value();
    }
    
    public Boolean isProducerEnabled() {
        return this.getBoolean(PRODUCER_ENABLED_CONFIG);
    }
    
    public String getProducerQueue() {
        return this.getString(PRODUCER_QUEUE_CONFIG);
    }
    
    public Boolean isConsumerEnabled() {
        return this.getBoolean(CONSUMER_ENABLED_CONFIG);
    }

    public String getConsumerQueue() {
        return this.getString(CONSUMER_QUEUE_CONFIG);
    }

    
}
