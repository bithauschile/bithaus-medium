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
package cl.bithaus.medium.message.service.driver.azure;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.types.Password;

/**
 * Kakfa Driver configuration file
 * @author jmakuc
 */
public class MediumMessagingServiceAzureServiceBusDriverConfig extends AbstractConfig {

    public static final String CONNECTIONSTRING_CONFIG = "azure.servicebus.connectionString";
    public static final String CONNECTIONSTRING_DOC = "Connection string for service bus client";    
    public static final String CREDENTIAL_KEY_NAME_CONFIG = "azure.servicebus.credential.keyName";
    public static final String CREDENTIAL_KEY_NAME_DOC = "Shared access key name";
    public static final String CREDENTIAL_KEY_CONFIG = "azure.servicebus.credential.key";
    public static final String CREDENTIAL_KEY_DOC = "Shared access key";
    
    public static final String SENDER_ENABLED_CONFIG = "azure.servicebus.sender.enabled";
    public static final String SENDER_ENABLED_DOC = "Enables the sender client";
    public static final String SENDER_TOPIC_CONFIG = "azure.servicebus.sender.topic";
    public static final String SENDER_TOPIC_DOC = "Topic to publish messages to";
    public static final String SENDER_QUEUE_CONFIG = "azure.servicebus.sender.queue";
    public static final String SENDER_QUEUE_DOC = "QUeue to publibish messages to";
    
    public static final String PROCESSOR_ENABLED_CONFIG = "azure.servicebus.processor.enabled";
    public static final String PROCESSOR_ENABLED_DOC = "Enables the processor client (consumer)";
    public static final String PROCESSOR_TOPIC_CONFIG = "azure.servicebus.processor.topic";
    public static final String PROCESSOR_TOPIC_DOC = "Topic name to subscribe to";
    public static final String PROCESSOR_QUEUE_CONFIG = "azure.servicebus.processor.queue";
    public static final String PROCESSOR_QUEUE_DOC = "Queue name to consume from";

    

        
     

    public MediumMessagingServiceAzureServiceBusDriverConfig(Map<String,String> originals) {
        super(conf(), originals, true);
    }
    
    public static ConfigDef conf() {
        
        ConfigDef config =  new ConfigDef()
                                
                .define(CONNECTIONSTRING_CONFIG, ConfigDef.Type.PASSWORD, null, ConfigDef.Importance.LOW, CONNECTIONSTRING_DOC)
                .define(CREDENTIAL_KEY_NAME_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CREDENTIAL_KEY_NAME_DOC)
                .define(CREDENTIAL_KEY_CONFIG, ConfigDef.Type.PASSWORD, null, ConfigDef.Importance.LOW, CREDENTIAL_KEY_DOC)                
                
                .define(SENDER_ENABLED_CONFIG, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.MEDIUM, SENDER_ENABLED_DOC)
                .define(SENDER_TOPIC_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, SENDER_TOPIC_DOC)
                .define(SENDER_QUEUE_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, SENDER_QUEUE_DOC)

                .define(PROCESSOR_ENABLED_CONFIG, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.MEDIUM, PROCESSOR_ENABLED_DOC)
                .define(PROCESSOR_TOPIC_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, PROCESSOR_TOPIC_DOC)
                .define(PROCESSOR_QUEUE_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, PROCESSOR_QUEUE_DOC)
                
                ;
        
        return config;
    }

    
    public String getConnectionString() {
        return this.getPassword(CONNECTIONSTRING_CONFIG).value();
    }
    
    public String getCredentialKeyName() {
        return this.getString(CREDENTIAL_KEY_NAME_CONFIG);
    }
    
    public String getCredentialKey() {
        return this.getPassword(CREDENTIAL_KEY_CONFIG).value();
    }

    public Boolean isSenderEnabled() {        
        return this.getBoolean(SENDER_ENABLED_CONFIG);
    }
        
    public String getSenderTopic() {
        return this.getString(SENDER_TOPIC_CONFIG);
    }
    
    public String getSenderQueue() {
        return this.getString(SENDER_QUEUE_CONFIG);
    }

    public Boolean isProcessorEnabled() {
        return this.getBoolean(PROCESSOR_ENABLED_CONFIG);
    }
    
    public String getProcessorTopic() {
        return this.getString(PROCESSOR_TOPIC_CONFIG);
    }
    
    public String getProcessorQueue() {
        return this.getString(PROCESSOR_QUEUE_CONFIG);
    }
   
}
