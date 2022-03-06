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
package cl.bithaus.medium.message.service.driver.kafka;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

/**
 * Kakfa Driver configuration file
 * @author jmakuc
 */
public class MediumMessagingServiceKafkaDriverConfig extends AbstractConfig {
    
    public static final String PRODUCER_ENABLED_CONFIG = "kafka.producer.enabled";
    public static final String PRODUCER_ENABLED_DOC = "Enables the drivers Kafka producer";
    public static final String PRODUCER_BOOTSTRAP_SERVERS_CONFIG = "kafka.producer.bootstrapServer";
    public static final String PRODUCER_BOOTSTRAP_SERVERS_DOC = "Bootstrap server list used by the Kafka Producer (comma separated)";
    public static final String PRODUCER_CLIENTID_CONFIG = "kafka.producer.clientId";
    public static final String PRODUCER_CLIENTID_DOC = "KAfka producer client ID.";
    public static final String PRODUCER_CONFIG_FILE_CONFIG = "kafka.producer.configFile";
    public static final String PRODUCER_CONFIG_FILE_DOC = "Kafka producer configuration file. If setted, the contents will override other setting. (optional)";
    
    public static final String CONSUMER_ENABLED_CONFIG = "kafka.consumer.enabled";
    public static final String CONSUMER_ENABLED_DOC = "Enables the drivers Kafka consumer";
    public static final String CONSUMER_BOOTSTRAP_SERVERS_CONFIG = "kafka.consumer.bootstrapServer";
    public static final String CONSUMER_BOOTSTRAP_SERVERS_DOC = "Bootstrap server list used by the Kafka consumer (comma separated)";
    public static final String CONSUMER_GROUPID_CONFIG = "kafka.consumer.groupId";
    public static final String CONSUMER_GROUPID_DOC = "Group ID that this consumer belongs to.";
    public static final String CONSUMER_SUBSCRIPTIONTOPICS_CONFIG = "kafka.consumer.subcriptionTopics";
    public static final String CONSUMER_SUBSCRIPTIONTOPICS_DOC = "Topic list to subscribe to (comma separated).";
    public static final String CONSUMER_CONFIG_FILE_CONFIG = "kafka.consumer.configFile";
    public static final String CONSUMER_CONFIG_FILE_DOC = "Kafka consumer configuration file. If setted, the contents will override other setting. (optional)";    
    
    public static final String TESTING_MODE_ENABLED = "driver.testingModeEnabled";

    public MediumMessagingServiceKafkaDriverConfig(Map<String,String> originals) {
        super(conf(), originals, true);
    }
    
    public static ConfigDef conf() {
        
        ConfigDef config =  new ConfigDef()
                
                .define(TESTING_MODE_ENABLED, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.LOW, "")
                
                .define(PRODUCER_ENABLED_CONFIG, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.MEDIUM, PRODUCER_ENABLED_DOC)
                .define(PRODUCER_BOOTSTRAP_SERVERS_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, PRODUCER_BOOTSTRAP_SERVERS_DOC)
                .define(PRODUCER_CLIENTID_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, PRODUCER_CLIENTID_DOC)
                .define(PRODUCER_CONFIG_FILE_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, PRODUCER_CONFIG_FILE_DOC)
                
                .define(CONSUMER_ENABLED_CONFIG, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.MEDIUM, CONSUMER_ENABLED_DOC)
                .define(CONSUMER_BOOTSTRAP_SERVERS_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONSUMER_BOOTSTRAP_SERVERS_DOC)
                .define(CONSUMER_GROUPID_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONSUMER_GROUPID_DOC)
                .define(CONSUMER_SUBSCRIPTIONTOPICS_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONSUMER_SUBSCRIPTIONTOPICS_DOC)
                .define(CONSUMER_CONFIG_FILE_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONSUMER_CONFIG_FILE_DOC)                
                
                ;
        
        return config;
    }
    
    public Boolean isProducerEnabled() {
        return this.getBoolean(PRODUCER_ENABLED_CONFIG);
    }
    
    public String getProducerBootstrapServers() {
        return this.getString(PRODUCER_BOOTSTRAP_SERVERS_CONFIG);
    }
    
    public String getProducerClientId() {
        return this.getString(PRODUCER_CLIENTID_CONFIG);
    }
    
    public String getProducerConfigFile() {
        return this.getString(PRODUCER_CONFIG_FILE_CONFIG);
    }
    
    public Boolean isConsumerEnabled() {
        return this.getBoolean(CONSUMER_ENABLED_CONFIG);
    }
    
    public String getConsumerBootstrapServers() {
        return this.getString(CONSUMER_BOOTSTRAP_SERVERS_CONFIG);
    }
    
    public String getConsumerGroupId() {
        return this.getString(CONSUMER_GROUPID_CONFIG);
    }
    
    public String getConsumerConfigFile() {
        return this.getString(CONSUMER_CONFIG_FILE_CONFIG);
    }
    
    public String[] getConsumerSubscriptionTopics() {
        String value = this.getString(CONSUMER_SUBSCRIPTIONTOPICS_CONFIG);
        
        if(value == null)
            return null;
        
        return value.split(",");        
    }
    
    public Boolean isTestingModeEnabled() {
        
        Boolean value = this.getBoolean(TESTING_MODE_ENABLED);
        
        if(value == null)
            return false;
        
        return value;
    }
    
}
