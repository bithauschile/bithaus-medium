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
package cl.bithaus.medium.message.service;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

/**
 *
 * @author jmakuc
 */
public class MediumMessagingServiceConfig extends AbstractConfig {

    public static final String DRIVER_CLASSNAME_CONFIG = "medium.message.service.driver.className";
    public static final String DRIVER_CLASSNAME_DOC = "Medium MessagingService Driver Class Name";

    public static final String DRIVER_CONFIGFILE_CONFIG = "medium.message.service.driver.configFile";
    public static final String DRIVER_CONFIGFILE_DOC = "Medium messaging sercvice driver configuration file path";        
    
    public static final String LOGGER_SUFIX_CONFIG = "medium.message.service.loggerSuffix";
    public static final String LOGGER_SUFIX_DOC = "Medium messaging service logger suffix";

    public static final String DEFAULT_PRODUCER_TOPIC_CONFIG = "mmedium.message.service.defaultProducerTopic";
    public static final String DEFAULT_PRODUCER_TOPIC_DOC = "Medium messaging service default producer topic";

    public MediumMessagingServiceConfig(Map originals) {
        super(conf(), originals);
    }

    public static ConfigDef conf() {

        return new ConfigDef()
                .define(DRIVER_CLASSNAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, DRIVER_CLASSNAME_DOC)
                .define(DRIVER_CONFIGFILE_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, DRIVER_CONFIGFILE_DOC)
                .define(LOGGER_SUFIX_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, LOGGER_SUFIX_DOC)
                .define(DEFAULT_PRODUCER_TOPIC_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, DEFAULT_PRODUCER_TOPIC_DOC)                
                ;                
    }      

    public String getDriverClassName() {

        return this.getString(DRIVER_CLASSNAME_CONFIG);
    }

    public String getDriverConfigFile() {

        return this.getString(DRIVER_CONFIGFILE_CONFIG);
    }
    
    public String getLoggerSuffix() {
        
        return this.getString(LOGGER_SUFIX_CONFIG);
    }

    public String getDefaultProducerTopic() {
        
        return this.getString(DEFAULT_PRODUCER_TOPIC_CONFIG);
    } 
}
