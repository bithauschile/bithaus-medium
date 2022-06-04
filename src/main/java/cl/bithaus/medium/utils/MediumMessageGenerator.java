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
package cl.bithaus.medium.utils;

import cl.bithaus.medium.message.MediumMessage;
import cl.bithaus.medium.message.service.MediumMessagingService;
import cl.bithaus.medium.message.service.MediumMessagingServiceConfig;
import cl.bithaus.medium.utils.test.TestMessageGenerator;
import com.google.gson.Gson;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author jmakuc
 */
public abstract class MediumMessageGenerator<M extends MediumMessage> {
    
    protected final static Logger logger = LoggerFactory.getLogger(MediumMessageGenerator.class);
    private final static Gson gson = new Gson();

    
    
    
    public static void main(String[] args) throws Exception {
        
        if(args.length < 1 || args[0] == null)
            showUsage();
        
        String filename = args[0];
        
        logger.info("Reading configurations from " + filename);
        
        Map originals = MapUtils.loadPropertiesFile(filename);
        Config config = new Config(originals);
        
        logger.info("Loading messaging service configuration from " + config.getMessagingServiceConfig());
        Map msgSvcConfigMap = MapUtils.loadPropertiesFile(config.getMessagingServiceConfig());
        MediumMessagingServiceConfig msgSvcConfig = new MediumMessagingServiceConfig(msgSvcConfigMap);
        MediumMessagingService service = new MediumMessagingService(msgSvcConfig);
        
        service.start();
        logger.info("Messaging service started OK");
        
        logger.info("Instancing generator " + config.getGeneratorClassName());
        MediumMessageGenerator generator = 
                (MediumMessageGenerator) Class.forName(config.getGeneratorClassName()).getConstructor().newInstance();

        if(config.getGeneratorConfigFile() != null && !config.getGeneratorConfigFile().isEmpty()){
         
            logger.info("Loading generator config from " + config.getGeneratorConfigFile());
            Map generatorConfigMap = MapUtils.loadPropertiesFile(config.getGeneratorConfigFile());
            generator.init(generatorConfigMap);
        }
        
        
        
        String topic = config.getTopic();
        
        Date from = getTimestamp(config.getDateFrom());
        Date to = getTimestamp(config.getDateTo());
        int quantity = config.getQuantity();
        Integer keys = config.getKeys();
        
        Collection<MediumMessage> messages = generator.getMessages(from, to, quantity, keys);

        logger.info("Sending messages via " + topic);
        
        try {
            for(var m : messages) {
                
                if(logger.isTraceEnabled())
                    logger.trace("OUT: " + gson.toJson(m));
                service.send(m, topic);
            }
        }
        catch(Exception e) {
            throw new Exception("Error sending message: " + e, e);
        }
        
    }

    private static Date getTimestamp(String dateTimeStr) {
        
        return new Date(LocalDateTime.parse(dateTimeStr).toEpochSecond(ZoneOffset.UTC)*1000);
    }







    
    // instance variables
    // ------------------------------------------------------------------
    
    public MediumMessageGenerator() {
    }
    
    public abstract void init(Map<String,String> config);
    
    protected abstract M getNextMessage();
    
    
    
    
    public Collection<M> getMessages(Date from, Date to, int quantity, Integer keys) {
        
        if(from == null)
            throw new IllegalArgumentException("from time cannot be null");
        
        if(to == null)
            throw new IllegalArgumentException("to time cannot be null");
        
        if(from.getTime() > to.getTime())
            throw new IllegalArgumentException("from-time must be lower than to-time");
        
        if(quantity < 1)
            throw new IllegalArgumentException("Quantity must be greater than 0");
        
        
        long totalTime = to.getTime() - from.getTime();
        
        long delta = (long) totalTime / quantity;
        
        logger.info("Generating " + quantity + " messages from " + from + " to " + to + ", delta=" + delta);
        
        String[] keysArray = null;
        
        if(keys != null && keys > 0) {
            
            keysArray = new String[keys];
            for(int i = 0; i < keys; i++)
                keysArray[i] = "key-" + i;
        }
        
        Collection<M> messages = new ArrayList<>();
        
        for(int i = 0; i < quantity; i++) {
            
            long timestamp = from.getTime() + delta * i;
            // control entries don't know milliseconds
            timestamp = timestamp - (timestamp % 1000); 
            
            M m = this.getNextMessage();
            m.getMetadata().setTimestamp(timestamp);
            
            if(m.getMetadata().getKey() == null) {
            
                if(keysArray != null)
                    m.getMetadata().setKey(keysArray[i%keysArray.length]);
            }
            
            messages.add(m);
            
        }
        
        return messages;
        
    }

    
    private static void showUsage() {
        logger.error("Parameters: CONFIG_FILE");
        System.exit(1);   
    }
    
    public static class Config extends AbstractConfig {
        
        public static final String TOPIC_CONFIG = "medium.msgGen.topic";
        public static final String TOPIC_DOC = "Topic to write to";

        public static final String MESSAGING_SERVICE_CONFIG = "medium.msgGen.msgSvcConfig";
        public static final String MESSAGING_SERVICE_DOC = "Messaging service configuration file";    
        
        public static final String DATE_FROM_CONFIG = "medium.msgGen.dateFrom";
        public static final String DATE_FROM_DOC = "Date and time where to start generating messages (yyyy-MM-dd HH:mm:ss)";
        
        public static final String DATE_TO_CONFIG = "medium.msgGen.dateTo";
        public static final String DATE_TO_DOC = "Date and time where to stop generating messages (yyyy-MM-dd HH:mm:ss)";
        
        public static final String QUANTITY_CONFIG = "medium.msgGen.quantity";
        public static final String QUANTITY_DOC = "How many messages to generate (Default: 100)";
        
        public static final String KEYS_CONFIG = "medium.msgGen.keys";
        public static final String KEYS_DOC = "How many different keys to generate. 0 or null for none, given key will remain.";
        
        public static final String GENERATOR_CLASSNAME_CONFIG = "medium.msgGen.generator.classname";
        public static final String GENERATOR_CLASSNAME_DOC = "Classname of the generator class to use (Default: cl.bithaus.medium.utils.test.TestMessageGenerator)";
        
        public static final String GENERATOR_CONFIGFILE_CONFIG = "medium.msgGen.generator.configFile";
        public static final String GENERATOR_CONFIGFILE_DOC = "Message generation configuration file";
        
        public Config(Map originals) {
            super(conf(), originals);
        }
        
        public static ConfigDef conf() {
            
            return new ConfigDef()
                    .define(TOPIC_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, TOPIC_DOC)
                    .define(MESSAGING_SERVICE_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, MESSAGING_SERVICE_DOC)
                    .define(DATE_FROM_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, DATE_FROM_DOC)
                    .define(DATE_TO_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, DATE_TO_DOC)
                    .define(QUANTITY_CONFIG, ConfigDef.Type.INT, 100, ConfigDef.Importance.HIGH, QUANTITY_DOC)
                    .define(KEYS_CONFIG, ConfigDef.Type.INT, null, ConfigDef.Importance.LOW, KEYS_DOC)
                    .define(GENERATOR_CLASSNAME_CONFIG, ConfigDef.Type.STRING, TestMessageGenerator.class.getName(), ConfigDef.Importance.LOW, GENERATOR_CLASSNAME_DOC)
                    .define(GENERATOR_CONFIGFILE_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, TOPIC_DOC)
                    ;
        }
        
        public String getTopic() {
            return this.getString(TOPIC_CONFIG);
        }
        
        public String getMessagingServiceConfig(){
            return this.getString(MESSAGING_SERVICE_CONFIG);
        }
        
        public String getDateFrom() {
            return this.getString(DATE_FROM_CONFIG);
        }
        
        public String getDateTo() {
            return this.getString(DATE_TO_CONFIG);
        }
        
        public Integer getQuantity() {
            return this.getInt(QUANTITY_CONFIG);
        }
        
        public Integer getKeys() {
            return this.getInt(KEYS_CONFIG);
        }
        
        public String getGeneratorClassName() {
            return this.getString(GENERATOR_CLASSNAME_CONFIG);
        }
        
        public String getGeneratorConfigFile() {
            return this.getString(GENERATOR_CONFIGFILE_CONFIG);
        }
        
        
    }
}
