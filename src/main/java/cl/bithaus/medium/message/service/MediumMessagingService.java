/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cl.bithaus.medium.message.service;

import cl.bithaus.medium.message.MediumMessage;
import cl.bithaus.medium.message.MediumMessage.Metadata;
import cl.bithaus.medium.message.exception.MediumMessagingServiceException;
import cl.bithaus.medium.message.exception.SendToDeadLetterException;
import cl.bithaus.medium.record.MediumConsumerRecord;
import cl.bithaus.medium.message.service.driver.MediumMessagingServiceNetworkDriver;
import cl.bithaus.medium.record.MediumProducerRecord;
import cl.bithaus.medium.utils.MapUtils;
import com.google.gson.Gson;
import java.io.FileNotFoundException;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Medium Messagin Service
 * Abstraction layer to the underlying transport system.
 * @author jmakuc
 */
public class MediumMessagingService {
                      
    private final Logger logger;
    
    private Gson gson;
    
    // Driver
    private MediumMessagingServiceNetworkDriver driver;
    private MediumMessagingServiceConfig serviceConfig;
    
    // Handlers
    private MediumMessagingServiceRawHandler rawHandler;
    private boolean running = false;
    private String defaultProducerTopic;
    private Consumer<MediumConsumerRecord> sendToDeadletterCallback;
    
    
    public MediumMessagingService(MediumMessagingServiceConfig config) throws MediumMessagingServiceException {
        
        if(config == null)
            throw new NullPointerException("Service configuration cannot be null");
     
        String loggerSufix = config.getLoggerSuffix()!=null?config.getLoggerSuffix():"";
        
        this.serviceConfig = config;
        this.logger = LoggerFactory.getLogger(this.getClass().getName() + loggerSufix);
                
        this.rawHandler = new MediumMessagingServiceDefaultRawHandler();
        
        if(config.getDefaultProducerTopic() != null && config.getDefaultProducerTopic().length() > 0) {
            
            this.defaultProducerTopic = config.getDefaultProducerTopic();
            logger.info("Default output topic: " + this.defaultProducerTopic);
        }
        else {
            
            logger.info("Default output topic: <NONE>");
        }
        
        initDriver();
        
        this.gson = new Gson();
    }
    
    private void initDriver() throws MediumMessagingServiceException {
        
        try {
             
            if(this.serviceConfig.getDriverClassName() == null || this.serviceConfig.getDriverClassName().length() < 1)
                throw new IllegalArgumentException("Driver classname not found");
            
            
            logger.info(String.format("Network driver %s, config: %s", this.serviceConfig.getDriverClassName(), this.serviceConfig.getDriverConfigFile()));

            Map driverConfigMap = null;            
            if(this.serviceConfig.getDriverConfigFile() != null && this.serviceConfig.getDriverConfigFile().length() > 0)
                driverConfigMap = MapUtils.loadPropertiesFile(this.serviceConfig.getDriverConfigFile());
            
            this.driver = (MediumMessagingServiceNetworkDriver) Class.forName(this.serviceConfig.getDriverClassName()).getConstructor().newInstance();
            
            driver.init(driverConfigMap, (var record) -> {
                try {
                    rawHandler.onData(record);
                }
                catch(SendToDeadLetterException e) {
                    
                    sendToDeadLetter(record);
                }
            });
            
            logger.info("Driver init OK!");
            
        }
        catch(FileNotFoundException e) {
            
            throw new MediumMessagingServiceException("Driver config file not found: " + this.serviceConfig.getDriverConfigFile(), e);
        }
        catch(InstantiationException e) {
            
            throw new MediumMessagingServiceException("Invalid driver " + this.serviceConfig.getDriverClassName(), e);
        }
        catch(ClassNotFoundException e) {
            
            throw new MediumMessagingServiceException("Driver class not found", e);
        }
        catch(InvocationTargetException e) {
            
            throw new MediumMessagingServiceException("Error instantiating driver", e);
        }
        catch(Throwable t) {
            
            throw new MediumMessagingServiceException("Error intializing driver", t);
        }
            
    }
    
    private void sendToDeadLetter(MediumConsumerRecord record) {
        
        if(sendToDeadletterCallback != null) {

            logger.warn("Sending record to dead letter from topic " + record.toString() + " VALUE: " + record.getValue());
            sendToDeadletterCallback.accept(record);
        }
        else {

            logger.warn("No deadletter callback, discarting record: " + gson.toJson(record));
        }        
    }
    
    public void start() throws MediumMessagingServiceException {
        
        if(running)
          throw new IllegalStateException("Service is already running");

        logger.info("BcsMessagingService Start...");

        if (this.driver == null)
          throw new IllegalStateException("Network driver has not been properly initialized");
        
        driver.start();
        running = true;        
    }
    
    public void stop() throws MediumMessagingServiceException {
        
        running = false;
        driver.stop();                
    }
    
    public void send(MediumMessage message, String topic) throws MediumMessagingServiceException {
        
        try {
            
            if(!running)
                throw new IllegalStateException("Messaging Service is not running");
            
            if(message == null)
                throw new IllegalArgumentException("Message cannot be null");
            
            String producerTopic = topic;
            
            if(producerTopic == null) {
                
                if(message.getMetadata() != null)
                    producerTopic = message.getMetadata().getTxTopic();
                    
                if(producerTopic == null) {
                    producerTopic = this.defaultProducerTopic;
                }
                
            }  
            
            
            
            if(producerTopic == null)
                throw new IllegalArgumentException("Topic cannot be null when is not default producer topic configured");
            
            
            String serializedData = gson.toJson(message);
            
            if(logger.isDebugEnabled()) {
                
                if(logger.isTraceEnabled()) {
                    
                    logger.debug(String.format("Sending message [Ch:%s]: (%s) %s", topic, message.getClass().getName(), serializedData));
                }
                else {
                    
                    logger.debug(String.format("Sending message [Ch:%s]: %s", topic, message.getClass().getName()));
                }
            }
            
            /**
             * Some fields are overwriten at this point to reflect the real 
             * values used in the transmition.
             */
            Metadata metadata = message.getMetadata();
            metadata.setTxTopic(producerTopic);
            
            Map<String, String> headers = metadata.getHeaders();
            headers.put(MediumMessage.HEADER_MESSAGE_CLASS, message.getClass().getName());
            headers.put(MediumMessage.HEADER_MESSAGE_SOURCE, metadata.getSource());
            headers.put(MediumMessage.HEADER_MESSAGE_TARGET, metadata.getTarget());
            
            String key = metadata.getKey();
            String value = this.gson.toJson(message);
            
            long timestamp = metadata.getTimestamp();
            
            
            MediumProducerRecord record = 
                    new MediumProducerRecord(key, value, topic, headers, timestamp);
             
            driver.send(record);
            
        }
        catch(Throwable e) {
            
            throw new MediumMessagingServiceException("Error sending message", e);
        }
        
    }
    
//    public void sendRaw(String channel, String rawMessage) throws MediumMessagingServiceException {
//        
//        try {
//            
//            if(!running)
//                throw new IllegalStateException("Messaging Service is not running");
//            
//            if(channel == null)
//                throw new IllegalArgumentException("Channel cannot be null");
//            
//            if(rawMessage == null || rawMessage.length() < 1)
//                throw new IllegalArgumentException("Message cannot be null nor empty");
//            
//            
//            if(logger.isDebugEnabled()) {
//                                
//                logger.debug(String.format("Sending message [Ch:%s]: %s", channel, rawMessage));                
//            }
//                        
//            driver.send(channel, rawMessage);
//        }
//        catch(Throwable e) {
//            
//            throw new MediumMessagingServiceException("Error sending message", e);
//        }
//        
//    }
    
    public <M extends MediumMessage> void addMessageListener(Class<M> messageType, MediumMessageListener<? super M> handler) {
    
        this.rawHandler.addMessageListener(messageType, handler);
    }
     
    public <M extends MediumMessage> boolean removeMessageListener(Class<M> messageType, MediumMessageListener<? super M> handler) {
        
        return this.rawHandler.removeMessageListener(messageType, handler);
    }
    
    public void setRawHandler(MediumMessagingServiceRawHandler rawHandler) {
        
        logger.info("Replacing raw handler with: " + rawHandler);
        this.rawHandler = rawHandler;
    }
    
    public MediumMessagingServiceRawHandler getRawHandler() {
        
        return this.rawHandler;
    }
    
    public MediumMessagingServiceNetworkDriver getDriver() {
        
        return this.driver;
    }

    public Gson getGson() {
        return gson;
    }

    public void setGson(Gson gson) {
        
        logger.info("Replacing gson instance with " + gson);
        this.gson = gson;
    }

    public Consumer<MediumConsumerRecord> getSendToDeadletterCallback() {
        return sendToDeadletterCallback;
    }

    public void setSendToDeadletterCallback(Consumer<MediumConsumerRecord> sendToDeadletterCallback) {
        this.sendToDeadletterCallback = sendToDeadletterCallback;
    }
    
    
    
}
