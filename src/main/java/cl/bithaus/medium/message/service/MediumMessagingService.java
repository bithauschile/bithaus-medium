/*
 * Copyright (c) BitHaus Software Factory & Boutique (Chile)
 * All rights reserved. www.bithaus.cl.
 * 
 * All rights to this product are owned by BitHaus Chile and may only by used 
 * under the terms of its associated license document. 
 * You may NOT copy, modify, sublicense or distribute this source file or 
 * portions of it unless previously authorized by writing by BitHaus Chile.
 * In any event, this notice must always be included verbatim with this file.
 * 
 */
package cl.bithaus.medium.message.service;

import java.io.FileNotFoundException;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import cl.bithaus.medium.message.MediumMessage;
import cl.bithaus.medium.message.MediumMessage.Metadata;
import cl.bithaus.medium.message.exception.MediumMessagingServiceException;
import cl.bithaus.medium.message.exception.SendToDeadLetterException;
import cl.bithaus.medium.message.service.driver.MediumMessagingServiceNetworkDriver;
import cl.bithaus.medium.record.MediumConsumerRecord;
import cl.bithaus.medium.record.MediumProducerRecord;
import cl.bithaus.medium.utils.MapUtils;
import cl.bithaus.medium.utils.MessageUtils;
import io.opentelemetry.instrumentation.annotations.WithSpan;

/**
 * Medium Messaging Service
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
        
        this.gson = MessageUtils.getMediumGson();
    }
    
    private void initDriver() throws MediumMessagingServiceException {
        
        try {
             
            if(this.serviceConfig.getDriverClassName() == null || this.serviceConfig.getDriverClassName().length() < 1)
                throw new IllegalArgumentException("Driver classname not found");
            
            
            logger.info(String.format("Network driver %s, config: %s", this.serviceConfig.getDriverClassName(), this.serviceConfig.getDriverConfigFile()));

            Map driverConfigMap = null;            
            if(this.serviceConfig.getDriverConfigFile() != null && this.serviceConfig.getDriverConfigFile().length() > 0)
                driverConfigMap = MapUtils.loadPropertiesFile(this.serviceConfig.getDriverConfigFile());
            else if(this.serviceConfig.getDriverConfigMap() != null)
                driverConfigMap = this.serviceConfig.getDriverConfigMap();
            
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

    /**
     * Synchroneous message send to the default topic
     * @param message
     * @throws MediumMessagingServiceException
     */
    public void send(MediumMessage message) throws MediumMessagingServiceException {

        send(message, null);
    }
    
    /**
     * Synchroneous message send to the specified topic
     * @param message
     * @param topic
     * @throws MediumMessagingServiceException
     */
    public void send(MediumMessage message, String topic) throws MediumMessagingServiceException {
        
        send(message, topic, false);
    }

    /**
     * Message sending through the underlying transport system
     * @param message Message to send
     * @param topic Topic to send the message to (null to use default topic)
     * @param async If true, the message will be sent asynchronously
     */
    @WithSpan
    public void send(MediumMessage message, String topic, boolean async) throws MediumMessagingServiceException {

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
                throw new IllegalArgumentException("Topic cannot be null when is no default producer topic configured");
            
            
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
            
            Long timestamp = metadata.getTimestamp();

            if(timestamp == null) {
                timestamp = System.currentTimeMillis();
                metadata.setTimestamp(timestamp);
            }            
            
            MediumProducerRecord outputRecord = 
                    new MediumProducerRecord(key, value, topic, headers, timestamp);
             
            if(async) {
                
                this.driver.sendAsync(outputRecord);
            }
            else {
                
                this.driver.send(outputRecord);
            }
            
        }
        catch(Throwable e) {
            
            throw new MediumMessagingServiceException("Error sending message", e);
        }
        
    }

    /**
     * Flushes the underlying producer. This method should block until all
     * the messages are acknowledged by the underlying messaging system.s
     * @throws MediumMessagingServiceException
     */
    public void flush() throws MediumMessagingServiceException {
        
        this.driver.flush();
    }
     
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
