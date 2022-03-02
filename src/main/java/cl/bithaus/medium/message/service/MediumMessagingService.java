/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cl.bithaus.medium.message.service;

import cl.bithaus.medium.message.MediumMessage;
import cl.bithaus.medium.message.exception.MediumMessagingServiceException;
import cl.bithaus.medium.message.exception.SendToDeadLetterException;
import cl.bithaus.medium.message.service.driver.MediumMessagingServiceConsumerRecord;
import cl.bithaus.medium.message.service.driver.MediumMessagingServiceNetworkDriver;
import cl.bithaus.medium.message.service.driver.MediumMessagingServiceNetworkDriverCallback;
import cl.bithaus.medium.message.service.driver.MediumMessagingServiceProducerRecord;
import cl.bithaus.medium.utils.MapUtils;
import cl.bithaus.message.BHRootMessage;
import cl.bithaus.messaging.service.driver.BHMessagingServiceNetworkDriver;
import cl.bithaus.messaging.service.exception.BHMessagingServiceException;
import com.google.gson.Gson;
import java.io.FileNotFoundException;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Medium Messagin Service
 * Abstraction layer to the underlying transport system.
 * @author jmakuc
 */
public class MediumMessagingService {
    
    public static final String PROP_LOGGER_SUFIX = "medium.message.service.loggerSufix";
            
    
    private static final int VERSION = 1;
    
    private final Logger logger;
    
    private Map<String,String> properties;    
    private Gson gson;
    
    // Driver
    private MediumMessagingServiceNetworkDriver driver;
    private MediumMessagingServiceConfig serviceConfig;
    
    // Handlers
    private MediumMessagingServiceRawHandler rawHandler;
    private boolean running = false;
    
    
    public MediumMessagingService(MediumMessagingServiceConfig config) throws MediumMessagingServiceException {
        
        if(config == null)
            throw new NullPointerException("Service configuration cannot be null");
     
        String loggerSufix = config.getLoggerSuffix()!=null?config.getLoggerSuffix():"";
        
        this.serviceConfig = config;
        this.logger = LoggerFactory.getLogger(this.getClass().getName() + loggerSufix);
                
        this.rawHandler = new MediumMessagingServiceDefaultRawHandler();
        
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
            
            driver.init(driverConfigMap, new MediumMessagingServiceNetworkDriverCallback() {

                @Override
                public void onMessage(MediumMessagingServiceConsumerRecord record) throws MediumMessagingServiceException, SendToDeadLetterException {
                
                    rawHandler.onData(record);
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
    
    public void send(String channel, MediumMessage message) throws MediumMessagingServiceException {
        
        try {
            
            if(!running)
                throw new IllegalStateException("Messaging Service is not running");
            
            if(channel == null)
                throw new IllegalArgumentException("Channel cannot be null");
            
            if(message == null)
                throw new IllegalArgumentException("Message cannot be null");
            
            String serializedData = gson.toJson(message);
            
            if(logger.isDebugEnabled()) {
                
                if(logger.isTraceEnabled()) {
                    
                    logger.debug(String.format("Sending message [Ch:%s]: (%s) %s", channel, message.getClass().getName(), serializedData));
                }
                else {
                    
                    logger.debug(String.format("Sending message [Ch:%s]: %s", channel, message.getClass().getName()));
                }
            }


            /**
             * TODO:
             * validar el reemplazo de metadata
             */
            MediumMessagingServiceProducerRecord record = 
                    new MediumMessagingServiceProducerRecord(channel, channel, channel, properties, Long.MIN_VALUE);
            

            BHMessageEnvelope envelope = new BHMessageEnvelope();
            envelope.setData(serializedData);
            envelope.setMessageType(message.getClass().getName());
            envelope.setVersion(VERSION);

            
            driver.send(record);

            
        }
        catch(Throwable e) {
            
            throw new BHMessagingServiceException("Error sending message", e);
        }
        
    }
    
    public void send(String topic, MediumMessage[] messages) throws MediumMessagingServiceException {
        
        try {
            
            if(!running)
                throw new IllegalStateException("Messaging Service is not running");
            
            if(topic == null)
                throw new IllegalArgumentException("Channel cannot be null");
            
            if(messages == null || messages.length < 1)
                throw new IllegalArgumentException("Message cannot be null or empty");
            
                    
            
            if(logger.isDebugEnabled()) {
                
                if(logger.isTraceEnabled()) {
                    
                    logger.debug(String.format("Sending message batch [Ch:%s]: %s", topic, messages.length));
                }
                else {
                    
                    logger.debug(String.format("Sending message batch [Ch:%s]: %s", topic, messages.length));
                }
            }
                        
            if(this.writeWrappingEnvelope) {
                
                String[] serializedEnvelopes = new String[messages.length];
                
                for(int i = 0; i < messages.length; i++) {
                    
                    String serializedData = gson.toJson(messages[i]);
                    BHMessageEnvelope envelope = new BHMessageEnvelope();
                    envelope.setData(serializedData);
                    envelope.setMessageType(messages[i].getClass().getName());
                    envelope.setVersion(VERSION);
                    
                    serializedEnvelopes[i] = gson.toJson(envelope);
                }                

                driver.send(topic, serializedEnvelopes);
            }
            else {
                
                String[] serializedMessages = new String[messages.length];
                
                for(int i = 0; i < messages.length; i++) {
                    
                    serializedMessages[i] = gson.toJson(messages[i]);
                }
                
                driver.send(topic, serializedMessages);
            }
            
        }
        catch(Throwable e) {
            
            throw new MediumMessagingServiceException("Error sending message: " + e.getMessage(), e);
        }
        
        
    }
    
    public void sendRaw(String channel, String rawMessage) throws MediumMessagingServiceException {
        
        try {
            
            if(!running)
                throw new IllegalStateException("Messaging Service is not running");
            
            if(channel == null)
                throw new IllegalArgumentException("Channel cannot be null");
            
            if(rawMessage == null || rawMessage.length() < 1)
                throw new IllegalArgumentException("Message cannot be null nor empty");
            
            
            if(logger.isDebugEnabled()) {
                                
                logger.debug(String.format("Sending message [Ch:%s]: %s", channel, rawMessage));                
            }
                        
            driver.send(channel, rawMessage);
        }
        catch(Throwable e) {
            
            throw new MediumMessagingServiceException("Error sending message", e);
        }
        
    }
    
    public <M extends BHRootMessage> void addMessageHandler(Class<M> messageType, BHMessageHandler<? super M> handler) {
    
        this.rawHandler.addMessageHandler(messageType, handler);
    }
     
    public <M extends BHRootMessage> boolean removeMessageHandler(Class<M> messageType, BHMessageHandler<? super M> handler) {
        
        return this.rawHandler.removeMessageHandler(messageType, handler);
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

    public boolean isWriteWrappingEnvelope() {
        return writeWrappingEnvelope;
    }

    /**
     * Sets the writing of the envelope object in the outgoing string.
     * @param writeWrappingEnvelope 
     */
    public void setWriteWrappingEnvelope(boolean writeWrappingEnvelope) {
        
        logger.info(writeWrappingEnvelope?"Envelope writing enabled":"Envelope writing DISABLED");
        
        this.writeWrappingEnvelope = writeWrappingEnvelope;
    }

    public Gson getGson() {
        return gson;
    }

    public void setGson(Gson gson) {
        
        logger.info("Replacing gson instance with " + gson);
        this.gson = gson;
    }
    
}
