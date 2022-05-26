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

import cl.bithaus.medium.message.exception.MediumMessagingServiceException;
import cl.bithaus.medium.message.service.driver.MediumMessagingServiceNetworkDriver;
import cl.bithaus.medium.message.service.driver.MediumMessagingServiceNetworkDriverCallback;
import cl.bithaus.medium.record.MediumConsumerRecord;
import cl.bithaus.medium.record.MediumProducerRecord;
import com.azure.core.credential.AzureNamedKeyCredential;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusProcessorClient;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceivedMessageContext;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import io.netty.util.internal.StringUtil;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Azure service bus synchronous client for Medium
 * @author jmakuc
 */
public class MediumMessagingServiceAzureServiceBusDriver implements MediumMessagingServiceNetworkDriver {

    private static final Logger logger = LoggerFactory.getLogger(MediumMessagingServiceAzureServiceBusDriver.class);
    
    private MediumMessagingServiceNetworkDriverCallback callback;
    
    private ServiceBusSenderClient sender;
    private ServiceBusProcessorClient procesor;
    /**
     * Name of the queue or topic to write to
     */
    private String senderPublishToName;
    private String processorReadFromName;
            
    public MediumMessagingServiceAzureServiceBusDriver() {}
            
    
    @Override
    public void init(Map driverProperties, MediumMessagingServiceNetworkDriverCallback callback) throws MediumMessagingServiceException {
        
        logger.info("Initializing");
        
        this.callback = callback;
        
        MediumMessagingServiceAzureServiceBusDriverConfig config = 
                new MediumMessagingServiceAzureServiceBusDriverConfig(driverProperties);
        
        initProcessor(config);
        initSender(config);
        
    }
    
    private void initSender(MediumMessagingServiceAzureServiceBusDriverConfig config) throws MediumMessagingServiceException {
        
        try {
            
            if(!config.isSenderEnabled()) {
                
                logger.info("Sender disabled");
                return;
            }
            
            ServiceBusClientBuilder builder = new ServiceBusClientBuilder();
            
            String connectionString = config.getConnectionString();
            
            if(connectionString == null)
                throw new MediumMessagingServiceException("Sender connection string is required");
            
            builder = builder.connectionString(connectionString);
            
            logger.info("Creating named credentials with " + config.getCredentialKeyName());
            AzureNamedKeyCredential k = new AzureNamedKeyCredential(config.getCredentialKeyName(), config.getCredentialKey());
            builder.credential(k);
            
            
            
            if(!StringUtil.isNullOrEmpty(config.getSenderQueue()) && !StringUtil.isNullOrEmpty(config.getSenderTopic()))
                throw new MediumMessagingServiceException("Topic and queue configuration cannot be setted together, choose wisely");
            
            ServiceBusClientBuilder.ServiceBusSenderClientBuilder senderBuilder = builder.sender();
            
            if(!StringUtil.isNullOrEmpty(config.getSenderQueue())) {
                senderBuilder.queueName(config.getSenderQueue());
                logger.info("Sender queue name: " + config.getSenderQueue());
                this.senderPublishToName = config.getSenderQueue();
            }
            else if(StringUtil.isNullOrEmpty(config.getSenderTopic())) {
                senderBuilder.topicName(config.getSenderTopic());
                logger.info("Sender topic name: " + config.getSenderTopic());
                this.senderPublishToName = config.getSenderTopic();
            }
            else
                throw new MediumMessagingServiceException("A queue or topic name is required for the sender (only one)");
            
            
            this.sender = senderBuilder.buildClient();
             
        }
        catch(Exception e) {
            
            throw new MediumMessagingServiceException("Error initilizing sender", e);
        }
    }
    

    private void initProcessor(MediumMessagingServiceAzureServiceBusDriverConfig config) throws MediumMessagingServiceException {
        
        try {
            
            if(!config.isProcessorEnabled()) {
                
                logger.info("Processor disabled");
                return;
            }
            
            logger.info("Initializing processor client");
            
            ServiceBusClientBuilder builder = new ServiceBusClientBuilder();
            
            String connectionString = config.getConnectionString();
            
            if(connectionString == null)
                throw new MediumMessagingServiceException("Connection string is required");
            
            builder = builder.connectionString(connectionString);
            
            logger.info("Creating named credentials with " + config.getCredentialKeyName());
            AzureNamedKeyCredential k = new AzureNamedKeyCredential(config.getCredentialKeyName(), config.getCredentialKey());
            builder.credential(k);
            
            
            
            if(!StringUtil.isNullOrEmpty(config.getProcessorQueue()) && !StringUtil.isNullOrEmpty(config.getProcessorTopic()))
                throw new MediumMessagingServiceException("Topic and queue configuration cannot be setted together, choose wisely");
            
            ServiceBusClientBuilder.ServiceBusProcessorClientBuilder processorBuilder = builder.processor();
            
                        if(!StringUtil.isNullOrEmpty(config.getProcessorQueue())) {
                processorBuilder.queueName(config.getProcessorQueue());
                logger.info("Sender queue name: " + config.getProcessorQueue());
                this.senderPublishToName = config.getProcessorQueue();
            }
            else if(StringUtil.isNullOrEmpty(config.getProcessorTopic())) {
                processorBuilder.topicName(config.getProcessorTopic());
                logger.info("Sender topic name: " + config.getProcessorTopic());
                this.senderPublishToName = config.getProcessorTopic();
            }
            else
                throw new MediumMessagingServiceException("A queue or topic name is required for the sender (only one)");
            
            
            processorBuilder.processMessage((t) -> {
                
                MediumConsumerRecord record = null;
                
                try {
                    
                    if(logger.isTraceEnabled())
                        logger.trace("INCOMMING > " + t);
                    
                    record = fromServiceBus(t);
                    
                    this.callback.onMessage(record);
                }
                catch(Exception e) {
                    
                    logger.error("Error processing consumer record, sending to dead letter: " + record, e);
                    t.deadLetter();
                }
            });
            
            processorBuilder.processError((t) -> {
               
                logger.error("Error in processor: " + t);
                
            });
            
            
            this.procesor = processorBuilder.buildProcessorClient();            
             
        }
        catch(Exception e) {
            
            throw new MediumMessagingServiceException("Error initilizing processor", e);
        }
    }    
    

    @Override
    public void send(MediumProducerRecord record) throws MediumMessagingServiceException {
        
        if(this.sender == null)
            throw new MediumMessagingServiceException("Sender is not enabled");
        
        if(record.getTopic() != null && record.getTopic().compareTo(this.senderPublishToName) != 0)
            throw new MediumMessagingServiceException("Unsupported record topic name (" + record.getTopic() + "), accepted: " + this.senderPublishToName);
        
        ServiceBusMessage msg = fromMediumProducerRecord(record);
        
        if(logger.isTraceEnabled())
            logger.trace("OUT > " + msg);
        
        this.sender.sendMessage(msg);
    }

    @Override
    public void subscribe(String[] topics) throws MediumMessagingServiceException {
        throw new UnsupportedOperationException("Service bus does not support dynamic topic subscription"); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String[] getAvailableTopic() {
        return new String[] { this.processorReadFromName } ;
    }

    @Override
    public boolean isReady() {
        return true;
    }

    @Override
    public void start() throws MediumMessagingServiceException {
        
        if(this.procesor != null)
            this.procesor.start();
        
        logger.info("Start completed");
    }

    @Override
    public void stop() throws MediumMessagingServiceException {
        
        try {
            if(this.procesor != null)
                this.procesor.close();
        }
        catch(Exception e) {
            throw new MediumMessagingServiceException("Error trying to close processor", e);
        }        
        
        try {
            if(this.sender != null)
                this.sender.close();
        }
        catch(Exception e) {
            throw new MediumMessagingServiceException("Error trying to close sender", e);
        }
        
        
    }
    
    
    
    public static ServiceBusMessage fromMediumProducerRecord(MediumProducerRecord record) {
        
        
       
        ServiceBusMessage msg = new ServiceBusMessage(record.getValue());
        msg.getApplicationProperties().putAll(record.getHeaders());
        msg.getApplicationProperties().put("timestamp", record.getTimestamp());
        msg.getApplicationProperties().put("topic", record.getTopic());
        
        
        msg.setPartitionKey(record.getKey());
        
        return msg;
    }
    
    public static MediumConsumerRecord fromServiceBus(ServiceBusReceivedMessageContext context) {
        
        
        ServiceBusReceivedMessage message = context.getMessage();
        
        Map<String,String> headers = new HashMap<>();
        message.getApplicationProperties().forEach((k, v) -> {
            headers.put(k, v + "");
        });
        
        String key = message.getPartitionKey();
        String topic = message.getApplicationProperties().get("topic") + "";
        Object timestampStr = message.getApplicationProperties().get("timestamp");
        Long timestamp = timestampStr!=null?Long.parseLong(timestampStr.toString()):null;
        
        
        return new MediumConsumerRecord(key, message.getBody().toString(), topic, headers, timestamp);
        
    }
    
}
