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
package cl.bithaus.medium.utils.test;

import cl.bithaus.medium.message.MediumMessage;
import cl.bithaus.medium.message.exception.MediumMessagingServiceException;
import cl.bithaus.medium.message.exception.SendToDeadLetterException;
import cl.bithaus.medium.record.MediumConsumerRecord;
import cl.bithaus.medium.message.service.driver.MediumMessagingServiceNetworkDriver;
import cl.bithaus.medium.message.service.driver.MediumMessagingServiceNetworkDriverCallback;
import cl.bithaus.medium.record.MediumProducerRecord;
import cl.bithaus.medium.utils.MessageUtils;
import com.google.gson.Gson;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author jmakuc
 */
public class TestNetworkDriver implements MediumMessagingServiceNetworkDriver {

    private static final Logger logger = LoggerFactory.getLogger(TestNetworkDriver.class);
    
    private MediumMessagingServiceNetworkDriverCallback callback;
    
    private static final Gson gson = new Gson();
    
    private List<String> topics = new LinkedList<>();
    
    private LinkedBlockingQueue<MediumProducerRecord> recordQueue = new LinkedBlockingQueue<>();
    
    private AtomicBoolean ready = new AtomicBoolean(false);
    
    @Override
    public void init(Map driverProperties, MediumMessagingServiceNetworkDriverCallback callback) throws MediumMessagingServiceException {
        
        if(callback == null)
            throw new NullPointerException("callback cannot be null");
                
        logger.info("INIT: Properties: " + gson.toJson(driverProperties));
        this.callback = callback;
        
        this.topics.add("topic1");
        this.topics.add("topic2");
    }

    /**
     * Invoked by the messaging service to send records to the network.
     * @param record
     * @throws MediumMessagingServiceException 
     */
    @Override
    public void send(MediumProducerRecord record) throws MediumMessagingServiceException {
        
        if(!this.isReady())
            throw new MediumMessagingServiceException("Services has not been started");
        
        logger.info("OUT > " + record);
                
        this.recordQueue.offer(record);
    }

    @Override
    public String[] getAvailableTopic() {
        return this.topics.toArray(new String[this.topics.size()]);
    }

    @Override
    public void start() throws MediumMessagingServiceException {
        
        logger.info("START");
        this.ready.set(true);
    }

    @Override
    public void stop() throws MediumMessagingServiceException {
        logger.info("STOP");
        this.ready.set(false);
    }
    
    /**
     * Waits for a record to be sent to the network. Returns inmediately if a
     * record was already sent.
     * @return Record sent to the network
     */
    public MediumProducerRecord waitForRecord() {
        
        return this.recordQueue.poll();        
    }
    
    /**
     * Waits for a record to be sent to the network. Returns inmediately if a
     * record was already sent.
     * @param timeout maxium time to wait for the record
     * @param timeUnit time unit of the timeout
     * @return Record sent to the network
     * @throws InterruptedException 
     */
    public MediumProducerRecord waitForRecord(long timeout, TimeUnit timeUnit) 
            throws InterruptedException {
        
        return this.recordQueue.poll(timeout, timeUnit);
    }
    
    /**
     * Waits for a certain type of message to be sent to the network
     * @param <M> message class
     * @param messageClass message class
     * @return message
     */
    public <M extends MediumMessage> M waitForMessage(Class<M> messageClass) throws ClassNotFoundException {
        
        while(true) {
            
            MediumProducerRecord pr = this.waitForRecord();
            
            String recordClassName = pr.getHeaders().get(MediumMessage.HEADER_MESSAGE_CLASS);
            Class recordClass = Class.forName(recordClassName);
            
            if(messageClass.isAssignableFrom(recordClass)) {
                
                MediumConsumerRecord cr = TestRecordGenerator.fromProducerRecord(pr);
                
                return MessageUtils.toMedium(messageClass, cr);
            }
                        
        }        
        
    }
    
    /**
     * Waits for a message of a certain type to be sent to the network discarding
     * all other messages.
     * @param <M> message class
     * @param messageClass message class
     * @param timeout maxium time to wait for the record
     * @param timeUnit time unit of the timeout
     * @return message
     * @throws java.lang.ClassNotFoundException
     * @throws java.lang.InterruptedException
     */
    public <M extends MediumMessage> M waitForMessage(Class<M> messageClass, long timeout, TimeUnit timeUnit) 
            throws ClassNotFoundException, InterruptedException {
        
        while(true) {
            
            MediumProducerRecord pr = this.waitForRecord(timeout, timeUnit);
            
            String recordClassName = pr.getHeaders().get(MediumMessage.HEADER_MESSAGE_CLASS);
            Class recordClass = Class.forName(recordClassName);
            
            if(messageClass.isAssignableFrom(recordClass)) {
                
                MediumConsumerRecord cr = TestRecordGenerator.fromProducerRecord(pr);
                
                return MessageUtils.toMedium(messageClass, cr);
            }
                        
        }          
    }    
    
    /**
     * Simulates an incomming message from the network.
     * @param incomming incomming message
     * @throws MediumMessagingServiceException Internal processing error.
     * @throws SendToDeadLetterException when the message cannot be processed and should be handled as dead letter.
     */
    public void simulateIncommingMessage(MediumMessage incomming) throws MediumMessagingServiceException, SendToDeadLetterException {
        
        logger.info("IN > " + incomming);
        MediumConsumerRecord cr = TestRecordGenerator.generateConsumerRecord(incomming);
        this.callback.onMessage(cr);        
    }

    
    public void simulateIncommingRaw(String key, String value, String topic) throws MediumMessagingServiceException, SendToDeadLetterException {
        
        MediumConsumerRecord cr = new MediumConsumerRecord(key, value, topic);
        this.callback.onMessage(cr);
    }
    

    @Override
    public boolean isReady() {
        return this.ready.get();
    }

    @Override
    public void subscribe(String[] topics) {
        this.topics.addAll(Arrays.asList(topics));
    }
    
}
