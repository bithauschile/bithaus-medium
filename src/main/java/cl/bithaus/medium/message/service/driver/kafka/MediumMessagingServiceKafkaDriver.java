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

import cl.bithaus.medium.message.exception.MediumMessagingServiceException;
import cl.bithaus.medium.message.service.driver.MediumMessagingServiceNetworkDriver;
import cl.bithaus.medium.message.service.driver.MediumMessagingServiceNetworkDriverCallback;
import cl.bithaus.medium.record.MediumConsumerRecord;
import cl.bithaus.medium.record.MediumProducerRecord;
import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple
 * @author jmakuc
 */
public class MediumMessagingServiceKafkaDriver implements MediumMessagingServiceNetworkDriver {

    
    
    private static final Logger logger = LoggerFactory.getLogger(MediumMessagingServiceKafkaDriver.class);
    
    private final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
    
    private MediumMessagingServiceNetworkDriverCallback callback;    
    
    private MediumMessagingServiceKafkaDriverConfig config;
    private Producer<String, String> producer;
    private Consumer<String, String> consumer;
    
    private final Semaphore startLock = new Semaphore(0);
    
    private boolean ready = false;

    
    public MediumMessagingServiceKafkaDriver() { }
    
    @Override
    public void init(Map driverProperties, MediumMessagingServiceNetworkDriverCallback callback) throws MediumMessagingServiceException {
        
        this.config = new MediumMessagingServiceKafkaDriverConfig(driverProperties);
        this.callback = callback;
        
        try {
            if(config.isConsumerEnabled()) {

                logger.info("Consumer enabled");
                initConsumer(config);
            }
            else {

                logger.info("Consumer disabled");
            }
        }
        catch(Exception e) {
            
            throw new MediumMessagingServiceException("Error creating kafka consumer", e);
        }

        try {
            if(config.isProducerEnabled()) {

                logger.info("Producer enabled");
                initProducer(config);
            }
        }
        catch(Exception e) {
            
            throw new MediumMessagingServiceException("Error creating kafka producer", e);
        }
             
        if(consumer == null && producer == null)
            throw new MediumMessagingServiceException("Consumer and producer are both disabled");
        
        logger.info("Init OK");
        
    }
    
    private void initConsumer(MediumMessagingServiceKafkaDriverConfig config) throws IOException {        
        
        final Properties props = new Properties();
        
        if(config.getConsumerConfigFile() == null || config.getConsumerConfigFile().length() < 1) {
                        
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getConsumerBootstrapServers());
            props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getConsumerGroupId());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            
        }
        else {
            logger.info("Reading consumer configuration file " + config.getConsumerConfigFile());
            props.load(new FileInputStream(config.getConsumerConfigFile()));
        }
        
        if(config.isTestingModeEnabled()) {
            
            logger.warn("TESTING MODE ENABLED, creating MockConsumer");
            consumer = new MockConsumer(OffsetResetStrategy.EARLIEST);
        }
        else {
            consumer = new KafkaConsumer(props, new StringDeserializer(), new StringDeserializer());
            
        }
        
                
        logger.info("Consumer OK");
    }
    
    private void initProducer(MediumMessagingServiceKafkaDriverConfig config) throws IOException {
        
        final Properties props = new Properties();
        
        if(config.getProducerConfigFile() == null || config.getProducerConfigFile().length() < 1) {
                        
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getProducerBootstrapServers());
            props.put(ConsumerConfig.CLIENT_ID_CONFIG, config.getProducerClientId());
            
        }
        else {
            logger.info("Reading producer configuration file " + config.getProducerConfigFile());
            props.load(new FileInputStream(config.getProducerConfigFile()));
        }
        
        if(config.isTestingModeEnabled()) {
            
            logger.warn("TESTING MODE ENABLED, creating MockProducer");
            producer = new MockProducer(true, new StringSerializer(), new StringSerializer());
        }
        else {
            
            producer = new KafkaProducer(props, new StringSerializer(), new StringSerializer());
        }
                
        logger.info("Producer OK");

    }

    /**
     * Synchronous sending to kafka 
     * @param record
     * @throws MediumMessagingServiceException 
     */
    @Override
    public void send(MediumProducerRecord record) throws MediumMessagingServiceException {

        try {
            
            ProducerRecord<String,String> pr = fromMediumProducerRecord(record);
            
            if(logger.isTraceEnabled())
                logger.trace("OUT > " + pr);
            
            Future f = this.producer.send(pr);
            this.producer.flush();;
            f.get();
        }
        catch(Exception e) {
            
            throw new MediumMessagingServiceException("Error sending producer record to kafka client", e);
        }

    }

    @Override
    public String[] getAvailableTopic() {
        
        if(consumer == null)
            return null;
        
        Set<TopicPartition> assignment = consumer.assignment();
        List<String> topics = new LinkedList<>();
        assignment.forEach((t) -> {
            topics.add(t.topic());
        });
        
        return topics.toArray(new String[topics.size()]);
    }

    @Override
    public boolean isReady() {
        return this.ready;
    }

    @Override
    public void start() throws MediumMessagingServiceException {
        
        
        boolean shouldBeReady = true;

        if(consumer != null) {
            
            try {

                shouldBeReady = false;

                String[] topics = config.getConsumerSubscriptionTopics();

                if(topics != null && topics.length > 0) {

                    if(config.isTestingModeEnabled()) {

                        logger.info("TEST Subscribing to " + Arrays.toString(topics) + " with partition 0");
                        Collection<TopicPartition> partitions = new LinkedList<>();

                        Arrays.stream(topics).forEach((t) -> {

                            partitions.add(new TopicPartition(t, 0));
                        });

                        consumer.assign(partitions);
                    }
                    else {

                        logger.info("Subscribing to " + topics.length + " topics: " + Arrays.toString(topics));
                        consumer.subscribe(Arrays.asList(topics), getConsumerRebalanceListener());
                        this.executor.execute(getConsumerPoller());
                        logger.info("Waiting for partition asignment...");
                        this.startLock.acquire();
                        setReady(true);
                    }
                }                       

            }
            catch(Exception e) {

                throw new MediumMessagingServiceException("Error starting consumer", e);
            }

        }
        else {
            
            // if the consumer is null, then only the producer is enabled.             
            setReady(true);
        }
        

        logger.info("Start complete");

    }

    @Override
    public void stop() throws MediumMessagingServiceException {
        
        if(consumer != null) {
            
            logger.info("Closing consumer");
            consumer.close();
        }
        
        logger.info("Close OK");
    }    

    @Override
    public void subscribe(String[] topics) throws MediumMessagingServiceException {
        
        if(this.consumer == null)
            throw new MediumMessagingServiceException("Consumer is not enabled"); 
        
        logger.info("Subscribing to " + topics.length + " topics: " + Arrays.toString(topics));
        this.consumer.subscribe(Arrays.asList(topics));
                        
    }
    
    private void setReady(boolean value) {
        
        this.ready = value;
        logger.info("READY = " + value);
    }

    public Producer<String, String> getProducer() {
        return producer;
    }

    public Consumer<String, String> getConsumer() {
        return consumer;
    }

    
    private Runnable getConsumerPoller() {
        
        return new Runnable() {
            @Override
            public void run() {
                
                try {
                
                    logger.info("ConsumerPoller : started");

                    while(true) {

                        ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(1000));

                        logger.trace("Received messages " + records.count());

                        Iterator<ConsumerRecord<String,String>> i = records.iterator();

                        int sent = 0;

                        while(i.hasNext()) {

                            ConsumerRecord record = i.next();

                            MediumConsumerRecord mediumRecord = fromKafkaConsuemrRecord(record);

                            try {

                                callback.onMessage(mediumRecord);
                                sent++;
                            }
                            catch(MediumMessagingServiceException e) {

                                logger.error("ConsumerPoller : Error consuming record offer " + record.offset() + " VALUE: " + record.value(), e);
                            } 
                        }

                        if(sent > 0)
                            consumer.commitSync();


                    }
                }
                catch(Exception e) {
                    
                    logger.error("ERROR POLLING", e);
                }
                
            }
        };
    }
    
    private ConsumerRebalanceListener getConsumerRebalanceListener() {
        
        return new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> arg0) {
                
                logger.info("PARTITIONS REVOKED: " + arg0);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> arg0) {
                
                logger.info("PARTITIONS ASSIGNED: " + arg0);
                startLock.release();
            }
        };
    }
    
    
    
    public static ProducerRecord<String,String> fromMediumProducerRecord(MediumProducerRecord src) {
        
        Iterable<Header> iterableHeaders = getIterableHeaders(src.getHeaders());
        ProducerRecord<String,String> pr = new ProducerRecord(src.getTopic(), src.getPartition(), src.getTimestamp(), src.getKey(), src.getValue(), iterableHeaders);
        
        return pr;
    }
    
    public static MediumConsumerRecord fromKafkaConsuemrRecord(ConsumerRecord<String,String> kr) {
        
        Map<String,String> headers = new HashMap<>();
        
        kr.headers().forEach((h) -> {
        
            headers.put(h.key(), new String(h.value()));
        });        
        
        return new MediumConsumerRecord(kr.key(), kr.value(), kr.topic(), headers, kr.timestamp(), kr.partition());
    }
    
    public static Iterable<Header> getIterableHeaders(Map<String,String> headers) {
        
        final List<Header> i = new LinkedList<>();
        
        headers.forEach((k,v) -> {
        
            Header h = new RecordHeader(k, v!=null?v.getBytes():null);
            i.add(h);
        });
        
        return i;
    }
 

}
