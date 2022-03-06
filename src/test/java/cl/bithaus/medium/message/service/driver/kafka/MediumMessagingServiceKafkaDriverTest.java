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
import cl.bithaus.medium.message.service.driver.MediumMessagingServiceNetworkDriverCallback;
import cl.bithaus.medium.record.MediumConsumerRecord;
import cl.bithaus.medium.record.MediumProducerRecord;
import com.google.gson.Gson;
import java.io.FileInputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author jmakuc
 */
public class MediumMessagingServiceKafkaDriverTest {
    
    private static Logger logger = LoggerFactory.getLogger(MediumMessagingServiceKafkaDriverTest.class);
    private Gson gson = new Gson();
    
    public MediumMessagingServiceKafkaDriverTest() throws Exception {
        
        java.util.logging.LogManager.getLogManager().readConfiguration(new FileInputStream("conf-example/logger.properties"));

    }
    
    @BeforeAll
    public static void setUpClass() throws Exception {
        

    }
    
    @AfterAll
    public static void tearDownClass() {
    }
    
    @BeforeEach
    public void setUp() throws Exception {
             
    }
    
    @AfterEach
    public void tearDown() {
    }

    /**
     * Test of init method, of class MediumMessagingServiceKafkaDriver.
     */
    @Test
    public void testConsumer() throws Exception {
        logger.info("init");
        
        String topicToUse = "topic2";
        
        LinkedBlockingDeque<MediumConsumerRecord> list = new LinkedBlockingDeque<>();
        
        Map driverProperties = getConfigMap();
        
        MediumMessagingServiceNetworkDriverCallback callback = new MediumMessagingServiceNetworkDriverCallback() {
            @Override
            public void onMessage(MediumConsumerRecord record) throws MediumMessagingServiceException {
                
                logger.info("RECEIVED > " + gson.toJson(record));
                
                list.add(record);
            }
        };
                
        MediumMessagingServiceKafkaDriver instance = new MediumMessagingServiceKafkaDriver();
        instance.init(driverProperties, callback);
        instance.start();
        
        
        Map<String,String> headers = new HashMap<>();
        headers.put("prop1", "value1");
        headers.put("prop2", "value2");        
        MediumConsumerRecord expected = new MediumConsumerRecord("key", "value " + new Date(), topicToUse, headers, new Date().getTime(), 0);

        MediumProducerRecord record = 
                    new MediumProducerRecord("key", "value " + new Date(), topicToUse, headers, expected.getTimestamp());
        
        logger.info("consuming all messages");
        
        while(true) {
            
            MediumConsumerRecord r = list.poll(1, TimeUnit.SECONDS);
            
            if(r == null)
                break;
        }
        
        
        logger.info("Sending record");
        instance.send(record);
        logger.info("Record Sent");
        
        MediumConsumerRecord received = list.take();
        logger.info("Record received: " + gson.toJson(received));        
        
        Assertions.assertEquals(expected, received);
        
    }
    
    private Map<String,String> getConfigMap() {
        
        long timestampo = new Date().getTime();
        
        Map<String,String> map = new HashMap<>();
//        map.put(MediumMessagingServiceKafkaDriverConfig.TESTING_MODE_ENABLED, "true");
        map.put(MediumMessagingServiceKafkaDriverConfig.CONSUMER_ENABLED_CONFIG, "true");        
        map.put(MediumMessagingServiceKafkaDriverConfig.CONSUMER_BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        map.put(MediumMessagingServiceKafkaDriverConfig.CONSUMER_GROUPID_CONFIG, "group2-" + timestampo);
        map.put(MediumMessagingServiceKafkaDriverConfig.CONSUMER_SUBSCRIPTIONTOPICS_CONFIG, "topic1,topic2");
        
        map.put(MediumMessagingServiceKafkaDriverConfig.PRODUCER_ENABLED_CONFIG, "true");
        map.put(MediumMessagingServiceKafkaDriverConfig.PRODUCER_BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        map.put(MediumMessagingServiceKafkaDriverConfig.PRODUCER_CLIENTID_CONFIG, "client-junit");        
        
        return map;
    }
    
     
}
