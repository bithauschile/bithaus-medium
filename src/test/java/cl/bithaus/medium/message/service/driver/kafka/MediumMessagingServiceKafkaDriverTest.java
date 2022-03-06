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
import java.util.Properties;
import java.util.concurrent.LinkedBlockingDeque;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.AfterAll;
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
        
        
        logger.info("Sending record");
        instance.send(record);
        logger.info("Record Sent");
        
        MediumConsumerRecord received = list.take();
        logger.info("Record received: " + gson.toJson(received));
        
        
        
//        Assertions.assertEquals(expected, list.get(0));
        
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
    
    
//
//    /**
//     * Test of send method, of class MediumMessagingServiceKafkaDriver.
//     */
//    @Test
//    public void testSend() throws Exception {
//        System.out.println("send");
//        MediumProducerRecord record = null;
//        MediumMessagingServiceKafkaDriver instance = new MediumMessagingServiceKafkaDriver();
//        instance.send(record);
//        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
//    }
//
//    /**
//     * Test of getAvailableTopic method, of class MediumMessagingServiceKafkaDriver.
//     */
//    @Test
//    public void testGetAvailableTopic() {
//        System.out.println("getAvailableTopic");
//        MediumMessagingServiceKafkaDriver instance = new MediumMessagingServiceKafkaDriver();
//        String[] expResult = null;
//        String[] result = instance.getAvailableTopic();
//        assertArrayEquals(expResult, result);
//        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
//    }
//
//    /**
//     * Test of isReady method, of class MediumMessagingServiceKafkaDriver.
//     */
//    @Test
//    public void testIsReady() {
//        System.out.println("isReady");
//        MediumMessagingServiceKafkaDriver instance = new MediumMessagingServiceKafkaDriver();
//        boolean expResult = false;
//        boolean result = instance.isReady();
//        assertEquals(expResult, result);
//        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
//    }
//
//    /**
//     * Test of start method, of class MediumMessagingServiceKafkaDriver.
//     */
//    @Test
//    public void testStart() throws Exception {
//        System.out.println("start");
//        MediumMessagingServiceKafkaDriver instance = new MediumMessagingServiceKafkaDriver();
//        instance.start();
//        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
//    }
//
//    /**
//     * Test of stop method, of class MediumMessagingServiceKafkaDriver.
//     */
//    @Test
//    public void testStop() throws Exception {
//        System.out.println("stop");
//        MediumMessagingServiceKafkaDriver instance = new MediumMessagingServiceKafkaDriver();
//        instance.stop();
//        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
//    }
//
//    /**
//     * Test of subscribe method, of class MediumMessagingServiceKafkaDriver.
//     */
//    @Test
//    public void testSubscribe() throws Exception {
//        System.out.println("subscribe");
//        String[] topics = null;
//        MediumMessagingServiceKafkaDriver instance = new MediumMessagingServiceKafkaDriver();
//        instance.subscribe(topics);
//        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
//    }
//
//    /**
//     * Test of fromMediumProducerRecord method, of class MediumMessagingServiceKafkaDriver.
//     */
//    @Test
//    public void testFromMediumProducerRecord() {
//        System.out.println("fromMediumProducerRecord");
//        MediumProducerRecord src = null;
//        ProducerRecord<String, String> expResult = null;
//        ProducerRecord<String, String> result = MediumMessagingServiceKafkaDriver.fromMediumProducerRecord(src);
//        assertEquals(expResult, result);
//        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
//    }
//
//    /**
//     * Test of getIterableHeaders method, of class MediumMessagingServiceKafkaDriver.
//     */
//    @Test
//    public void testGetIterableHeaders() {
//        System.out.println("getIterableHeaders");
//        Map<String, String> headers = null;
//        Iterable<Header> expResult = null;
//        Iterable<Header> result = MediumMessagingServiceKafkaDriver.getIterableHeaders(headers);
//        assertEquals(expResult, result);
//        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
//    }
//    
}
