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
package cl.bithaus.medium.message.service.driver;

import cl.bithaus.medium.message.MediumMessage;
import cl.bithaus.medium.record.MediumProducerRecord;
import cl.bithaus.medium.record.MediumConsumerRecord;
import cl.bithaus.medium.message.exception.MediumMessagingServiceException;
import cl.bithaus.medium.message.exception.SendToDeadLetterException;
import cl.bithaus.medium.utils.test.TestMessage;
import cl.bithaus.medium.utils.test.TestNetworkDriver;
import com.google.gson.Gson;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
public class MediumMessagingServiceNetworkDriverTest {
    
    private static Logger logger = LoggerFactory.getLogger(MediumMessagingServiceNetworkDriverTest.class);
    private static Gson gson = new Gson();
    
    public MediumMessagingServiceNetworkDriverTest() {
    }
    
    @BeforeAll
    public static void setUpClass() {
    }
    
    @AfterAll
    public static void tearDownClass() {
    }
    
    @BeforeEach
    public void setUp() {
    }
    
    @AfterEach
    public void tearDown() {
    }

    /**
     * Test of init method, of class MediumMessagingServiceNetworkDriver.
     */
    @Test
    public void testOk() throws Exception {
        System.out.println("testOk");
        Map driverProperties = new HashMap();
        
        final List<MediumConsumerRecord> messageQueue = 
                new LinkedList<>();
        
        MediumMessagingServiceNetworkDriverCallback callback = (var record) -> {
            messageQueue.add(record);
        };
                                
                
        TestNetworkDriver driver = new TestNetworkDriver();
        driver.init(driverProperties, callback);
        driver.start();
        
        
        // no message sent, wait should fail
        try {
            
            MediumProducerRecord pr0 = driver.waitForRecord(1, TimeUnit.SECONDS);
            
            Assertions.assertNull(pr0);
        }
        catch(InterruptedException e) {}
        
        
        TestMessage tm1 = new TestMessage("hola");
        tm1.getMetadata().setKey("llave1");
        tm1.getMetadata().setTxTopic("output");
        tm1.getMetadata().setTimestamp(10L);
        
        MediumProducerRecord pr1 = MediumMessagingServiceUtils.fromMedium(tm1);        
        driver.send(pr1);
        
        // reception
        
        MediumProducerRecord pr2 = driver.waitForRecord(1, TimeUnit.SECONDS);        
        Assertions.assertEquals(pr1, pr2);
        
        // reception of message type
        driver.send(pr1);
        TestMessage tm2 = driver.waitForMessage(TestMessage.class, 1, TimeUnit.SECONDS);
        
        // cheat: txTopic and rxTopic are inverted in the producer v/s consumer
        tm2.getMetadata().setTxTopic(tm2.getMetadata().getRxTopic());
        tm2.getMetadata().setRxTopic(null);
        
        
        Assertions.assertEquals(tm1, tm2);
        
        // bounce back, message should be in the messageQueue
        driver.simulateIncommingMessage(tm2);
        
        MediumConsumerRecord cr1 = messageQueue.get(0);
        TestMessage tm3 = MediumMessagingServiceUtils.toMedium(TestMessage.class, cr1);
        
        // topic correction again
        tm3.getMetadata().setTxTopic(tm3.getMetadata().getRxTopic());
        tm3.getMetadata().setRxTopic(null);
        
        Assertions.assertEquals(tm3, tm2);
        
        
    }
 
    
}
