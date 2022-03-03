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
package cl.bithaus.medium.message.service;

import cl.bithaus.medium.message.exception.MediumMessageListenerException;
import cl.bithaus.medium.message.service.driver.MediumMessagingServiceConsumerRecord;
import cl.bithaus.medium.utils.test.TestMessage;
import cl.bithaus.medium.utils.test.TestRecordGenerator;
import com.google.gson.Gson;
import java.util.concurrent.atomic.AtomicReference;
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
public class MediumMessagingServiceDefaultRawHandlerTest {
    
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private Gson gson = new Gson();
    
    public MediumMessagingServiceDefaultRawHandlerTest() {
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
     * Test of onData method, of class MediumMessagingServiceDefaultRawHandler.
     */
    @Test
    public void testOnData() throws Exception {
        logger.info("onData");
        
        TestMessage testMessage = new TestMessage("this is a test");        
        
        MediumMessagingServiceConsumerRecord record = TestRecordGenerator.generateConsumerRecord(testMessage);
        MediumMessagingServiceDefaultRawHandler instance = new MediumMessagingServiceDefaultRawHandler();
        
        AtomicReference<TestMessage> dest = new AtomicReference<>();
        
        instance.addMessageHandler(TestMessage.class, new MediumMessageListener<TestMessage>() {
            @Override
            public String getName() {
                return "test-message-listener";
            }

            @Override
            public void onMessage(TestMessage message) throws MediumMessageListenerException {
                dest.set(message);
            }
        });
        
        instance.onData(record);
        
        logger.info("Original: " + gson.toJson(testMessage));
        logger.info("Deserialized: " + gson.toJson(dest.get()));
        
        Assertions.assertEquals(testMessage, dest.get());
                
    }
    
    
}