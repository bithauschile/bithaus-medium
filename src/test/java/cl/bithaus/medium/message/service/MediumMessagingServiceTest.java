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

import cl.bithaus.medium.message.MediumMessage;
import cl.bithaus.medium.message.exception.MediumMessageListenerException;
import cl.bithaus.medium.utils.test.TestMessage;
import cl.bithaus.medium.utils.test.TestNetworkDriver;
import com.google.gson.Gson;
import java.io.FileInputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
public class MediumMessagingServiceTest {
    
    private static final Logger logger = LoggerFactory.getLogger(MediumMessagingServiceTest.class);
    private Gson gson = new Gson();

    public MediumMessagingServiceTest() throws Exception {
        java.util.logging.LogManager.getLogManager().readConfiguration(new FileInputStream("conf-example/logger.properties"));
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
     * Test of start method, of class MediumMessagingService.
     */
    @Test
    public void testStart() throws Exception {
        System.out.println("start");
        
        Map originals = new HashMap();
//        originals.put(MediumMessagingServiceConfig.DEFAULT_PRODUCER_TOPIC_CONFIG, "topic1");
        originals.put(MediumMessagingServiceConfig.DRIVER_CLASSNAME_CONFIG, TestNetworkDriver.class.getName());
//        originals.put(MediumMessagingServiceConfig.DRIVER_CONFIGFILE_CONFIG, "topic1");
//        originals.put(MediumMessagingServiceConfig.LOGGER_SUFIX_CONFIG, "topic1");
        
        MediumMessagingServiceConfig config = new MediumMessagingServiceConfig(originals);
        MediumMessagingService instance = new MediumMessagingService(config);
        TestNetworkDriver driver = (TestNetworkDriver) instance.getDriver();
        
        List<MediumMessage> messageList = new LinkedList<>();
        
        instance.addMessageListener(TestMessage.class, new MediumMessageListener<TestMessage>() {
            @Override
            public String getName() {
                return "listaner-1";
            }

            @Override
            public void onMessage(TestMessage message) throws MediumMessageListenerException {
                messageList.add(message);
            }
        });
        
        instance.start();
        
        try {
            
            instance.send(new TestMessage("hola " + new Date()), null);
            Assertions.fail("no default topic, this should fail");
        }
        catch(Exception e) {
            
            
        }
        
        TestMessage tm1 = new TestMessage("hola " + new Date());
        instance.send(tm1, "topic1");
        
        TestMessage tm2 = driver.waitForMessage(TestMessage.class);
        
        tm1.getMetadata().setRxTopic("topic1");
        tm2.getMetadata().setTxTopic("topic1");
        
        logger.info(gson.toJson(tm1));
        logger.info(gson.toJson(tm2));
        
        Assertions.assertEquals(tm1, tm2);
        
    }

    
}
