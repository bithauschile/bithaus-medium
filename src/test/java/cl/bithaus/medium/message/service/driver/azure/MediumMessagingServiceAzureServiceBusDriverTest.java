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

import cl.bithaus.medium.message.MediumMessage;
import cl.bithaus.medium.message.service.driver.MediumMessagingServiceNetworkDriverCallback;
import cl.bithaus.medium.message.service.driver.MediumMessagingServiceUtils;
import cl.bithaus.medium.record.MediumConsumerRecord;
import cl.bithaus.medium.utils.test.TestMessage;
import cl.bithaus.medium.utils.test.TestRecordGenerator;
import com.google.gson.Gson;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
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
public class MediumMessagingServiceAzureServiceBusDriverTest {
    
    private static final Logger logger = LoggerFactory.getLogger(MediumMessagingServiceAzureServiceBusDriverTest.class);
    
    Gson gson = new Gson();
    
    public MediumMessagingServiceAzureServiceBusDriverTest() throws Exception {
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
     * Test of init method, of class MediumMessagingServiceAzureServiceBusDriver.
     */
    @Test
    public void testInit() throws Exception {
        
        System.out.println("init");
        Map driverProperties = getConfigMap();
        
        LinkedBlockingQueue<MediumConsumerRecord> queue = new LinkedBlockingQueue<>();
        
        
        MediumMessagingServiceNetworkDriverCallback callback = (t) -> {
            
            logger.info("Callback " + t);
            queue.add(t);
        };
                
                
        MediumMessagingServiceAzureServiceBusDriver instance = new MediumMessagingServiceAzureServiceBusDriver();
        instance.init(driverProperties, callback);
        instance.start();
        
        while(true) {
            
            MediumConsumerRecord record = queue.poll(2, TimeUnit.SECONDS);
            
            if(record == null)
                break;
        }
        
        MediumMessage message = new TestMessage("hola hola");        
        instance.send(TestRecordGenerator.fromMediumMessage(message, "medium-queue-dcp"));
        
        
        MediumConsumerRecord record = queue.take();
        
        logger.info("Record " + record);
        
        TestMessage received = MediumMessagingServiceUtils.toMedium(TestMessage.class, record);
        
        logger.info("MESSAGE  " + gson.toJson(message));
        logger.info("RECEIVED " + gson.toJson(received));
        
        // the extra headers added by the driver must be removed in order to be equal
        received.getMetadata().getHeaders().remove("timestamp");
        received.getMetadata().getHeaders().remove("topic");
        // and topic tx/rx fix
        received.getMetadata().setTxTopic(received.getMetadata().getRxTopic());
        received.getMetadata().setRxTopic(null);
        
        Assertions.assertEquals(message, received);

        
    }
    
    public Map<String,String> getConfigMap() {
        
        Map<String,String> map = new HashMap<>();
        
        map.put(MediumMessagingServiceAzureServiceBusDriverConfig.CONNECTIONSTRING_CONFIG, "Endpoint=sb://sb-mediumdcp.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=H+k26DVHSAWwQd30BtIfR6sgDkrYAXhK8IIBEg7oF5k=");
        map.put(MediumMessagingServiceAzureServiceBusDriverConfig.CREDENTIAL_KEY_NAME_CONFIG, "medium_cmi-dcp");
        map.put(MediumMessagingServiceAzureServiceBusDriverConfig.CREDENTIAL_KEY_CONFIG, "WORHcFfdbzOBZR6OAgKnoW7wkhUUmHJKRYBj9datQIU=");

        map.put(MediumMessagingServiceAzureServiceBusDriverConfig.SENDER_ENABLED_CONFIG, "true");
        map.put(MediumMessagingServiceAzureServiceBusDriverConfig.SENDER_QUEUE_CONFIG, "medium-queue-dcp");

        map.put(MediumMessagingServiceAzureServiceBusDriverConfig.PROCESSOR_ENABLED_CONFIG, "true");
        map.put(MediumMessagingServiceAzureServiceBusDriverConfig.PROCESSOR_QUEUE_CONFIG, "medium-queue-dcp");
        
        return map;
    }
 
    
}
