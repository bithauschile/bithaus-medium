package cl.bithaus.medium.message.service.driver.amqp;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;

public class MediumMessagingServiceAMQPDriverTest {

    public MediumMessagingServiceAMQPDriverTest() throws Exception {

        java.util.logging.LogManager.getLogManager().readConfiguration(new FileInputStream("conf-example/logger_unitTests.properties"));

    }

    // @Test
    void testStart() throws Exception {

        Map driverProperties = getConfigMap();

        MediumMessagingServiceAMQPDriver instance = new MediumMessagingServiceAMQPDriver();
        instance.init(driverProperties, record -> {
            System.out.println("RECEIVED: " + record);
        });

        instance.start();

        Thread.sleep(Long.MAX_VALUE);
    }


    
    private Map<String,String> getConfigMap() {
                
        Map<String,String> map = new HashMap<>();
        map.put(MediumMessagingServiceAMQPDriverConfig.SERVER_URI_CONFIG, "amqps://EXAMPLE.servicebus.windows.net");        
        map.put(MediumMessagingServiceAMQPDriverConfig.SERVER_USERNAME_CONFIG, "shared-access-key-name");        
        map.put(MediumMessagingServiceAMQPDriverConfig.SERVER_PASSWORD_CONFIG, "shared-access-key=");
        map.put(MediumMessagingServiceAMQPDriverConfig.CONSUMER_ENABLED_CONFIG, "true");        
        map.put(MediumMessagingServiceAMQPDriverConfig.CONSUMER_QUEUE_CONFIG, "queue-name");         
        
        return map;
    }    
}
 