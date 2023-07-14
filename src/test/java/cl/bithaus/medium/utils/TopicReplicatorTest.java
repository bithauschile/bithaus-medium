package cl.bithaus.medium.utils;

import java.io.FileInputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.Test;

import cl.bithaus.medium.message.service.MediumMessagingService;
import cl.bithaus.medium.message.service.MediumMessagingServiceConfig;
import cl.bithaus.medium.message.service.driver.kafka.MediumMessagingServiceKafkaDriver;
import cl.bithaus.medium.message.service.driver.kafka.MediumMessagingServiceKafkaDriverConfig;
import cl.bithaus.medium.utils.test.TestMessage;

public class TopicReplicatorTest {

    public TopicReplicatorTest() throws Exception {
        java.util.logging.LogManager.getLogManager().readConfiguration(new FileInputStream("conf-example/logger_unitTests.properties"));

    }


    @Test
    void testReplicate() throws Exception {


        String sourceMsgSvcConfig = "conf-example/replicator/source-service-kafka.properties";
        String targetMsgSvcConfig = "conf-example/replicator/target-service-kafka.properties";        

        String sourceTopic = "source-topic-" + new Date().getTime();
        String targetTopic = "target-topic-" + new Date().getTime();
        long beginOffset = 1;
        long endOffset = 5;

        // setup
        Map setupConfig = MapUtils.loadPropertiesFile(targetMsgSvcConfig);
        setupConfig.put("kafka.consumer.groupId", "setup");
        setupConfig.put("kafka.producer.clientId", "setupClient");
        MediumMessagingService setupSvc = new MediumMessagingService(new MediumMessagingServiceConfig(setupConfig));
        setupSvc.start();
         
        setupSvc.send(new TestMessage("Data 1"), sourceTopic, false); 
        setupSvc.send(new TestMessage("Data 2"), sourceTopic, false); 
        setupSvc.send(new TestMessage("Data 3"), sourceTopic, false); 
        setupSvc.send(new TestMessage("Data 4"), sourceTopic, false);
        setupSvc.send(new TestMessage("Data 5"), sourceTopic, false);
        setupSvc.send(new TestMessage("Data 6"), sourceTopic, false);
        setupSvc.send(new TestMessage("Data 7"), sourceTopic, false);
        
        
        

        TopicReplicator replicator = new TopicReplicator(sourceMsgSvcConfig, targetMsgSvcConfig);
        replicator.replicate(sourceTopic, targetTopic, beginOffset, endOffset);
 
        // Admin admin = getAdmin(); 
        // admin.deleteTopics(java.util.Arrays.asList(sourceTopic, targetTopic));
    
    }

    private Admin getAdmin() {
     
        Map<String,Object> adminConfigMap = new HashMap<>();
        adminConfigMap.put("bootstrap.servers", "localhost:9092");
        return Admin.create(adminConfigMap);
    }    
}


