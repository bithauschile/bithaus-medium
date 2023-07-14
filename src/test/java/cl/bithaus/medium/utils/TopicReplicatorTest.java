package cl.bithaus.medium.utils;

import static org.junit.jupiter.api.Assertions.fail;

import java.io.FileInputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cl.bithaus.medium.message.service.MediumMessagingService;
import cl.bithaus.medium.message.service.MediumMessagingServiceConfig;
import cl.bithaus.medium.message.service.driver.kafka.MediumMessagingServiceKafkaDriver;
import cl.bithaus.medium.utils.test.TestMessage;

public class TopicReplicatorTest {

    private static final Logger logger = LoggerFactory.getLogger(TopicReplicatorTest.class);

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

        try {
            replicator.replicate(sourceTopic, targetTopic, beginOffset, endOffset);
        }
        finally {
 
            Admin admin = getAdmin(); 
            admin.deleteTopics(java.util.Arrays.asList(sourceTopic, targetTopic));
        }
    
    }


    @Test
    void testReplicateOutOfRange() throws Exception {


        String sourceMsgSvcConfig = "conf-example/replicator/source-service-kafka.properties";
        String targetMsgSvcConfig = "conf-example/replicator/target-service-kafka.properties";        

        String sourceTopic = "source-topic-" + new Date().getTime();
        String targetTopic = "target-topic-" + new Date().getTime();
        long beginOffset = 100;
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
        try {
            replicator.replicate(sourceTopic, targetTopic, beginOffset, endOffset);
            fail("Should have thrown exception because beginOffset > currentMaxOffset");
        }
        catch(Exception e) {
            Admin admin = getAdmin(); 
            admin.deleteTopics(java.util.Arrays.asList(sourceTopic, targetTopic));
        }
 
    
    }

    @Test
    public void testAux() throws Exception {

        String topic = "cummins-control";

        String sourceMsgSvcConfig = "conf-example/replicator/source-service-kafka.properties";

        MediumMessagingServiceConfig config = new MediumMessagingServiceConfig(MapUtils.loadPropertiesFile(sourceMsgSvcConfig));

        MediumMessagingService svc = new MediumMessagingService(config);

        MediumMessagingServiceKafkaDriver driver = (MediumMessagingServiceKafkaDriver) svc.getDriver();

        Consumer<String,String> consumer = driver.getConsumer();

        List<PartitionInfo> partitionsInfo = consumer.partitionsFor(topic);
        List<TopicPartition> topicPartitions = new ArrayList<>();

        partitionsInfo.forEach(p -> {
            logger.info("Partition: " + p.partition() + " Leader: " + p.leader() + " Replicas: " + p.replicas() + " ISR: " + p.inSyncReplicas()); 
            topicPartitions.add(new TopicPartition(topic, p.partition()));            
        });

        consumer.assign(topicPartitions);                

        consumer.seekToBeginning(topicPartitions);
        consumer.poll(Duration.ofSeconds(1));

        topicPartitions.forEach(tp -> {

            Long position = consumer.position(tp); 
            logger.info("Partition: " + tp.partition() + " Position: " + position);
        });
        

        consumer.seekToEnd(topicPartitions);
        consumer.poll(Duration.ofSeconds(1));

        topicPartitions.forEach(tp -> {

            Long position = consumer.position(tp); 
            logger.info("Partition: " + tp.partition() + " Position: " + position);
        });

    }


    private Admin getAdmin() {
     
        Map<String,Object> adminConfigMap = new HashMap<>();
        adminConfigMap.put("bootstrap.servers", "localhost:9092");
        return Admin.create(adminConfigMap);
    }    
}


