package cl.bithaus.medium.utils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cl.bithaus.medium.message.service.MediumMessagingService;
import cl.bithaus.medium.message.service.MediumMessagingServiceConfig;
import cl.bithaus.medium.message.service.driver.kafka.MediumMessagingServiceKafkaDriver;

public class TopicReplicator {
    
    private static final Logger logger = LoggerFactory.getLogger(TopicReplicator.class);

    private MediumMessagingService sourceMsgSvc;
    private MediumMessagingService targetMsgSvc;

    public TopicReplicator(MediumMessagingService sourceMsgSvc, MediumMessagingService targetMsgSvc) {

        if(sourceMsgSvc == null)
            throw new IllegalArgumentException("Source messaging service cannot be null");

        if(targetMsgSvc == null)    
            throw new IllegalArgumentException("Target messaging service cannot be null");

        this.sourceMsgSvc = sourceMsgSvc;
        this.targetMsgSvc = targetMsgSvc;
    }

    public TopicReplicator(MediumMessagingServiceConfig sourceConfig, MediumMessagingServiceConfig targetConfig) throws Exception {

        if(sourceConfig == null)
            throw new IllegalArgumentException("Source messaging service config cannot be null");   

        if(targetConfig == null)
            throw new IllegalArgumentException("Target messaging service config cannot be null");

        logger.info("Creating source messaging service");
        this.sourceMsgSvc = new MediumMessagingService(sourceConfig);

        logger.info("Creating target messaging service");
        this.targetMsgSvc = new MediumMessagingService(targetConfig);
    }

    public TopicReplicator(Map sourceConfig, Map targetConfig) throws Exception {

        if(sourceConfig == null)
            throw new IllegalArgumentException("Source messaging service config cannot be null");   

        if(targetConfig == null)
            throw new IllegalArgumentException("Target messaging service config cannot be null");

        logger.info("Creating source messaging service");
        this.sourceMsgSvc = new MediumMessagingService(new MediumMessagingServiceConfig(sourceConfig));

        logger.info("Creating target messaging service");
        this.targetMsgSvc = new MediumMessagingService(new MediumMessagingServiceConfig(targetConfig));
    }

    public TopicReplicator(String sourceConfigFilename, String targetConfigFilename) throws Exception {

        if(sourceConfigFilename == null)
            throw new IllegalArgumentException("Source messaging service config filename cannot be null");   

        if(targetConfigFilename == null)
            throw new IllegalArgumentException("Target messaging service config filename cannot be null");

        logger.info("Creating source messaging service");
        Map sourceConfigMap = MapUtils.loadPropertiesFile(sourceConfigFilename);
        this.sourceMsgSvc = new MediumMessagingService(new MediumMessagingServiceConfig(sourceConfigMap));    

        logger.info("Creating target messaging service");
        Map targetConfigMap = MapUtils.loadPropertiesFile(targetConfigFilename);
        this.targetMsgSvc = new MediumMessagingService(new MediumMessagingServiceConfig(targetConfigMap));    
    }

    public void replicate(final String sourceTopic, final String targetTopic, final Long sourceInitialOffset, final Long sourceEndOffset) throws Exception {

        logger.info("Starting targer messaging service");
        this.targetMsgSvc.start();        

        Producer<String,String> producer = ((MediumMessagingServiceKafkaDriver) this.targetMsgSvc.getDriver()).getProducer();    

        logger.info("Starting source messaging service");

        MediumMessagingServiceKafkaDriver sourceDriver = 
            (MediumMessagingServiceKafkaDriver) this.sourceMsgSvc.getDriver();

        Consumer<String,String> consumer = sourceDriver.getConsumer();

        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean ok = new AtomicBoolean(false);

        List<PartitionInfo> partitionInfo = consumer.partitionsFor(sourceTopic);
        
        if(partitionInfo == null || partitionInfo.isEmpty())
            throw new RuntimeException("Could not find topic " + sourceTopic);

        List<TopicPartition> topicParticionList = new ArrayList<>();
        partitionInfo.forEach((pi) -> {
            topicParticionList.add(new TopicPartition(pi.topic(), pi.partition()));
        });

        consumer.assign(topicParticionList);
        
        topicParticionList.forEach((p) -> {
                        
            logger.info("Seeking partition (" + p.topic() + "-" + p.partition() + ") to offset " + sourceInitialOffset);                         
            consumer.seek(p, sourceInitialOffset);
        });

        
        // consumer.subscribe(java.util.Arrays.asList(sourceTopic), new org.apache.kafka.clients.consumer.ConsumerRebalanceListener() {

        //     @Override
        //     public void onPartitionsRevoked(java.util.Collection<org.apache.kafka.common.TopicPartition> partitions) {
        //         logger.error("Partitions revoked");
        //     }

        //     @Override
        //     public void onPartitionsAssigned(java.util.Collection<org.apache.kafka.common.TopicPartition> partitions) {
        //         logger.info("Partitions assigned");

        //         partitions.forEach((p) -> {
                            
        //             logger.info("Seeking partition (" + p.topic() + "-" + p.partition() + ") to offset " + sourceInitialOffset);                         
        //             consumer.seek(p, sourceInitialOffset);
        //         });

        //         ok.set(true);
        //         latch.countDown();
        //     }
        // });

        // consumer.poll(Duration.ofSeconds(1));

        // latch.await();

        // if(!ok.get())
            // throw new RuntimeException("Could not subscribe to topic " + sourceTopic); 



        Set<TopicPartition> topicParticionSet = consumer.assignment();

        if(topicParticionSet.size() < 1)
            throw new RuntimeException("No topic partitions assigned"); 

        if(topicParticionSet.size() > 1)
            throw new RuntimeException("Only 1 topic partition is supported at a time");


        logger.info("Ready to read"); 

        while(true) {

            logger.info("Polling"); 

            ConsumerRecords<String,String> records = consumer.poll(Duration.ofSeconds(10));

            if(records == null || records.isEmpty()) {
                logger.info("No records found");
                break;
            }


            logger.info("Replicating " + records.count() + " records");

            AtomicBoolean reachedEnd = new AtomicBoolean(false);

            
            records.forEach((cr) -> {

                if(reachedEnd.get())
                    return;

                if(cr.offset() < sourceInitialOffset) {
                    return;
                }

                if(sourceEndOffset != null && cr.offset() > sourceEndOffset) {
                    logger.info("partition: " + cr.partition() + " - Reached end offset " + sourceEndOffset);
                    reachedEnd.set(true);
                    return;
                }

                ProducerRecord<String,String> producerRecord = new ProducerRecord<String,String>(targetTopic, cr.key(), cr.value());
                producer.send(producerRecord);
                
            });

            
            if(reachedEnd.get()) {
                logger.info("Reached end offset " + sourceEndOffset);
                break;
            }                
        }

        producer.flush();

        logger.info("DONE"); 
        
    }


    /**
     * Arguments: sourceConfigFilename targetConfigFilename sourceTopic targetTopic beginOffset [endOffset]
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
 
        if(args.length < 4)
            showUsage();

        String sourceConfigFilename = args[0];
        String targetConfigFilename = args[1];
        String sourceTopic = args[2];
        String targetTopic = args[3];
        Long beginOffset = Long.parseLong(args[4]);
        Long endOffset = null;

        if(args.length > 5)
            endOffset = Long.parseLong(args[5]);

        logger.info("Creating source messaging service from " + sourceConfigFilename);
        Map sourceConfigMap = MapUtils.loadPropertiesFile(sourceConfigFilename);
        MediumMessagingService sourceMsgSvc = new MediumMessagingService(new MediumMessagingServiceConfig(sourceConfigMap));

        logger.info("Creating target messaging service from " + targetConfigFilename);
        Map targetConfigMap = MapUtils.loadPropertiesFile(targetConfigFilename);
        MediumMessagingService targetMsgSvc = new MediumMessagingService(new MediumMessagingServiceConfig(targetConfigMap));

        TopicReplicator replicator = new TopicReplicator(sourceMsgSvc, targetMsgSvc);
        replicator.replicate(sourceTopic, targetTopic, beginOffset, endOffset);

        
        logger.info("Done");
    }

    private static void showUsage() {
        logger.info("Usage: java -jar <jarfile> <sourceConfigFilename> <targetConfigFilename> <targetTopic> <beginOffset> [endOffset]");
        System.exit(1);
    } 
}
