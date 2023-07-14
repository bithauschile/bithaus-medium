package cl.bithaus.medium.utils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
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
 

        Producer<String,String> producer = ((MediumMessagingServiceKafkaDriver) this.targetMsgSvc.getDriver()).getProducer();    

        logger.info("Starting source messaging service");

        MediumMessagingServiceKafkaDriver sourceDriver = 
            (MediumMessagingServiceKafkaDriver) this.sourceMsgSvc.getDriver();

        Consumer<String,String> consumer = sourceDriver.getConsumer();


        List<PartitionInfo> partitionInfoList = consumer.partitionsFor(sourceTopic);
        
        if(partitionInfoList == null || partitionInfoList.isEmpty())
            throw new RuntimeException("Could not find topic " + sourceTopic);

        List<TopicPartition> topicPartitionList = new ArrayList<>();
        partitionInfoList.forEach((p) -> {
            logger.info("Partition: " + p.partition() + " Leader: " + p.leader() + " Replicas: " + p.replicas() + " ISR: " + p.inSyncReplicas()); 
            topicPartitionList.add(new TopicPartition(p.topic(), p.partition()));
        });

        List<TopicPartition> partitionsOk = new ArrayList<>();

        consumer.subscribe(Collections.singletonList(sourceTopic));

        do {

            logger.info("Polling to assign partitions");
            consumer.poll(Duration.ofSeconds(1));
        
            Set<TopicPartition> assignedPartitions = consumer.assignment();

            if(assignedPartitions != null && !assignedPartitions.isEmpty()) {
                logger.info("Assigned partitions: " + assignedPartitions.size());
                break;
            }
        }
        while(true); 

        

        // consumer.assign(topicPartitionList);
        consumer.seekToBeginning(topicPartitionList);
        consumer.poll(Duration.ofSeconds(1));
        

        topicPartitionList.forEach((tp) -> {

            Long beginningOffset = consumer.position(tp);

            logger.info("Beginning offset - Partition " + tp.topic() + "-" + tp.partition() + " beginning offset: " + beginningOffset);

            if(sourceInitialOffset >= beginningOffset) {
                partitionsOk.add(tp);
            }            
        });

        consumer.seekToEnd(topicPartitionList);
        consumer.poll(Duration.ofSeconds(1));

        topicPartitionList.forEach((tp) -> {

            Long endOffset = consumer.position(tp);

            logger.info("End offset - Partition " + tp.topic() + "-" + tp.partition() + " end offset: " + endOffset);

            if(sourceInitialOffset < endOffset) {

                if(!partitionsOk.contains(tp))
                    partitionsOk.add(tp);
            }            
            else {
                    
                    partitionsOk.remove(tp);
            }
        });
         
        

        if(partitionsOk.isEmpty())
            throw new RuntimeException("Initial offset " + sourceInitialOffset + " is greater than end offset of all partitions");


        Set<TopicPartition> topicParticionSet = consumer.assignment();

        if(topicParticionSet.size() < 1)
            throw new RuntimeException("No topic partitions assigned"); 

        if(topicParticionSet.size() > 1)
            throw new RuntimeException("Only 1 topic partition is supported at a time");        
                

        topicPartitionList.forEach((p) -> {

            if(!partitionsOk.contains(p))
                return;
                        
            logger.info("Seeking partition (" + p.topic() + "-" + p.partition() + ") to offset " + sourceInitialOffset);                         
            consumer.seek(p, sourceInitialOffset);
        });
            
            
        logger.info("Ready to read"); 



        logger.info("Starting targer messaging service");
        this.targetMsgSvc.start();      
        
        logger.info("Starting replication ************************");

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

                final List<Header> i = new LinkedList<>();
                cr.headers().forEach((h) -> {
                                    
                    i.add(h);                    
                });



                ProducerRecord<String,String> producerRecord = new ProducerRecord<String,String>(targetTopic, cr.partition(), cr.timestamp(), cr.key(), cr.value(), (Iterable<Header>) i);

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
