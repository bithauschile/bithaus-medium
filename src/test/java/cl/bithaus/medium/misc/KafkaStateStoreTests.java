package cl.bithaus.medium.misc;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cl.bithaus.medium.stream.MediumProcessor;
import cl.bithaus.medium.utils.test.TestMessage;

public class KafkaStateStoreTests {

    static Logger logger = LoggerFactory.getLogger(KafkaStateStoreTests.class);

    static String storeName = "test-store";
    static String connectionTopic = "time-update";

    @Test
    public void testCrossAppCommunication() throws Exception {
        

        // Create instance 1 of the Streams application
        Properties streamsConfig1 = MediumProcessor.getDefaultSerdesProperties();
        streamsConfig1.put(StreamsConfig.APPLICATION_ID_CONFIG, "instance-1");
        streamsConfig1.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsConfig1.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfig1.put("punctuate", "true");
        

        Topology topology1 = new Topology();
        topology1.addSource("source-node", "source-topic");
        topology1.addProcessor("test-processor", TestMediumProcessor.supplier(), "source-node"); 
        // topology1.addStateStore(Stores.keyValueStoreBuilder(
        //     Stores.inMemoryKeyValueStore(storeName),
        //     Serdes.String(),
        //     Serdes.String()).withCachingDisabled(), "test-processor");

        topology1.addGlobalStore(Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore(storeName), 
            Serdes.String(), 
            Serdes.String()).withCachingDisabled().withLoggingDisabled(), 
            "store-source-node",
            Serdes.String().deserializer(),
            Serdes.String().deserializer(),
            "store-topic",
            "store-processor",   
            new ProcessorSupplier<String, String, Void, Void>() {
                @Override
                public org.apache.kafka.streams.processor.api.Processor<String, String, Void, Void> get() {
                    return new org.apache.kafka.streams.processor.api.Processor<String, String, Void, Void>() {

                        private KeyValueStore<String, String> store;

                        @Override
                        public void init(ProcessorContext<Void, Void> context) {
                            this.store = context.getStateStore(storeName);
                            logger.info("Global store initialized");
                        }

                        @Override
                        public void process(Record<String, String> record) {
                            logger.info("Global store processing: " + record.key() + " - " + record.value());
                            store.put(record.key(), record.value());
                        }

                        @Override
                        public void close() {
                            logger.info("Global store closed");
                        }
                    };
                }
            }
        );
            

        topology1.addSink("sink-node-store", "store-topic", "test-processor");
        topology1.addSink("sink-node-connection", connectionTopic, "test-processor");
 


        logger.info("Topology1 OK");
        logger.info(topology1.describe().toString());
        KafkaStreams streams1 = new KafkaStreams(topology1, streamsConfig1);
        logger.info("Streams1 OK");
        streams1.start();
        logger.info("Streams1 start");

        

        logger.info("Creating instance 2...");
        // Create instance 2 of the Streams application
        Properties streamsConfig2 = MediumProcessor.getDefaultSerdesProperties();
        streamsConfig2.put(StreamsConfig.APPLICATION_ID_CONFIG, "instance-2");
        streamsConfig2.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsConfig2.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);


        Topology topology2 = new Topology();
        topology2.addSource("source-node", connectionTopic);
        topology2.addProcessor("test-processor", TestMediumProcessor.supplier(), "source-node"); 
        // topology2.addStateStore(Stores.keyValueStoreBuilder(
        //     Stores.inMemoryKeyValueStore(storeName),
        //     Serdes.String(),
        //     Serdes.String()).withCachingDisabled(), "test-processor");

        topology2.addGlobalStore(Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore(storeName), 
            Serdes.String(), 
            Serdes.String()).withCachingDisabled().withLoggingDisabled(), 
            "store-source-node",
            Serdes.String().deserializer(),
            Serdes.String().deserializer(),
            "store-topic",
            "store-processor",   
            new ProcessorSupplier<String, String, Void, Void>() {
                @Override
                public org.apache.kafka.streams.processor.api.Processor<String, String, Void, Void> get() {
                    return new org.apache.kafka.streams.processor.api.Processor<String, String, Void, Void>() {

                        private KeyValueStore<String, String> store;

                        @Override
                        public void init(ProcessorContext<Void, Void> context) {
                            this.store = context.getStateStore(storeName);
                            logger.info("Global store initialized");
                        }

                        @Override
                        public void process(Record<String, String> record) {
                            logger.info("Global store processing: " + record.key() + " - " + record.value());
                            store.put(record.key(), record.value());
                        }

                        @Override
                        public void close() {
                            logger.info("Global store closed");
                        }
                    };
                }
            }
        );


        logger.info("Topology2 OK");
        
        KafkaStreams streams2 = new KafkaStreams(topology2, streamsConfig2);
        logger.info("Streams2 OK");
        streams2.start();
        logger.info("Streams2 start");


        Thread.sleep(Long.MAX_VALUE);

        
    }



    public static class TestMediumProcessor extends MediumProcessor<TestMessage,TestMessage> {

        private KeyValueStore<String,String> stateStore;
        private Map<String, Object> configMap;

        @Override
        public void init(Map<String, Object> configMap) {
            this.configMap = configMap;
            this.stateStore = this.getStateStore(storeName);

            if(configMap.get("punctuate") == null || !configMap.get("punctuate").toString().equalsIgnoreCase("true"))
                return;

            logger.info(configMap.get(StreamsConfig.APPLICATION_ID_CONFIG) + " Initializing puntuation...");

            final KeyValueStore<String,String> store = this.stateStore;
            final ProcessorContext<String,String> c = this.context();
            final MediumProcessor p = this; 

            this.context().schedule(Duration.ofMillis(10000), PunctuationType.WALL_CLOCK_TIME, new Punctuator() {
                @Override
                public void punctuate(long timestamp) {
                    
                    Date time = new Date();

                    logger.info(configMap.get(StreamsConfig.APPLICATION_ID_CONFIG) + " Puntuate: time=" + time.toString());
                    // store.put("time", time.toString());
                    // store.flush();

                    TestMessage message = new TestMessage(time.toString());
                    message.getMetadata().setKey("time");
                    // p.forward(message);
                }
            });
        }
        
        @Override
        public Collection<TestMessage> onMessage(TestMessage message) {
            
            String time = this.stateStore.get("time");
            
            logger.info(configMap.get(StreamsConfig.APPLICATION_ID_CONFIG) + " state-time=" + time + " msgTime=" + message.getData());
            
            
            
            return Collections.emptyNavigableSet();
        }
        
        public static ProcessorSupplier<String,String,String,String> supplier() {
            
            return TestMediumProcessor::new;
            
        }

        @Override
        public Class<TestMessage> getInputMessageClass() {
            return TestMessage.class;
        }


        
    }    
}
