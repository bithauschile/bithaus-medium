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
package cl.bithaus.medium.stream;

import cl.bithaus.medium.message.MediumMessage;
import cl.bithaus.medium.utils.test.TestMessage;
import com.google.gson.Gson;
import java.io.FileInputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author jmakuc
 */
public class MediumProcessorTest {
        
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    
    private Topology topology;
    private MediumProcessor mediumProcessor;
    private TopologyTestDriver testDriver;
    
    private TestInputTopic<String,String> inputTopic1;
    private TestInputTopic<String,String> inputTopic2;
    private TestOutputTopic<String,String> outputTopic1;
    private TestOutputTopic<String,String> outputTopic2;

    private static String storeName = "dlwr-master-table";
    
    private static final Collection<MediumProcessor.BadData> badDataSet = new HashSet<>();
    private static final Collection<MediumProcessor.BadData> deadLetterSet = new HashSet<>();
    
    private Gson gson = new Gson();
    
    public MediumProcessorTest() throws Exception {
        java.util.logging.LogManager.getLogManager().readConfiguration(new FileInputStream("conf-example/logger_unitTests.properties"));
    }
    
    @BeforeEach
    public void setUp() {
                
        
        // Topology definition
        // ----------------------------------------------
        topology = new Topology();
        
        topology.addSource("source-node-1", "input-topic-1");         
        topology.addSource("source-node-2", "input-topic-2");         
        
        topology.addProcessor("medium-processor", TestMediumProcessor.supplier(), "source-node-1");
        
        topology.addSink("sink-node-1", "sink-topic-1", "medium-processor");
        topology.addSink("sink-node-2", "sink-topic-2", "source-node-2");
        
        topology.addStateStore(Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(storeName),
                        Serdes.String(),
                        Serdes.String()), "medium-processor");
        
        
        
        // Topology testing elements
        // ----------------------------------------------        
        testDriver = new TopologyTestDriver(topology, MediumProcessor.getDefaultSerdesProperties());
        
        
        inputTopic1 = testDriver.createInputTopic("input-topic-1", new StringSerializer(), new StringSerializer()); 
        inputTopic2 = testDriver.createInputTopic("input-topic-2", new StringSerializer(), new StringSerializer()); 
        outputTopic1 = testDriver.createOutputTopic("sink-topic-1", new StringDeserializer(), new StringDeserializer()); 
        outputTopic2 = testDriver.createOutputTopic("sink-topic-2", new StringDeserializer(), new StringDeserializer()); 
        
        badDataSet.clear();
        deadLetterSet.clear();
    }
    
    @Test
    public void testNoData() throws Exception {
    
        inputTopic2.pipeInput("key", "value");
        
        Assertions.assertTrue(badDataSet.size() == 0);
        Assertions.assertTrue(deadLetterSet.size() == 0);
        
        Assertions.assertFalse(outputTopic2.isEmpty());

    }
    
    @Test
    public void testBadData() throws Exception {
        
        KeyValueStore<String,String> kvs = testDriver.getKeyValueStore(storeName);
        inputTopic1.pipeInput("key", "value");
        Assertions.assertTrue(kvs.approximateNumEntries() == 0L);
        
        Assertions.assertTrue(badDataSet.size() == 0);
        Assertions.assertTrue(deadLetterSet.size() == 1);
    } 
    
    @Test
    public void testProcess() {
      
        KeyValueStore<String,String> kvs = testDriver.getKeyValueStore(storeName);
        
        // another test message should be ignored
        AnotherTestMessage anotherTestMessage = new AnotherTestMessage("lala");
        anotherTestMessage.getMetadata().setKey("the-key");
        anotherTestMessage.getMetadata().setTxTopic("");
        inputTopic1.pipeInput(MediumStreamRecordConverter.fromMediumTestRecord(anotherTestMessage));

        
        Assertions.assertTrue(kvs.approximateNumEntries() == 0L);

        
        
        
        // ok, let's go..
        
        TestMessage message = new TestMessage("hola");
        message.getMetadata().setKey("the-key");
        message.getMetadata().setTxTopic("");
        
        TestRecord<String,String> tr = MediumStreamRecordConverter.fromMediumTestRecord(message);
        
        inputTopic1.pipeInput(tr);
        Assertions.assertTrue(kvs.approximateNumEntries() == 1L);
        Assertions.assertEquals(message.getData(), kvs.get(message.getMetadata().getKey()));
        
        Assertions.assertTrue(badDataSet.size() == 0);
        Assertions.assertTrue(deadLetterSet.size() == 0);
        
        TestMessage tmcheck = gson.fromJson(outputTopic1.readValue(), TestMessage.class);
        
        Assertions.assertEquals(message.getData() + "-OK", tmcheck.getData());
        
        Assertions.assertTrue(outputTopic2.isEmpty());
        
        
        TestMessage2 message2 = new TestMessage2("hola2");
        message2.getMetadata().setKey("the-key");
        message2.getMetadata().setTxTopic("");
        
        TestRecord<String,String> tr2 = MediumStreamRecordConverter.fromMediumTestRecord(message2);
        
        inputTopic1.pipeInput(tr2);
        
        // in this second iteration, approximateNumEntries honours its name
        // and approximates 2 entries :-/
        int numEntries = 0;
        KeyValueIterator i = kvs.all();
        while(i.hasNext()) {
            
            numEntries++;
            i.next();
        }
        
        Assertions.assertTrue(numEntries == 1);
        Assertions.assertEquals(message2.getData(), kvs.get(message2.getMetadata().getKey()));
        
        Assertions.assertTrue(badDataSet.size() == 0);
        Assertions.assertTrue(deadLetterSet.size() == 0);
        
        TestMessage tmcheck2 = gson.fromJson(outputTopic1.readValue(), TestMessage2.class);
        
        Assertions.assertEquals(message2.getData() + "-OK", tmcheck2.getData());
        
        Assertions.assertTrue(outputTopic2.isEmpty());
    }

    
    public static class TestMediumProcessor extends MediumProcessor<TestMessage,TestMessage> {

        private KeyValueStore<String,String> stateStore;
         

        @Override
        public void init(Map<String, Object> configMap) {
            this.stateStore = this.getStateStore(storeName);
            
            this.setBadDataConsumer((t) -> { badDataSet.add(t); });
            this.setDeadLetterConsumer((t) -> { deadLetterSet.add(t); });
        }
        
        @Override
        public Collection<TestMessage> onMessage(TestMessage message) {
            
            this.stateStore.put(message.getMetadata().getKey(), message.getData());
            this.stateStore.flush();
            
            message.setData(message.getData() + "-OK");
            
            return Collections.singleton(message);
        }
        
        public static ProcessorSupplier<String,String,String,String> supplier() {
            
            return TestMediumProcessor::new;
            
        }

        @Override
        public Class<TestMessage> getInputMessageClass() {
            return TestMessage.class;
        }


        
    }


    public static class AnotherTestMessage extends MediumMessage {

         private static final AtomicLong serializer = new AtomicLong();

        private String data;

        public AnotherTestMessage() {

            super("" + serializer.incrementAndGet());
            initMetadata();
        }


        public AnotherTestMessage(String data) {

            this();
            this.data = data;
            initMetadata();
        }

        private void initMetadata() {

            this.getMetadata().setKey("key-" + getUuid());
            this.getMetadata().setSource("source-system");
            this.getMetadata().setTarget("target-system");
            this.getMetadata().setTimestamp(new Date().getTime());

        }

        public String getData() {
            return data;
        }

        public void setData(String data) {
            this.data = data;
        }



    }

    
    public static class TestMessage2 extends TestMessage {

        public TestMessage2(String data) {
            super(data);
        }
    }
}
