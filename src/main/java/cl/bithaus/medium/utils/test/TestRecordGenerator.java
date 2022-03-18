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
package cl.bithaus.medium.utils.test;

import cl.bithaus.medium.message.MediumMessage;
import cl.bithaus.medium.message.MediumMessage.Metadata;
import cl.bithaus.medium.record.MediumConsumerRecord;
import cl.bithaus.medium.record.MediumProducerRecord;
import com.google.gson.Gson;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.streams.test.TestRecord;

/**
 *
 * @author jmakuc
 */
public class TestRecordGenerator {

    private static final Gson gson = new Gson();
    
    public static MediumConsumerRecord generateConsumerRecord(MediumMessage message) {
        
        Metadata md = message.getMetadata();
               
        String key = md.getKey();
        String value = gson.toJson(message);
        String topic = md.getTxTopic();        
        Long timestamp = md.getTimestamp();
        Integer partition = null;
        
        Map<String,String> headers = md.getHeaders();
        headers.put(MediumMessage.HEADER_MESSAGE_CLASS, message.getClass().getName());
        headers.put(MediumMessage.HEADER_MESSAGE_SOURCE, md.getSource());
        headers.put(MediumMessage.HEADER_MESSAGE_TARGET, md.getTarget());
                    
        return new MediumConsumerRecord(key, value, topic, headers, timestamp, partition);        
        
    }
    
    public static MediumConsumerRecord fromProducerRecord(MediumProducerRecord pr) {
        
        String key = pr.getKey();
        String value = pr.getValue();
        String topic = pr.getTopic();
        Long timestamp = pr.getTimestamp();
        Integer partition = pr.getPartition();
        
        Map<String,String> headers = new HashMap<>();
        headers.putAll(pr.getHeaders());
        
        return new MediumConsumerRecord(key, value, topic, headers, timestamp, partition);
    }      
    
    public static MediumProducerRecord fromMediumMessage(MediumMessage message, String topic) {
        
        Metadata metadata = message.getMetadata();
        metadata.setTxTopic(topic);

        Map<String, String> headers = metadata.getHeaders();
        headers.put(MediumMessage.HEADER_MESSAGE_CLASS, message.getClass().getName());
        headers.put(MediumMessage.HEADER_MESSAGE_SOURCE, metadata.getSource());
        headers.put(MediumMessage.HEADER_MESSAGE_TARGET, metadata.getTarget());

        String key = metadata.getKey();
        String value = new Gson().toJson(message);

        long timestamp = metadata.getTimestamp();


        return new MediumProducerRecord(key, value, topic, headers, timestamp);

    }
    
    public static TestRecord getStreamsTestRecord(MediumMessage message) {
        
        MediumMessage.Metadata md = message.getMetadata();
        
        TestRecord<String,String> record = new TestRecord<>(md.getKey(), gson.toJson(message)); 
        
        
        md.getHeaders().forEach((k, v) -> {
            
            record.headers().add(k, v.getBytes());
        });
        
        return record;        
    }
    
    
}
