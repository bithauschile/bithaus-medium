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
import cl.bithaus.medium.message.service.driver.kafka.MediumKafkaUtils;
import cl.bithaus.medium.utils.MessageUtils;
import cl.bithaus.medium.record.MediumConsumerRecord;
import cl.bithaus.medium.record.MediumProducerRecord;
import com.google.gson.Gson;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.test.TestRecord;

/**
 *
 * @author jmakuc
 */
public class MediumStreamRecordConverter {
        
    private static final Gson gson = new Gson();
        
    public static MediumConsumerRecord getConsumerRecord(Record r, RecordMetadata metadata) {
        
        String key = r.key() != null?r.key().toString():null;
        String value = r.value() != null?r.value().toString():null;
        long timestamp = r.timestamp();
        Map<String,String> headers = new HashMap<>();
        String topic = metadata!=null?metadata.topic():null;
        Integer partition = metadata!=null?metadata.partition():null;
        
        r.headers().forEach((h) -> {
        
            headers.put(h.key(), new String(h.value()));
        });                                
        
        return new MediumConsumerRecord(key, value, topic, headers, timestamp, partition);
        
    }
    

    /**
     * Deserialize payload and headers into a formal Medium messaage.
     * @param <M> Medium Message class
     * @param messageClass Medium message class
     * @param record incoming record
     * @param topic topic from this message from received from
     * @param offset offset in the topic for this message
     * @return deserialized Medium message
     */
    public static <M extends MediumMessage> M toMedium(Class<M> messageClass, Record<String,String> record, String topic, Long offset) {
        
        Map<String,String> headers = new HashMap<>();
        
        record.headers().forEach((h) -> {
            headers.put(h.key(), h.value()==null?null:new String(h.value()));
        });
        
        MediumMessage.Metadata metadata = new MediumMessage.Metadata();
        metadata.setKey(record.key());
        metadata.setHeaders(headers);
        metadata.setRxTopic(topic);
        metadata.setSource(headers.get(MediumMessage.HEADER_MESSAGE_SOURCE));
        metadata.setTarget(headers.get(MediumMessage.HEADER_MESSAGE_TARGET));
        metadata.setTimestamp(record.timestamp());
        metadata.setTopicOffset(offset);
        
        M message = gson.fromJson(record.value(), messageClass);        
        message.setMetadata(metadata);
        
        return message;
                
    }    
    
    public static Record<String,String> fromMedium(MediumMessage message) {
        
        if(message == null)
            return null;
        
        MediumMessage.Metadata md = message.getMetadata();
        
        if(md.getKey() == null)
            throw new NullPointerException("Metadata.key cannot be null");
        
        if(md.getTimestamp() == null)
            md.setTimestamp(new Date().getTime());
        
        Record<String,String> record = new Record<>(md.getKey(), gson.toJson(message), md.getTimestamp());
        
        md.getHeaders().forEach((k, v) -> {
            
            record.headers().add(k, v.getBytes());
        });
        
        return record;
    }
    
    public static TestRecord<String,String> fromMediumTestRecord(MediumMessage message) {
        
        MediumProducerRecord mpr = MessageUtils.fromMedium(message);
        return new TestRecord<>(MediumKafkaUtils.fromMediumProducerRecord(mpr));        
    }     
    
    public static <M extends MediumMessage> M toMedium(Class<M> messageClass, TestRecord<String,String> record, String topic, Long offset) { 
        
        Map<String,String> headers = new HashMap<>();
        
        record.headers().forEach((h) -> {
            headers.put(h.key(), new String(h.value()));
        });
        
        MediumMessage.Metadata metadata = new MediumMessage.Metadata();
        metadata.setKey(record.key());
        metadata.setHeaders(headers);
        metadata.setRxTopic(topic);
        metadata.setSource(headers.get(MediumMessage.HEADER_MESSAGE_SOURCE));
        metadata.setTarget(headers.get(MediumMessage.HEADER_MESSAGE_TARGET));
        metadata.setTimestamp(record.timestamp());
        metadata.setTopicOffset(offset);
        
        M message = gson.fromJson(record.value(), messageClass);        
        message.setMetadata(metadata);
        
        return message;        
    }
    
    public static Map<String,String> getHeadersMap(Headers headers) {
        
        Map<String,String> map = new HashMap<>();
        
        headers.forEach((h) -> {
            map.put(h.key(), h.value()==null?null:new String(h.value()));
        });
        
        return map;
    }
    
}
