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
package cl.bithaus.medium.message.service.driver.kafka;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;

import com.google.gson.Gson;

import cl.bithaus.medium.message.MediumMessage;
import cl.bithaus.medium.record.MediumConsumerRecord;
import cl.bithaus.medium.record.MediumProducerRecord;
import cl.bithaus.medium.utils.MessageUtils;

/**
 *
 * @author jmakuc
 */
public class MediumKafkaUtils {
    
    private static final Gson gson = MessageUtils.getMediumGson();

    
    public static ProducerRecord<String,String> fromMediumProducerRecord(MediumProducerRecord src) {
        
        Iterable<Header> iterableHeaders = getIterableHeaders(src.getHeaders());
        ProducerRecord<String,String> pr = new ProducerRecord(src.getTopic(), src.getPartition(), src.getTimestamp(), src.getKey(), src.getValue(), iterableHeaders);
        
        return pr;
    }
    
    public static MediumConsumerRecord fromKafkaConsumerRecord(ConsumerRecord<String,String> kr) {
        
        Map<String,String> headers = new HashMap<>();
        
        kr.headers().forEach((h) -> {
        
            headers.put(h.key(), new String(h.value()));
        });        
        
        return new MediumConsumerRecord(kr.key(), kr.value(), kr.topic(), headers, kr.timestamp(), kr.partition(), kr.offset());
    }
    
    public static Iterable<Header> getIterableHeaders(Map<String,String> headers) {
        
        final List<Header> i = new LinkedList<>();
        
        headers.forEach((k,v) -> {
        
            Header h = new RecordHeader(k, v!=null?v.getBytes():null);
            i.add(h);
        });
        
        return i;
    }
    
    public static <M extends MediumMessage> M toMediumMessage(ConsumerRecord<String,String> kr) throws ClassNotFoundException {
        
        Map<String,String> headers = new HashMap<>();
        
        kr.headers().forEach((h) -> {
        
            headers.put(h.key(), h.value()==null?null:new String(h.value()));
        });        
                                
        MediumMessage.Metadata metadata = new MediumMessage.Metadata();
        metadata.setKey(kr.key());
        metadata.setHeaders(headers);
        metadata.setRxTopic(kr.topic());
        metadata.setSource(headers.get(MediumMessage.HEADER_MESSAGE_SOURCE));
        metadata.setTarget(headers.get(MediumMessage.HEADER_MESSAGE_TARGET));
        metadata.setTimestamp(kr.timestamp());
        metadata.setTopicOffset(kr.offset());
        
        String messageClassStr = headers.get(MediumMessage.HEADER_MESSAGE_CLASS);
        Class<M> messageClass = (Class<M>) Class.forName(messageClassStr);
        
        M message = gson.fromJson(kr.value(), messageClass);        
        message.setMetadata(metadata);
        
        return message;        
    }
    
    public static ConsumerRecord<String,String> fromMediumMessage(MediumMessage message, String topic, int partition, long offset) {
        
        message.getMetadata().setTxTopic(topic);
        message.getMetadata().setTopicOffset(offset);        
        message.getMetadata().getHeaders().put(MediumMessage.HEADER_MESSAGE_CLASS, message.getClass().getName());
        String value = gson.toJson(message);
               
        Headers headers = new RecordHeaders(getIterableHeaders(message.getMetadata().getHeaders()));

        ConsumerRecord<String,String> r = 
                new ConsumerRecord(topic, partition, offset, message.getMetadata().getTimestamp(), TimestampType.CREATE_TIME, message.getMetadata().getKey().length(), value.length(), message.getMetadata().getKey(), value, headers, Optional.empty());
        
        
        return r;
    }
     
}
