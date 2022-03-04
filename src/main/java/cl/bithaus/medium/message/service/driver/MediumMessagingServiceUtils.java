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
package cl.bithaus.medium.message.service.driver;

import cl.bithaus.medium.record.MediumProducerRecord;
import cl.bithaus.medium.record.MediumConsumerRecord;
import cl.bithaus.medium.message.MediumMessage;
import com.google.gson.Gson;
import java.util.HashMap;
import java.util.Map;

/**
 * Conversion utilities
 * @author jmakuc
 */
public class MediumMessagingServiceUtils {
        
    private static final Gson gson = new Gson();
    
    public static MediumProducerRecord fromMedium(MediumMessage message) {
        
        String key = message.getMetadata().getKey();
        String value = gson.toJson(message);
        String topic = message.getMetadata().getTxTopic();
        Map<String,String> headers = new HashMap<>(message.getMetadata().getHeaders());
        Long timestamp = message.getMetadata().getTimestamp();        
        
        
        return new MediumProducerRecord(key, value, topic, headers, timestamp);
    }
    
    /**
     * Deserialize payload and headers into a formal Medium messaage.
     * @param <M> Medium Message class
     * @param messageClass Medium message class
     * @param record incoming record
     * @return deserialized Medium message
     */
    public static <M extends MediumMessage> M toMedium(Class<M> messageClass, MediumConsumerRecord record) {
        
        Map<String,String> headers = record.getHeaders();
        
        MediumMessage.Metadata metadata = new MediumMessage.Metadata();
        metadata.setKey(record.getKey());
        metadata.setHeaders(headers);
        metadata.setRxTopic(record.getTopic());
        metadata.setSource(headers.get(MediumMessage.HEADER_MESSAGE_SOURCE));
        metadata.setTarget(headers.get(MediumMessage.HEADER_MESSAGE_TARGET));
        metadata.setTimestamp(record.getTimestamp());
        
        M message = gson.fromJson(record.getValue(), messageClass);        
        message.setMetadata(metadata);
        
        return message;
                
    }
    
}
