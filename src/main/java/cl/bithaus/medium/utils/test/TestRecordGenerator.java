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
import cl.bithaus.medium.message.service.driver.MediumMessagingServiceConsumerRecord;
import com.google.gson.Gson;
import java.util.Map;

/**
 *
 * @author jmakuc
 */
public class TestRecordGenerator {

    private static final Gson gson = new Gson();
    
    public static MediumMessagingServiceConsumerRecord generateConsumerRecord(MediumMessage message) {
        
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
                    
        return new MediumMessagingServiceConsumerRecord(key, value, topic, headers, timestamp, partition);        
        
    }
}
