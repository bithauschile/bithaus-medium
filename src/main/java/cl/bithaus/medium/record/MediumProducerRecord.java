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
package cl.bithaus.medium.record;

import java.util.HashMap;
import java.util.Map;

/**
 * A message being produced into the underlying Kafka.
 * Abstraction layer to handle easily both {@link org.apache.kafka.clients.producer.ProducerRecord}
 * and {@link org.apache.kafka.connect.connector.SourceRecord}
 * @author jmakuc
 */
public class MediumProducerRecord {
    
    private final String key;
    private final String value;
    private final String topic;
    private final Map<String, String> headers;
    private final Integer partition;
    private final Long timestamp;    

    /**
     * Constructor
     * @param key key to be used for balancing the message across partitions
     * @param value payload
     * @param topic topic to publish the message to
     */
    public MediumProducerRecord(String key, String value, String topic) {
        
        this.key = key;
        this.value = value;
        this.topic = topic;
        this.partition = null;
        this.headers = new HashMap<>();
        this.timestamp = null;
    }
    
    
    /**
     * Constructor
     * @param key key to be used for balancing the message across partitions
     * @param value payload
     * @param topic topic to publish the message to
     * @param headers Headers of the message     
     */
    public MediumProducerRecord(String key, String value, String topic, Map<String,String> headers) {
        
        this.key = key;
        this.value = value;
        this.topic = topic;
        this.partition = null;
        this.headers = headers;
        this.timestamp = null;
    }
    
    
    /**
     * Constructor
     * @param key key to be used for balancing the message across partitions
     * @param value payload
     * @param topic topic to publish the message to
     * @param headers Headers of the message     
     * @param timestamp generation timestmap of the information inside the message. 
     *                  If provided, implies {@link TimestampType.CREATE_TIME}. If not, underlying Kafka will assign timestamp.
     */
    public MediumProducerRecord(String key, String value, String topic, Map<String,String> headers, Long timestamp) {
        
        this.key = key;
        this.value = value;
        this.topic = topic;
        this.headers = headers;
        this.timestamp = timestamp;
        this.partition = null;
    }    
    
    /**
     * Constructor
     * @param key key to be used for balancing the message across partitions
     * @param value payload
     * @param topic topic to publish the message to
     * @param headers Headers of the message     
     * @param timestamp generation timestmap of the information inside the message. 
     *                  If provided, implies {@link TimestampType.CREATE_TIME}. If not, underlying Kafka will assign timestamp.
     * @param partition Forced partition to be used
     */
    public MediumProducerRecord(String key, String value, String topic, Map<String,String> headers, Long timestamp, Integer partition) {
        
        this.key = key;
        this.value = value;
        this.topic = topic;
        this.headers = headers;
        this.timestamp = timestamp;
        this.partition = partition;
    }        
     
    
    /**
     * key to be used for balancing the message across partitions
     * @return 
     */
    public String getKey() {
        return key;
    }

    /**
     * Payload, the {@link cl.bithaus.medium.message.MediumMessage} serialized as JSON vía {@link com.google.gson.Gson}.
     * @return 
     */
    public String getValue() {
        return value;
    }

    /**
     * Topic where to write the message to
     * @return 
     */
    public String getTopic() {
        return topic;
    }

    /**
     * Forced partition to write the message to. (OPTIONAL)
     * @return 
     */
    public Integer getPartition() {
        return partition;
    }

    /**
     * Headers of the message. Contains the original message class name for deserialization.
     * @return 
     */
    public Map<String, String> getHeaders() {
        return headers;
    }

    /**
     * Timestamp of the message at the application layer. (OPTIONAL)
     * @return 
     */
    public Long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "MediumMessagingServiceProducerRecord{" + "key=" + key + ", value=" + value + ", topic=" + topic + ", headers=" + headers + ", partition=" + partition + ", timestamp=" + timestamp + '}';
    }
    
    
    
}
