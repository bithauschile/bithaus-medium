/*
 * Copyright (c) BitHaus Software Factory & Boutique (Chile)
 * All rights reserved. www.bithaus.cl.
 * 
 * All rights to this product are owned by BitHaus Chile and may only by used 
 * under the terms of its associated license document. 
 * You may NOT copy, modify, sublicense or distribute this source file or 
 * portions of it unless previously authorized by writing by BitHaus Chile.
 * In any event, this notice must always be included verbatim with this file.
 * 
 */
package cl.bithaus.medium.stream;

import cl.bithaus.medium.message.MediumMessage;
import cl.bithaus.medium.message.exception.MediumMessagingServiceException;
import cl.bithaus.medium.message.exception.SendToDeadLetterException;
import cl.bithaus.medium.utils.MessageUtils;
import cl.bithaus.medium.utils.StatsD;
import io.opentelemetry.instrumentation.annotations.WithSpan;

import com.google.gson.Gson;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Medium's default deserializer and message dispatcher
 * @author jmakuc
 * @param <I> Input message type
 * @param <O> Output message type
 */
public abstract class MediumProcessor<I extends MediumMessage, O extends MediumMessage> 
        extends ContextualProcessor<String, String, String, String> {
    
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected Gson gson = MessageUtils.getMediumGson();
    
    private Consumer<BadData> badDataConsumer;
    private Consumer<BadData> deadLetterConsumer;

    private StatsD statsD;
    
      
    public MediumProcessor() {
        super();
    }    
    
    
    @Override
    public void init(ProcessorContext<String, String> context) {

        super.init(context);
        
        try {
            this.init(context.appConfigs());
        }
        catch(Exception e) {
            throw new RuntimeException("Error initializing MediumProcessor: " + e, e);
        }
    } 
    
    @WithSpan
    @Override
    public void process(Record<String, String> record) {
        
        RecordMetadata metadata = this.context().recordMetadata().get();                
        Map<String,String> headers = MediumStreamRecordConverter.getHeadersMap(record.headers());
        String topic = metadata.topic();
        Long offset = metadata.offset();
        Integer partition = metadata.partition();
        
        
        try {
            
            onData(record, headers, topic, partition, offset);             
            
        }
        catch(MediumMessagingServiceException e) {
            
            logger.error("Error processing record " + record, e);

            this.statsD.recordException("Error processing record", e);
            
            if(this.badDataConsumer != null) {
                
                this.badDataConsumer.accept(new BadData(record, e));
            }
        }
        catch(SendToDeadLetterException e) {
            
            logger.error("DEAD LETTER: " + record);
            
            if(this.deadLetterConsumer != null) {
                
                this.deadLetterConsumer.accept(new BadData(record, e));
            }
            
        }
    }      
    
    @WithSpan
    private void onData(Record<String,String> record, Map<String,String> headers, String topic, Integer partition, Long offset) throws MediumMessagingServiceException, SendToDeadLetterException {
               
        Class<? extends I> expectedMessageClass = this.getInputMessageClass();
        String incommingMessageClassString = headers.get(MediumMessage.HEADER_MESSAGE_CLASS);
        
        if(incommingMessageClassString == null) {
            
            if(logger.isTraceEnabled()) {

                logger.trace("No message type for message: " + record + ", headers: " + gson.toJson(headers) + ", message discarted...");
            }            
            
            throw new SendToDeadLetterException("No Medium message type");
            
        }
        
        Class<? extends MediumMessage> incommingMessageClass = null;
        try {
        
            incommingMessageClass = (Class<? extends MediumMessage>) Class.forName(incommingMessageClassString);
            
        }
        catch(ClassNotFoundException e) {
            
            throw new SendToDeadLetterException("Class " + incommingMessageClassString + " not found");
        }         
        
        if(!expectedMessageClass.isAssignableFrom(incommingMessageClass))
            return;
        
        
        I message = MediumStreamRecordConverter.toMedium(expectedMessageClass, record, topic, partition, offset);
        if(logger.isTraceEnabled())
            logger.trace("Dispatching message " + message.getUuid() + " to message handler method"); 
        
        Collection<O> output = null;
        try {
            
            output = onMessage(message);
        }
        catch(MediumMessagingServiceException e) {
            
            throw e;
        }
        catch(Exception e) {
            
            throw new MediumMessagingServiceException("Error processing medium message: " + e.getMessage(), e);
        }
        
        if(output != null) {
            output.forEach((m) -> {

                context().forward(MediumStreamRecordConverter.fromMedium(m));
            });            
        }
                
    }
    
    public <K,V> KeyValueStore<K,V> getStateStore(String name) {
        
        return context().getStateStore(name);
    }
    
    public void setBadDataConsumer(Consumer<BadData> consumer) {
        
        this.badDataConsumer = consumer;
    }
    
    public void setDeadLetterConsumer(Consumer<BadData> consumer) {
        
        this.deadLetterConsumer = consumer;
    }
    
    public static Properties getDefaultSerdesProperties() {
        
        Properties p = new Properties();
        p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        return p;
    }
    
    public abstract void init(Map<String,Object> configMap) throws MediumProcessorException;
    
    public abstract Collection<O> onMessage(I message) throws MediumMessagingServiceException;     
     
    public abstract Class<? extends I> getInputMessageClass();
    
    public static class BadData {
        
        private final Record<String,String> record;
        private final Throwable cause;
        
        public BadData(Record<String,String> record, Throwable cause) {
            
            this.record = record;
            this.cause = cause;
        }

        public Record<String, String> getRecord() {
            return record;
        }

        public Throwable getCause() {
            return cause;
        }
        
        
    }
}
