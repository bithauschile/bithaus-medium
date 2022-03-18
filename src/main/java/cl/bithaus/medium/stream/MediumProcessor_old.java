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
import com.google.gson.Gson;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Medium's default deserializer and message dispatcher
 * @author jmakuc
 */
public class MediumProcessor_old extends ContextualProcessor<String, String, String, String>{
    
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected final ConcurrentHashMap<String, LinkedList<ChildProcessorEntry>> handlers = 
            new ConcurrentHashMap<>();
    
    protected Gson gson = new Gson();
    
      
    public MediumProcessor_old() {
        super();
    }    
    
    @Override
    public void process(Record<String, String> record) {
        
        RecordMetadata metadata = this.context().recordMetadata().get();                
//        MediumConsumerRecord mcr = MediumStreamRecordConverter.getConsumerRecord(record, metadata);
        Map<String,String> headers = MediumStreamRecordConverter.getHeadersMap(record.headers());
        String topic = metadata.topic();
        Long offset = metadata.offset();
        
        
        try {
            
            onData(record, headers, topic, offset);             
            
        }
        catch(MediumMessagingServiceException e) {
            
            logger.error("Error processing record " + record);
        }
        catch(SendToDeadLetterException e) {
            
            logger.error("DEAD LETTER: " + record);
        }
    }  
    
    private void forward(Collection<MediumMessage> messages) {
        
        if(messages == null)
            return;
        
        messages.forEach((m) -> {
           
            context().forward(MediumStreamRecordConverter.fromMedium(m));
        });
        
    }

    
    public void onData(Record<String,String> record, Map<String,String> headers, String topic, Long offset) throws MediumMessagingServiceException, SendToDeadLetterException {
        
        String key = record.key();
        
        
        String messageType = headers.get(MediumMessage.HEADER_MESSAGE_CLASS);
        
        if(messageType == null) {
            
            if(logger.isTraceEnabled()) {

                logger.trace("No message type for message: " + record + ", headers: " + gson.toJson(headers) + ", message discarted...");
            }            
            
            throw new SendToDeadLetterException("No Medium message type");
            
        }
        
        LinkedList<ChildProcessorEntry> handlersList = 
                handlers.get(messageType);

        if(handlersList == null || handlersList.size() < 1) {

            if(logger.isTraceEnabled()) {

                logger.trace("No handler for " + messageType + ", message discarted...");
            }

            return;
        }

        Class<? extends MediumMessage> messageClass = handlersList.get(0).getInputMessageClass();

        MediumMessage message = MediumStreamRecordConverter.toMedium(messageClass, record, topic, offset);
        

        if(logger.isTraceEnabled())
            logger.trace(String.format("Dispatching message to %s handlers. ", handlersList.size()));

        for(ChildProcessorEntry entry : handlersList) {

            try {
                
                if(logger.isTraceEnabled())
                    logger.trace("Dispatching message " + message.getUuid() + " to " + entry.listener.getName());
                
                Collection<MediumMessage> output = entry.listener.onMessage(message);
                
                forward(output);
                
            }
            catch(Exception e) {
                logger.error("Error dispatching message " + message.getUuid() + " to " + entry.listener.getName(), e);
            }

        }            
    }
    
    public <I extends MediumMessage, O extends MediumMessage> void addMessageProcessor(Class<I> messageType, MediumMessageProcessor<? super I, ? super O> processor) {
    
        if(messageType == null)
            throw new IllegalArgumentException("Message type cannot by null");
        
        if(processor == null)
            throw new IllegalArgumentException("Handler cannot by null");
        
        
        LinkedList<ChildProcessorEntry> handlerList = this.handlers.get(messageType.getName());
        
        if(handlerList == null) {
        
            handlerList = new LinkedList<>(); 
            handlers.put(messageType.getName(), handlerList);
        }
                
        handlerList.add(new ChildProcessorEntry(messageType, processor));
        
        logger.info(String.format("Message handler registered for %s (%s)", messageType.getName(), processor.toString()));
        
    }
     
    
    public <I extends MediumMessage, O extends MediumMessage> boolean removeMessageProcessor(Class<I> messageType, MediumMessageProcessor<? super I, ? super O> processor) {
        
        if(processor == null)
            throw new IllegalArgumentException("Handler cannot by null");

        LinkedList<ChildProcessorEntry> handlerList = this.handlers.get(messageType.getName());
        
        if(handlerList == null) {
        
            return false;
        }
        
        for(ChildProcessorEntry e : handlerList) {
                        
            if(e.listener == processor) {
                handlerList.remove(e);
                return true;
            }
        }
        
        return false;        
    }    
        
    public <K,V> KeyValueStore<K,V> getStateStore(String name) {
        
        return (KeyValueStore<K,V>) context().getStateStore(name);
    }    
    
    public ProcessorSupplier<String,String,String,String> getSupplier() {
        
        return () -> this;        
    }    
    

    

    
    protected static class ChildProcessorEntry<I extends MediumMessage, O extends MediumMessage> {
        
        private MediumMessageProcessor<? super I, ? super O> listener;
        private Class<I> inputMessageClass;
        
        public ChildProcessorEntry(Class<I> inputMessageClass, MediumMessageProcessor<? super I, ? super O> handler) {
            
            this.listener = handler;
            this.inputMessageClass = inputMessageClass;
        }

        public MediumMessageProcessor<? super I, ? super O> getListener() {
            return listener;
        }

        public void setHandler(MediumMessageProcessor<? super I, ? super O> handler) {
            this.listener = handler;
        }

        public Class<I> getInputMessageClass() {
            return inputMessageClass;
        }
               

        public void setInputMessageClass(Class<I> inputMessageClass) {
            this.inputMessageClass = inputMessageClass;
        }
        
    }    
}
