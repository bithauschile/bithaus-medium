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
package cl.bithaus.medium.message.service;

import cl.bithaus.medium.message.MediumMessage;
import cl.bithaus.medium.message.exception.MediumMessagingServiceException;
import cl.bithaus.medium.message.exception.SendToDeadLetterException;
import cl.bithaus.medium.record.MediumConsumerRecord;
import cl.bithaus.medium.utils.MessageUtils;
import io.opentelemetry.instrumentation.annotations.WithSpan;

import com.google.gson.Gson;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Medium's default deserializer and message dispatcher
 * @author jmakuc
 */
public class MediumMessagingServiceDefaultRawHandler implements MediumMessagingServiceRawHandler {
    
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected final ConcurrentHashMap<String, LinkedList<HandlerEntry>> handlers = 
            new ConcurrentHashMap<>();
    
    protected Gson gson = new Gson();
    
    public MediumMessagingServiceDefaultRawHandler() {
        
    }

    @Override
    @WithSpan
    public void onData(MediumConsumerRecord record) throws MediumMessagingServiceException, SendToDeadLetterException {
        
        String key = record.getKey();
        Map<String,String> headers = record.getHeaders();
        
        
        String messageType = headers.get(MediumMessage.HEADER_MESSAGE_CLASS);
        
        if(messageType == null) {
            
            if(logger.isTraceEnabled()) {

                logger.trace("No message type for message: " + record + ", headers: " + gson.toJson(headers) + ", message discarted...");
            }            
            
            throw new SendToDeadLetterException("No Medium message type");
            
        }
        
        LinkedList<HandlerEntry> handlersList = 
                handlers.get(messageType);

        if(handlersList == null || handlersList.size() < 1) {

            if(logger.isTraceEnabled()) {

                logger.trace("No handler for " + messageType + ", message discarted...");
            }

            return;
        }

        Class<? extends MediumMessage> messageClass = handlersList.get(0).getMessageClass();

        MediumMessage message = MessageUtils.toMedium(messageClass, record);
        

        if(logger.isTraceEnabled())
            logger.trace(String.format("Dispatching message to %s handlers. ", handlersList.size()));

        for(HandlerEntry entry : handlersList) {

            try {
                
                if(logger.isTraceEnabled())
                    logger.trace("Dispatching message " + message.getUuid() + " to " + entry.listener.getName());
                
                entry.listener.onMessage(message);
            }
            catch(Exception e) {
                logger.error("Error dispatching message " + message.getUuid() + " to " + entry.listener.getName(), e);
            }

        }            
    }
    
    @Override
    public <M extends MediumMessage> void addMessageListener(Class<M> messageType, MediumMessageListener<? super M> handler) {
    
        if(messageType == null)
            throw new IllegalArgumentException("Message type cannot by null");
        
        if(handler == null)
            throw new IllegalArgumentException("Handler cannot by null");
        
        
        LinkedList<HandlerEntry> handlerList = this.handlers.get(messageType.getName());
        
        if(handlerList == null) {
        
            handlerList = new LinkedList<HandlerEntry>(); 
            handlers.put(messageType.getName(), handlerList);
        }
                
        handlerList.add(new HandlerEntry(messageType, handler));
        
        logger.info(String.format("Message handler registered for %s (%s)", messageType.getName(), handler.toString()));
        
    }
     
    @Override
    public <M extends MediumMessage> boolean removeMessageListener(Class<M> messageType, MediumMessageListener<? super M> handler) {
        
        if(handler == null)
            throw new IllegalArgumentException("Handler cannot by null");

        LinkedList<HandlerEntry> handlerList = this.handlers.get(messageType.getName());
        
        if(handlerList == null) {
        
            return false;
        }
        
        for(HandlerEntry e : handlerList) {
                        
            if(e.listener == handler) {
                handlerList.remove(e);
                return true;
            }
        }
        
        return false;        
    }    

    

    

    
    protected static class HandlerEntry<M extends MediumMessage> {
        
        private MediumMessageListener<? super M> listener;
        private Class<M> messageClass;
        
        public HandlerEntry(Class<M> messageType, MediumMessageListener<? super M> handler) {
            
            this.listener = handler;
            this.messageClass = messageType;
        }

        public MediumMessageListener<? super M> getListener() {
            return listener;
        }

        public void setHandler(MediumMessageListener<? super M> handler) {
            this.listener = handler;
        }

        public Class<M> getMessageClass() {
            return messageClass;
        }

        public void setMessageClass(Class<M> messageClass) {
            this.messageClass = messageClass;
        }
        
    }    
}
