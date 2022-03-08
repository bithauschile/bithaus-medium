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

/**
 * Handler for raw record from the underlying Kafka
 * @author jmakuc
 */
public interface MediumMessagingServiceRawHandler {
    
    /**
     * Raw message handling
     * @param record received message
     * @throws MediumMessagingServiceException 
     */
    public void onData(MediumConsumerRecord record) throws MediumMessagingServiceException, SendToDeadLetterException;
    
    /**
     * Adds a message listener for a specific message type
     * @param <M> message class
     * @param messageType message class
     * @param handler message listener to be called
     */
    public <M extends MediumMessage> void addMessageListener(Class<M> messageType, MediumMessageListener<? super M> handler);
    
    /**
     * Removes a registered message listener
     * @param <M> messagae class
     * @param messageType message class
     * @param handler message listener to be removed
     * @return true if the message listener was present
     */
    public <M extends MediumMessage> boolean removeMessageListener(Class<M> messageType, MediumMessageListener<? super M> handler);
    
}
