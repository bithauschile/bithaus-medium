/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cl.bithaus.medium.stream;

import cl.bithaus.medium.message.exception.MediumMessageListenerException;
import cl.bithaus.medium.message.MediumMessage;
import java.util.Collection;

/**
 * Listener class for a specific Medium message type.
 * @author jmakuc
 * @param <I> message type to be read from upstream
 * @param <O> message type to be written downstream
 */
public interface MediumMessageProcessor<I extends MediumMessage, O extends MediumMessage> {
    
    public String getName();
    
    public Collection<O> onMessage(I message) throws MediumMessageListenerException;
}
