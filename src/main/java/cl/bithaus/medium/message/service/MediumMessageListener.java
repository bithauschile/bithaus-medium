/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cl.bithaus.medium.message.service;

import cl.bithaus.medium.message.exception.MediumMessageListenerException;
import cl.bithaus.medium.message.MediumMessage;

/**
 * Listener class for a specific Medium message type.
 * @author jmakuc
 * @param <M> message type to listen for
 */
public interface MediumMessageListener<M extends MediumMessage> {
    
    public String getName();
    
    public void onMessage(M message) throws MediumMessageListenerException;
}
