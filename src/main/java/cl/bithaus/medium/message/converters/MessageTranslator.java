/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cl.bithaus.medium.message.converters;

import cl.bithaus.medium.message.MediumMessage;
import cl.bithaus.medium.record.MediumConsumerRecord;
import java.util.Collection;

/**
 *
 * @author jmakuc
 * @param <O> Output message type for the converter
 */
public abstract class MessageTranslator <O extends MediumMessage> {
    
    public abstract Collection<O> toMedium(MediumConsumerRecord record) throws MessageTranslatorException;
    
}
