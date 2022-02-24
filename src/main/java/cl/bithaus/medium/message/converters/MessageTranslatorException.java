/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cl.bithaus.medium.message.converters;

/**
 *
 * @author jmakuc
 */
public class MessageTranslatorException extends Exception {
    
    public MessageTranslatorException(String msg) {
        
        super(msg);
    }
    
    public MessageTranslatorException(Exception e) {
        
        super(e);
    }
    
    public MessageTranslatorException(String msg, Exception e) {
        
        super(msg, e);
    }
}
