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
public class MessageConverterException extends Exception {
    
    public MessageConverterException(String msg) {
        
        super(msg);
    }
    
    public MessageConverterException(Exception e) {
        
        super(e);
    }
    
    public MessageConverterException(String msg, Exception e) {
        
        super(msg, e);
    }
}
