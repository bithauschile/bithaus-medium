/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cl.bithaus.medium.message.exception;

/**
 *
 * @author jmakuc
 */
public class MediumMessageListenerException extends Exception {
    
    public MediumMessageListenerException(String message) {
        
        super(message);
    }
    
    public MediumMessageListenerException(Throwable cause) {
        
        super(cause);
    }
    
    public MediumMessageListenerException(String message, Throwable cause) {
        
        super(message, cause);
    }
}
