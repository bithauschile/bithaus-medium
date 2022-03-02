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
package cl.bithaus.medium.message.exception;

/**
 * Error that signals the underlying network driver that the message currently
 * being process should go into dead-letter destination
 * @author jmakuc
 */
public class SendToDeadLetterException extends Exception {
    
    public SendToDeadLetterException(String error) {
        
        super(error);
    }
    
    public SendToDeadLetterException(Throwable cause) {
        
        super(cause);
    }
    
    public SendToDeadLetterException(String error, Throwable cause) {
        
        super(error, cause);
    }
    
}
