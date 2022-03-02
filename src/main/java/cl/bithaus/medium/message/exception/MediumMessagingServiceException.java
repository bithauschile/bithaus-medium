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
 *
 * @author jmakuc
 */
public class MediumMessagingServiceException extends Exception {
    
    public MediumMessagingServiceException(String error) {
        
        super(error);
    }
    
    public MediumMessagingServiceException(Throwable cause) {
        
        super(cause);
    }
    
    public MediumMessagingServiceException(String error, Throwable cause) {
        
        super(error, cause);
    }
    
}
