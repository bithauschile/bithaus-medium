/*
 * Copyright (c) BitHaus Software Chile
 * All rights reserved. www.bithaus.cl.
 * 
 * All rights to this product are owned by Bithaus Chile and may only by used 
 * under the terms of its associated license document. 
 * You may NOT copy, modify, sublicense or distribute this source file or 
 * portions of it unless previously authorized by writing by Bithaus Software Chile.
 * In any event, this notice must always be included verbatim with this file.
 */
package cl.bithaus.medium.stream;

/**
 *
 * @author jmakuc
 */
public class MediumProcessorException extends Exception {
    
    public MediumProcessorException(String message) {
        super(message);
    }
    
    public MediumProcessorException(Throwable cause) {
        super(cause);
    }
    
    public MediumProcessorException(String message, Throwable cause) {
        super(message, cause);
    }
}
