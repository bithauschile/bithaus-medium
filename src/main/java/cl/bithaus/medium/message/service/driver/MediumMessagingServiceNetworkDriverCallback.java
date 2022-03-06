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
package cl.bithaus.medium.message.service.driver;

import cl.bithaus.medium.record.MediumConsumerRecord;
import cl.bithaus.medium.message.exception.MediumMessagingServiceException;

/**
 * Simple callback invoked by the network driver upon message arrival
 * @author jmakuc
 */
public interface MediumMessagingServiceNetworkDriverCallback {
 
    /**
     * Message delivery from the underlying Kafka
     * @param record Message that arrived from the underlying Kafka
     * @throws cl.bithaus.medium.message.exception.MediumMessagingServiceException
     */
    public void onMessage(MediumConsumerRecord record) throws MediumMessagingServiceException;
}
