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
package cl.bithaus.medium.utils;

/**
 * Observability Class Configuration
 * @author jmakuc
 */
public class StatsDProperties {
    
    /**
     * Hostname of the statsD server. Default: localhost
     * If you use vía socket, you can use a unix socket path
     */
    private String hostname = "localhost";
    /**
     * Port of the statsD server. Default: 8125
     * If you use vía socket, put 0
     */
    private Integer port = 8125;
    /**
     * Prefix of the metrics. 
     */
    private String metricsPrefix;

    public StatsDProperties() {
     
    }

    public StatsDProperties(String hostname, Integer port, String metricsPrefix) {


        if(hostname != null && hostname.length() > 0)
            this.hostname = hostname;

        if(port != null)
            this.port = port;
        
        if(metricsPrefix != null && metricsPrefix.length() > 0)
            this.metricsPrefix = metricsPrefix;
        else
            throw new IllegalArgumentException("metricsPrefix is required");
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }
    
    public String getMetricsPrefix() {
        return metricsPrefix;
    }

    public void setMetricsPrefix(String metricsPrefix) {
        this.metricsPrefix = metricsPrefix;
    }


}


