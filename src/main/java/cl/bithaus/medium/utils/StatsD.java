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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.timgroup.statsd.Event;
import com.timgroup.statsd.Event.AlertType;
import com.timgroup.statsd.NoOpStatsDClient;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.NonBlockingStatsDClientBuilder;
import com.timgroup.statsd.StatsDClient;

/**
 * Bithaus Medium Observability Class
 * Use this to report metrics and events.
 * @author jmakuc
 */
public class StatsD {

    public static final Logger log = LoggerFactory.getLogger(StatsD.class);

    public static final String STATSD_HOST_DEFAULT = "/var/run/datadog/dsd.socket";
    public static final Integer STATSD_PORT_DEFAULT = 0;

    private StatsDClient client;

    public StatsD(StatsDClient client) {
        this.client = client;
    }


    public void close() {
        client.stop();
    }

    
    public void count(final String aspect, final long delta, final String... tags) {
        client.count(aspect, delta, tags);
    }

    public void count(final String aspect, final double delta, final String ... tags) {
        client.count(aspect, delta, tags);
    }

    public void incrementCounter(final String aspect, final String... tags) {
        client.incrementCounter(aspect, tags);
    }

    public void decrementCounter(final String aspect, final String... tags) {
        client.decrementCounter(aspect, tags);
    }

    public void recordGaugeValue(final String aspect, final double value, final String... tags) {
        client.recordGaugeValue(aspect, value, tags);
    }

    public void recordGaugeValue(final String aspect, final long value, final String... tags) {
        client.recordGaugeValue(aspect, value, tags);
    }

    public void recordHistogramValue(final String aspect, final double value, final String... tags) {
        client.recordHistogramValue(aspect, value, tags);
    }

    public void recordHistogramValue(final String aspect, final long value, final String... tags) {
        client.recordHistogramValue(aspect, value, tags);
    }

    public void recordDistributionValue(final String aspect, final double value, final String... tags) {
        client.recordDistributionValue(aspect, value, tags);
    }

    public void recordDistributionValue(final String aspect, final long value, final String... tags) {
        client.recordDistributionValue(aspect, value, tags);
    }

    public void recordExecutionTime(final String aspect, final long timeInMs, final String... tags) {
        client.recordExecutionTime(aspect, timeInMs, tags);
    }


    public void recordEvent(final Event event, final String... eventTags) {

        if(log.isTraceEnabled())
            log.trace("recordEvent: {} tags: {}", event, Arrays.toString(eventTags));

        client.recordEvent(event, eventTags);
    }

    public void recordException(String message, Throwable exception, String... eventTags) {

        if(log.isTraceEnabled())
            log.trace("recordException: {} tags: {}", message, Arrays.toString(eventTags));

        // Get the exception stacktrace on a String using PrintWriter
        StringWriter sw = new StringWriter();        
        exception.printStackTrace(new PrintWriter(sw));        

        Event event = Event.builder()
            .withTitle(message)
            .withText(sw.toString())
            .withAlertType(AlertType.ERROR)            
            .build(); 
            
        client.recordEvent(event, eventTags);
            
    }

    public void recordEventInfo(String message, String text, String... tags) {

        if(log.isTraceEnabled())
            log.trace("recordEventInfo: {} tags: {}", message, Arrays.toString(tags));

        Event event = Event.builder()
            .withTitle(message)
            .withText(text)
            .withAlertType(AlertType.INFO)            
            .build(); 
            
        client.recordEvent(event, tags);
    }

    public void recordEventSuccess(String message, String text, String... tags) {

        if(log.isTraceEnabled())
            log.trace("recordEventSuccess: {} tags: {}", message, Arrays.toString(tags));

        Event event = Event.builder()
            .withTitle(message)
            .withText(text)
            .withAlertType(AlertType.SUCCESS)            
            .build(); 
            
        client.recordEvent(event, tags);
    }

    public void recordEventWarning(String message, String text, String... tags) {

        if(log.isTraceEnabled())
            log.trace("recordEventWarning: {} tags: {}", message, Arrays.toString(tags));

        Event event = Event.builder()
            .withTitle(message)
            .withText(text)
            .withAlertType(AlertType.WARNING)            
            .build(); 
            
        client.recordEvent(event, tags);
    }

    /**
     * Returns a no-op version of this class
     */
    public static StatsD getNoOp() {

        return new StatsD(new NoOpStatsDClient());
    }      



    
    public static StatsD createFromEnvAndProperties(StatsDProperties statsDProperties) {

        // log.info("Configurando StatsDClient: " + statsDProperties);

        String service = System.getProperty("dd.service");

        if(service != null && service.length() > 0)
            service = System.getenv("DD_SERVICE");

        String env = System.getProperty("dd.env");

        if(env != null && env.length() > 0)
            env = System.getenv("DD_ENV");
        
        String version = System.getProperty("dd.version");

        if(version != null && version.length() > 0)
            version = System.getenv("DD_VERSION");

        if(version == null || version.length() == 0) {
            version = StatsD.class.getPackage().getImplementationVersion();
            if(version != null && version.length() > 0)
                System.setProperty("dd.version", version);
        }

        
        


        NonBlockingStatsDClientBuilder builder = new NonBlockingStatsDClientBuilder();

        if(statsDProperties == null) {

            String hostname = System.getenv("DOGSTATSD_HOST");

            if(hostname != null && hostname.length() > 0) {
                builder.hostname(hostname);
            }
            else {
                builder.hostname(STATSD_HOST_DEFAULT);
            }

            String port = System.getenv("DOGSTATSD_PORT");

            if(port != null && port.length() > 0) {
                builder.port(Integer.parseInt(port));
            }
            else {
                builder.port(STATSD_PORT_DEFAULT);
            }

        }
        else {

            if(statsDProperties.getHostname() != null) {

                builder.hostname(statsDProperties.getHostname());

                if(statsDProperties.getPort() != null) {
                    builder.port(statsDProperties.getPort());
                }
            }

            if(statsDProperties.getMetricsPrefix() != null) {
                builder.prefix(statsDProperties.getMetricsPrefix());
            }
        }
        

        NonBlockingStatsDClient client = builder.build();

        

        boolean serviceOk = false;
        boolean envOk = false;
        boolean versionOk = false;

        if(service == null || service.length() == 0) {
            log.warn("DD_SERVICE IS undefined");
        }
        else {
            log.info("DD_SERVICE: " + service);
            serviceOk = true;
        }

        if(env == null || env.length() == 0) {
            log.warn("DD_ENV IS undefined");
        }
        else {
            log.info("DD_ENV: " + env);
            envOk = true;
        }

        if(version == null || version.length() == 0) {
            log.warn("DD_VERSION IS undefined");
        }
        else {
            log.info("DD_VERSION: " + version);
            versionOk = true;
        }

        if(!serviceOk || !envOk || !versionOk) {
         
            String text = "";
            if(!serviceOk) {
                text += "DD_SERVICE IS undefined\n";
            }
            if(!envOk) {
                text += "DD_ENV IS undefined\n";
            }
            if(!versionOk) {
                text += "DD_VERSION IS undefined\n";
            }            

            Event event = Event.builder().
            withTitle("StatsD configuration incomplete").
            withText(text).
            withAlertType(AlertType.WARNING).build();

            client.recordEvent(event);            
        }


        return new StatsD(client);        
    }


}
