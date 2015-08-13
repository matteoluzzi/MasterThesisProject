package com.vimond.eventfetcher.resources;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Client for this service.
 *
 * @author Matteo Remo Luzzi	 mailto:matteo@vimond.com
 * @since 2015-08-07
 */
@Path("/ping")
public class PingResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(PingResource.class);
    private final Counter pingCounter;

    public PingResource(MetricRegistry metricRegistry) {
        this.pingCounter = metricRegistry.counter(name(PingResource.class, "pings"));
    }

    @GET
    public String ping() {
        LOGGER.info("Ping resource called...");
        pingCounter.inc();
        return "pong";
    }

}
