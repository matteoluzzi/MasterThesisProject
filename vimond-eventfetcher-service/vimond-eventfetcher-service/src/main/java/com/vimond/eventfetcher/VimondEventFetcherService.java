package com.vimond.eventfetcher;

import static com.vimond.common.shared.ObjectMapperConfiguration.configure;

import java.util.Properties;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.vimond.eventfetcher.configuration.VimondEventFetcherServiceConfiguration;
import com.vimond.eventfetcher.health.DummyHealthCheck;
import com.vimond.eventfetcher.resources.PingResource;
import com.vimond.eventfetcher.consumer.KafkaConsumerHandler;

/**
 * Main class of vimond-event-fetcher
 * @author matteoremoluzzi
 *
 */
public class VimondEventFetcherService extends Application<VimondEventFetcherServiceConfiguration> {

    public static void main(String[] args) throws Exception {
        new VimondEventFetcherService().run(args);
    }

    @Override
    public void initialize(Bootstrap<VimondEventFetcherServiceConfiguration> bootstrap) {

        bootstrap.getObjectMapper().registerModule(new JodaModule());
    }

    @Override
    public void run(final VimondEventFetcherServiceConfiguration configuration, Environment environment) throws Exception {
    	
        configure(environment.getObjectMapper());

        environment.jersey().register(new PingResource(environment.metrics()));
        environment.healthChecks().register(DummyHealthCheck.NAME, new DummyHealthCheck());
        
        @SuppressWarnings("rawtypes")
		KafkaConsumerHandler consumerHandler = new KafkaConsumerHandler(configuration);
        consumerHandler.registerConsumerGroup();
        consumerHandler.startListening();
    }
}