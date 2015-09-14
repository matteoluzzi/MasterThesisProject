package com.vimond.eventfetcher.resources;

import com.vimond.eventfetcher.VimondEventFetcherService;

import io.dropwizard.testing.junit.DropwizardAppRule;

import org.junit.Rule;
import org.junit.Test;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.UriBuilder;

import java.net.URI;

import static org.fest.assertions.api.Assertions.assertThat;

/**
 * Client for this service.
 *
 * @author Matteo Remo Luzzi	 mailto:matteo@vimond.com
 * @since 2015-08-07
 */
public class PingResourceTest {

    @SuppressWarnings({ "rawtypes", "unchecked" })
	@Rule
    public final DropwizardAppRule application = new DropwizardAppRule(VimondEventFetcherService.class, null);

    @Test
    public void testRespondToPingWithPong() throws Exception {

        String result = ping();
        assertThat(result).isEqualTo("pong");
    }

    private String ping() {
        URI uri = UriBuilder.fromUri("http://localhost")
                    .port(application.getAdminPort())
                    .path("/ping")
                .build();

        return ClientBuilder.newClient()
                        .target(uri)
                .request()
                .get(String.class)
                    .trim();
    }
}
