package com.vimond.eventfetcher;

import com.vimond.eventfetcher.configuration.VimondEventFetcherServiceConfiguration;

import io.dropwizard.cli.Command;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

/**
 * Test for this service.
 *
 * @author Matteo Remo Luzzi	 mailto:matteo@vimond.com
 * @since 2015-08-07
 */
public class VimondEventFetcherServiceTestService extends VimondEventFetcherService {

    public VimondEventFetcherServiceTestService() {
    }

    public static void main(String[] args) throws Exception {
        new VimondEventFetcherServiceTestService().run(args);
    }

    @Override
    public void initialize(Bootstrap<VimondEventFetcherServiceConfiguration> bootstrap) {
        super.initialize(bootstrap);
        bootstrap.addCommand(new TestCommand("test", "Start server in test-mode (without jetty)"));
    }

    @Override
    public void run(VimondEventFetcherServiceConfiguration configuration, Environment environment) throws Exception {
        super.run(configuration, environment);
    }

    public class TestCommand extends Command {

        @Override
        public void configure(Subparser subparser) {

        }

        @Override
        public void run(Bootstrap<?> bootstrap, Namespace namespace) throws Exception {

        }

        /**
         * Create a new command with the given name and description.
         *
         * @param name        the name of the command, used for command line invocation
         * @param description a description of the command's purpose
         */
        protected TestCommand(String name, String description) {
            super(name, description);
        }
    }
}
