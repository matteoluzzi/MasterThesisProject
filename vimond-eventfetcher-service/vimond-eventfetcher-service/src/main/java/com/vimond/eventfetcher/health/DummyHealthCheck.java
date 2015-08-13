package com.vimond.eventfetcher.health;

import com.codahale.metrics.health.HealthCheck;

public class DummyHealthCheck extends HealthCheck {
	public static final String NAME = "dummy";
    
    @Override
    protected Result check() throws Exception {
        return Result.healthy();
    }

}
