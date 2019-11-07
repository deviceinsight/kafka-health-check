package com.deviceinsight.kafka.health.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.config.MeterFilter;

@Configuration
public class MetricsConfig {

	@Value("${host}")
	private String host;

	@Value("${service}")
	private String service;

	@Value("${region}")
	private String region;

	@Bean
	MeterRegistryCustomizer<MeterRegistry> meterRegistry() {
		return registry -> registry.config().commonTags("host", host, "service", service, "region", region)
				.meterFilter(MeterFilter.deny(id -> {
					String uri = id.getTag("uri");
					return uri != null && uri.startsWith("/actuator");
				})).meterFilter(MeterFilter.deny(id -> {
					String uri = id.getTag("uri");
					return uri != null && uri.contains("favicon");
				}));
	}
}