package com.deviceinsight.kafka.health;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.ConfigurationPropertySource;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

import java.time.Duration;
import java.util.stream.Stream;

public class KafkaHealthPropertiesTest {

	// @formatter:off
	private static final ConfigurationPropertySource DURATION_PROPERTY_SOURCE = new MapConfigurationPropertySource(ImmutableMap.of(
			"kafka.health.topic", "custom-topic",
			"kafka.health.send-receive-timeout", "1m",
			"kafka.health.poll-timeout", "2s",
			"kafka.health.subscription-timeout", "10s",
			"kafka.health.cache.maximum-size", "42"
	));

	private static final ConfigurationPropertySource MILLISECONDS_PROPERTY_SOURCE = new MapConfigurationPropertySource(ImmutableMap.of(
			"kafka.health.topic", "custom-topic",
			"kafka.health.send-receive-timeout-ms", "60000",
			"kafka.health.poll-timeout-ms", "2000",
			"kafka.health.subscription-timeout-ms", "10000",
			"kafka.health.cache.maximum-size", "42"
	));
	// @formatter:on

	@ParameterizedTest(name = "using {0} based setters")
	@MethodSource("configurationPropertySources")
	@SuppressWarnings("unused")
	public void test_that_properties_bind_to_KafkaHealthProperties(String sourceName,
			ConfigurationPropertySource propertySource) {

		KafkaHealthProperties kafkaHealthProperties =
				new Binder(propertySource).bind("kafka.health", KafkaHealthProperties.class).get();

		assertThat(kafkaHealthProperties.getTopic()).isEqualTo("custom-topic");
		assertThat(kafkaHealthProperties.getSendReceiveTimeout()).isEqualTo(Duration.ofMinutes(1));
		assertThat(kafkaHealthProperties.getPollTimeout()).isEqualTo(Duration.ofSeconds(2));
		assertThat(kafkaHealthProperties.getSubscriptionTimeout()).isEqualTo(Duration.ofSeconds(10));
		assertThat(kafkaHealthProperties.getCache().getMaximumSize()).isEqualTo(42);
	}

	static Stream<Arguments> configurationPropertySources() {
		return Stream.of(arguments("Duration", DURATION_PROPERTY_SOURCE),
				arguments("long (milliseconds)", MILLISECONDS_PROPERTY_SOURCE));
	}

}
