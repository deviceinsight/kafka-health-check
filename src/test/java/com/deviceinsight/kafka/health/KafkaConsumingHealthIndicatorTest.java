package com.deviceinsight.kafka.health;

import static com.deviceinsight.kafka.health.KafkaConsumingHealthIndicatorTest.TOPIC;
import static org.assertj.core.api.Assertions.assertThat;

import kafka.server.KafkaServer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.core.BrokerAddress;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@ExtendWith(SpringExtension.class)
@EmbeddedKafka(topics = TOPIC)
public class KafkaConsumingHealthIndicatorTest {

	static final String TOPIC = "health-checks";

	private Consumer<String, String> consumer;

	@Autowired
	private EmbeddedKafkaBroker embeddedKafkaBroker;

	@BeforeEach
	public void setUp() {
		Map<String, Object> consumerConfigs =
				new HashMap<>(KafkaTestUtils.consumerProps("consumer", "false", embeddedKafkaBroker));
		consumer = new DefaultKafkaConsumerFactory<>(consumerConfigs, new StringDeserializer(),
				new StringDeserializer()).createConsumer();
		consumer.subscribe(Collections.singletonList(TOPIC));
		consumer.poll(Duration.ofSeconds(1));
	}

	@AfterEach
	public void tearDown() {
		consumer.close();
		embeddedKafkaBroker.getKafkaServers().forEach(KafkaServer::shutdown);
		embeddedKafkaBroker.getKafkaServers().forEach(KafkaServer::awaitShutdown);
	}

	@Test
	public void kafkaIsDown() throws Exception {
		final KafkaHealthProperties kafkaHealthProperties = new KafkaHealthProperties();
		kafkaHealthProperties.setTopic(TOPIC);

		final KafkaProperties kafkaProperties = new KafkaProperties();
		final BrokerAddress[] brokerAddresses = embeddedKafkaBroker.getBrokerAddresses();
		kafkaProperties.setBootstrapServers(Collections.singletonList(brokerAddresses[0].toString()));

		final KafkaConsumingHealthIndicator healthIndicator =
				new KafkaConsumingHealthIndicator(kafkaHealthProperties, kafkaProperties.buildConsumerProperties(),
						kafkaProperties.buildProducerProperties());
		healthIndicator.subscribeAndSendMessage();

		Health health = healthIndicator.health();
		assertThat(health.getStatus()).isEqualTo(Status.UP);

		shutdownKafka();

		Awaitility.await().untilAsserted(() -> assertThat(healthIndicator.health().getStatus()).isEqualTo(Status.DOWN));
	}

	private void shutdownKafka() {
		this.embeddedKafkaBroker.destroy();
	}
}
