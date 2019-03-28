package com.deviceinsight.kafka.health;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeoutException;
import java.util.stream.StreamSupport;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

public class KafkaConsumingHealthIndicator extends AbstractHealthIndicator {

	private static final Logger logger = LoggerFactory.getLogger(KafkaConsumingHealthIndicator.class);

	private final Consumer<String, String> consumer;

	private final Producer<String, String> producer;

	private final String topic;
	private final long sendReceiveTimeoutMs;
	private final long pollTimeoutMs;
	private final long subscriptionTimeoutMs;

	private final ExecutorService executor;


	public KafkaConsumingHealthIndicator(KafkaHealthProperties kafkaHealthProperties,
			Map<String, Object> kafkaConsumerProperties, Map<String, Object> kafkaProducerProperties) {

		this.topic = kafkaHealthProperties.getTopic();
		this.sendReceiveTimeoutMs = kafkaHealthProperties.getSendReceiveTimeoutMs();
		this.pollTimeoutMs = kafkaHealthProperties.getPollTimeoutMs();
		this.subscriptionTimeoutMs = kafkaHealthProperties.getSubscriptionTimeoutMs();

		Map<String, Object> kafkaConsumerPropertiesCopy = new HashMap<>(kafkaConsumerProperties);

		setConsumerGroup(kafkaConsumerPropertiesCopy);

		StringDeserializer deserializer = new StringDeserializer();
		StringSerializer serializer = new StringSerializer();

		this.consumer = new KafkaConsumer<>(kafkaConsumerPropertiesCopy, deserializer, deserializer);
		this.producer = new KafkaProducer<>(kafkaProducerProperties, serializer, serializer);

		this.executor = new ThreadPoolExecutor(0, 1, 0L, MILLISECONDS, new SynchronousQueue<>(),
				new ThreadPoolExecutor.AbortPolicy());
	}

	@PostConstruct
	void subscribeAndSendMessage() throws InterruptedException {
		subscribeToTopic();
		KafkaCommunicationResult kafkaCommunicationResult = sendAndReceiveMessage();
		if (kafkaCommunicationResult.isFailure()) {
			throw new RuntimeException("Kafka health check failed", kafkaCommunicationResult.getException());
		}
	}

	@PreDestroy
	void shutdown() {
		executor.shutdown();
		producer.close();
		consumer.close();
	}

	private void setConsumerGroup(Map<String, Object> kafkaConsumerProperties) {
		try {
			kafkaConsumerProperties.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG,
					"health-check-" + InetAddress.getLocalHost().getHostAddress());
		} catch (UnknownHostException e) {
			throw new IllegalStateException(e);
		}
	}

	private void subscribeToTopic() throws InterruptedException {

		final CountDownLatch subscribed = new CountDownLatch(1);

		logger.info("Subscribe to health check topic={}", topic);

		consumer.subscribe(Collections.singleton(topic), new ConsumerRebalanceListener() {

			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
				// nothing to do her
			}

			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				logger.debug("Got partitions = {}", partitions);

				if (!partitions.isEmpty()) {
					subscribed.countDown();
				}
			}
		});

		consumer.poll(pollTimeoutMs);
		if (!subscribed.await(subscriptionTimeoutMs, MILLISECONDS)) {
			throw new RuntimeException("Subscription to kafka failed, topic=" + topic);
		}
	}

	private KafkaCommunicationResult sendAndReceiveMessage() {

		Future<Void> sendReceiveTask = null;

		try {

			sendReceiveTask = executor.submit(() -> {
				sendAndReceiveKafkaMessage();
				return null;
			});

			sendReceiveTask.get(sendReceiveTimeoutMs, MILLISECONDS);

		} catch (ExecutionException e) {
			logger.warn("Kafka health check execution failed.", e);
			return KafkaCommunicationResult.failure(topic, e);
		} catch (TimeoutException | InterruptedException e) {
			logger.warn("Kafka health check timed out.", e);
			sendReceiveTask.cancel(true);
			return KafkaCommunicationResult.failure(topic, e);
		} catch (RejectedExecutionException e) {
			logger.debug("Ignore health check, already running...");
		}
		return KafkaCommunicationResult.success(topic);
	}

	private void sendAndReceiveKafkaMessage() throws Exception {

		String message = UUID.randomUUID().toString();
		long startTime = System.currentTimeMillis();

		logger.debug("Send health check message = {}", message);
		producer.send(new ProducerRecord<>(topic, message, message)).get(sendReceiveTimeoutMs, MILLISECONDS);

		while (messageNotReceived(message)) {
			logger.debug("Waiting for message={}", message);
		}

		logger.debug("Kafka health check succeeded. took= {} msec", System.currentTimeMillis() - startTime);
	}


	private boolean messageNotReceived(String message) {

		return StreamSupport.stream(consumer.poll(pollTimeoutMs).spliterator(), false)
				.noneMatch(msg -> msg.key().equals(message) && msg.value().equals(message));
	}


	@Override
	protected void doHealthCheck(Health.Builder builder) {
		KafkaCommunicationResult kafkaCommunicationResult = sendAndReceiveMessage();

		if (kafkaCommunicationResult.isFailure()) {
			builder.down(kafkaCommunicationResult.getException())
					.withDetail("topic", kafkaCommunicationResult.getTopic());
		} else {
			builder.up();
		}
	}
}
