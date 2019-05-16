package com.deviceinsight.kafka.health;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.deviceinsight.kafka.health.cache.CacheService;
import com.deviceinsight.kafka.health.cache.CaffeineCacheServiceImpl;

import com.google.common.annotations.VisibleForTesting;
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
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.StreamSupport;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

public class KafkaConsumingHealthIndicator extends AbstractHealthIndicator {

	private static final Logger logger = LoggerFactory.getLogger(
			com.deviceinsight.kafka.health.KafkaConsumingHealthIndicator.class);
	private static final String CONSUMER_GROUP_PREFIX = "health-check-";

	private final Consumer<String, String> consumer;

	private final Producer<String, String> producer;

	private final String topic;
	private final long sendReceiveTimeoutMs;
	private final long pollTimeoutMs;
	private final long subscriptionTimeoutMs;

	private final ExecutorService executor;
	private final AtomicBoolean running;
	private final CacheService<String> cacheService;

	private KafkaCommunicationResult kafkaCommunicationResult;

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

		this.executor = Executors.newFixedThreadPool(2);
		this.running = new AtomicBoolean(true);
		this.cacheService = new CaffeineCacheServiceImpl(calculateCacheExpiration(sendReceiveTimeoutMs));

		this.kafkaCommunicationResult = KafkaCommunicationResult.failure(topic, new RejectedExecutionException("Kafka Health Check is starting."));
	}

	@PostConstruct
	void subscribeAndSendMessage() throws InterruptedException {
		subscribeToTopic();

		sendMessage();

		if (kafkaCommunicationResult.isFailure()) {
			throw new RuntimeException("Kafka health check failed", kafkaCommunicationResult.getException());
		}

		executor.submit(() -> {
			while (running.get()) {
				if (messageNotReceived()) {
					this.kafkaCommunicationResult = KafkaCommunicationResult.failure(topic,
							new RejectedExecutionException("Ignore health check, already running..."));
				} else {
					this.kafkaCommunicationResult = KafkaCommunicationResult.success(topic);
				}
			}
		});
	}

	@PreDestroy
	void shutdown() {
		running.set(false);
		executor.shutdown();
		producer.close();
		consumer.close();
	}

	private void setConsumerGroup(Map<String, Object> kafkaConsumerProperties) {
		try {
			String groupId = (String) kafkaConsumerProperties.getOrDefault(ConsumerConfig.GROUP_ID_CONFIG,
					UUID.randomUUID().toString());
			kafkaConsumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG,
					CONSUMER_GROUP_PREFIX + groupId + "-" + InetAddress.getLocalHost().getHostAddress());
		} catch (UnknownHostException e) {
			throw new IllegalStateException(e);
		}
	}

	@VisibleForTesting
	void subscribeToTopic() throws InterruptedException {

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

		consumer.poll(Duration.ofMillis(pollTimeoutMs));
		if (!subscribed.await(subscriptionTimeoutMs, MILLISECONDS)) {
			throw new RuntimeException("Subscription to kafka failed, topic=" + topic);
		}
	}

	private void sendMessage() {

		Future<Void> sendReceiveTask = null;

		try {

			sendReceiveTask = executor.submit(() -> {
				sendKafkaMessage();
				return null;
			});

			sendReceiveTask.get(sendReceiveTimeoutMs, MILLISECONDS);
			this.kafkaCommunicationResult = KafkaCommunicationResult.success(topic);

		} catch (ExecutionException e) {
			logger.warn("Kafka health check execution failed.", e);
			this.kafkaCommunicationResult = KafkaCommunicationResult.failure(topic, e);
		} catch (TimeoutException | InterruptedException e) {
			logger.warn("Kafka health check timed out.", e);
			sendReceiveTask.cancel(true);
			this.kafkaCommunicationResult = KafkaCommunicationResult.failure(topic, e);
		} catch (RejectedExecutionException e) {
			logger.debug("Ignore health check, already running...");
		}
	}

	private void sendKafkaMessage() throws Exception {

		String message = UUID.randomUUID().toString();
		long startTime = System.currentTimeMillis();

		logger.debug("Send health check message = {}", message);

		producer.send(new ProducerRecord<>(topic, message, message)).get(sendReceiveTimeoutMs, MILLISECONDS);
		cacheService.write(message);

		logger.debug("Kafka health check succeeded. took= {} msec", System.currentTimeMillis() - startTime);
	}

	private boolean messageNotReceived() {

		return StreamSupport.stream(consumer.poll(Duration.ofMillis(pollTimeoutMs)).spliterator(), false)
				.noneMatch(msg -> cacheService.get(msg.key()) == null);
	}


	@Override
	protected void doHealthCheck(Health.Builder builder) {
		sendMessage();

		if (this.kafkaCommunicationResult.isFailure()) {
			builder.down(this.kafkaCommunicationResult.getException())
					.withDetail("topic", this.kafkaCommunicationResult.getTopic());
		} else {
			builder.up();
		}
	}

	private long calculateCacheExpiration(long timeout) {
		return (long) (timeout * 0.8);
	}
}
