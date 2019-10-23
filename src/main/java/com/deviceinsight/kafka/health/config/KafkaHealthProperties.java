package com.deviceinsight.kafka.health.config;

public class KafkaHealthProperties {

	private String topic = "health-checks";
	private long sendReceiveTimeoutMs = 2500;
	private long pollTimeoutMs = 200;
	private long subscriptionTimeoutMs = 5000;

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public long getSendReceiveTimeoutMs() {
		return sendReceiveTimeoutMs;
	}

	public void setSendReceiveTimeoutMs(long sendReceiveTimeoutMs) {
		this.sendReceiveTimeoutMs = sendReceiveTimeoutMs;
	}

	public long getPollTimeoutMs() {
		return pollTimeoutMs;
	}

	public void setPollTimeoutMs(long pollTimeoutMs) {
		this.pollTimeoutMs = pollTimeoutMs;
	}

	public long getSubscriptionTimeoutMs() {
		return subscriptionTimeoutMs;
	}

	public void setSubscriptionTimeoutMs(long subscriptionTimeoutMs) {
		this.subscriptionTimeoutMs = subscriptionTimeoutMs;
	}
}
