package com.deviceinsight.kafka.health;

final class KafkaCommunicationResult {

	private final String topic;

	private final Exception exception;

	private KafkaCommunicationResult() {
		this.topic = null;
		this.exception = null;
	}

	private KafkaCommunicationResult(String topic, Exception exception) {
		this.topic = topic;
		this.exception = exception;
	}

	static KafkaCommunicationResult success(String topic) {
		return new KafkaCommunicationResult();
	}

	static KafkaCommunicationResult failure(String topic, Exception exception) {
		return new KafkaCommunicationResult(topic, exception);
	}

	String getTopic() {
		return topic;
	}

	Exception getException() {
		return exception;
	}

	@Override
	public String toString() {
		return "KafkaCommunication{topic='" + topic + "', exception=" + exception + '}';
	}

	public boolean isFailure() {
		return exception != null;
	}
}
