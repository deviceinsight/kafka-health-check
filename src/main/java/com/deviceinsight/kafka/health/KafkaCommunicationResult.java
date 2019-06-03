package com.deviceinsight.kafka.health;

final class KafkaCommunicationResult {

	private final Exception exception;

	private KafkaCommunicationResult() {
		this.exception = null;
	}

	private KafkaCommunicationResult(Exception exception) {
		this.exception = exception;
	}

	static KafkaCommunicationResult success() {
		return new KafkaCommunicationResult();
	}

	static KafkaCommunicationResult failure(Exception exception) {
		return new KafkaCommunicationResult(exception);
	}

	Exception getException() {
		return exception;
	}

	@Override
	public String toString() {
		return "KafkaCommunication{exception=" + exception + '}';
	}

	public boolean isFailure() {
		return exception != null;
	}
}
