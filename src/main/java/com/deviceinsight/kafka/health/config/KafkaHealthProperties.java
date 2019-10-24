package com.deviceinsight.kafka.health.config;

import java.time.Duration;

public class KafkaHealthProperties {

  private String topic = "health-checks";
  private Duration sendReceiveTimeout = Duration.ofMillis(2500);
  private Duration pollTimeout = Duration.ofMillis(200);
  private Duration subscriptionTimeout = Duration.ofSeconds(5);

  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public Duration getSendReceiveTimeout() {
    return sendReceiveTimeout;
  }

  public void setSendReceiveTimeout(Duration sendReceiveTimeout) {
    this.sendReceiveTimeout = sendReceiveTimeout;
  }

  public Duration getPollTimeout() {
    return pollTimeout;
  }

  public void setPollTimeout(Duration pollTimeout) {
    this.pollTimeout = pollTimeout;
  }

  public Duration getSubscriptionTimeout() {
    return subscriptionTimeout;
  }

  public void setSubscriptionTimeout(Duration subscriptionTimeout) {
    this.subscriptionTimeout = subscriptionTimeout;
  }

  @Override
  public String toString() {
    return "KafkaHealthProperties{" + "topic='" + topic + '\'' + ", sendReceiveTimeout=" + sendReceiveTimeout
        + ", pollTimeout=" + pollTimeout + ", subscriptionTimeout=" + subscriptionTimeout + '}';
  }
}
