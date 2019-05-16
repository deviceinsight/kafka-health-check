package com.deviceinsight.kafka.health.cache;

public interface CacheService<T> {

	void write(T entry);

	T get(T entry);
}
