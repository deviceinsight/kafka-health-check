package com.deviceinsight.kafka.health.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.util.concurrent.TimeUnit;

public class CaffeineCacheServiceImpl implements CacheService<String> {

	private final Cache<String, String> cache;

	public CaffeineCacheServiceImpl(long expireAfterWrite) {
		this.cache = Caffeine.newBuilder()
				.expireAfterWrite(expireAfterWrite, TimeUnit.MILLISECONDS)
				.recordStats()
				.build();
	}

	@Override
	public void write(String entry) {
		this.cache.put(entry, entry);
	}

	@Override
	public String get(String entry) {
		return this.cache.getIfPresent(entry);
	}
}
