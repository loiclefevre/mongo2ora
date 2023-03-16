package com.oracle.mongo2ora.util;

import java.time.Duration;

public class Tools {
	public static String getDurationSince(long startTime) {
		final long durationMillis = System.currentTimeMillis() - startTime;
		if (durationMillis < 1000) {
			return String.format("0.%03ds", durationMillis);
		} else {
			final Duration duration = Duration.ofMillis(durationMillis);
			return duration.toString().substring(2).replaceAll("(\\d[HMS])(?!$)", "$1 ").replaceAll("\\.\\d+", "").toLowerCase();
		}
	}

	public static void sleep(long ms) {
		try {
			Thread.sleep(ms);
		}
		catch (InterruptedException ignored) {
		}
	}
}
