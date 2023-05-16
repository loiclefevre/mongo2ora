package com.oracle.mongo2ora.migration.mongodb;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ExpireAfterSeconds {
	@JsonProperty("$numberLong")
	public long value;

	public long getValue() {
		return value;
	}

	public void setValue(long value) {
		this.value = value;
	}
}
