package com.sais.utils.cassandra;

public enum ConsistencyLevel {

	ANY, ONE, TWO, THREE, QUORUM, ALL, LOCAL_QUORUM, EACH_QUORUM;
	
	public static com.datastax.driver.core.ConsistencyLevel toCQLDriverCL(ConsistencyLevel cl) {
		if (cl == null) return com.datastax.driver.core.ConsistencyLevel.ONE;
		switch (cl) {
			case ANY: return com.datastax.driver.core.ConsistencyLevel.ANY;
	        case ONE: return com.datastax.driver.core.ConsistencyLevel.ONE;
	        case TWO: return com.datastax.driver.core.ConsistencyLevel.TWO;
	        case THREE: return com.datastax.driver.core.ConsistencyLevel.THREE;
	        case QUORUM: return com.datastax.driver.core.ConsistencyLevel.QUORUM;
	        case ALL: return com.datastax.driver.core.ConsistencyLevel.ALL;
	        case LOCAL_QUORUM: return com.datastax.driver.core.ConsistencyLevel.LOCAL_QUORUM;
	        case EACH_QUORUM: return com.datastax.driver.core.ConsistencyLevel.EACH_QUORUM;
		}
		throw new AssertionError();
	}
	
	public com.datastax.driver.core.ConsistencyLevel toCQLDriverCL() {
		return ConsistencyLevel.toCQLDriverCL(this);
	}
}
