package com.sais.utils.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Cluster.Builder;

public class Keyspace {

	private String name;
	private Cluster cluster;
	private Session session;

	public Keyspace(String hosts, String name) {
		Builder builder = Cluster.builder();
		builder.addContactPoints(hosts.split(","));
		this.cluster = builder.build();
		this.session = cluster.connect(name);
	}

	public String getName() {
		return name;
	}

	public Session getSession() {
		return session;
	}

	public Mutator getMutator(ConsistencyLevel consistencyLevel,
	                          Integer ttlSeconds,
	                          NullPolicy nullPolicy) {
		return new Mutator(this, ttlSeconds, consistencyLevel, nullPolicy);
	}

	void execute(Mutator mutator) {
		String statement = mutator.getBatchStatement();
		session.execute(statement);
	}

	void executeAsync(Mutator mutator) {
		String statement = mutator.getBatchStatement();
		session.executeAsync(statement);
	}

	public void shutdown() {
		session.shutdown();
	}

}
