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
	                          NullPolicy nullPolicy,
	                          boolean synchronous,
	                          boolean atomic) {
		return new Mutator(this, ttlSeconds, consistencyLevel, nullPolicy, synchronous, atomic);
	}

	void execute(Mutator mutator) {
		String statement = mutator.getBatchStatement();
		if (mutator.isSynchronous())
			session.execute(statement);
		else
			session.executeAsync(statement);
	}

	public void shutdown() {
		session.shutdown();
	}

}
