package com.sais.utils.counting;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Cluster.Builder;

public class CounterService {
	
	private String columnFamilyName;
	private Session session;

	public CounterService(String contactPoints, String keyspaceName, String columnFamilyName) {
		this.columnFamilyName = columnFamilyName;
		Builder builder = Cluster.builder();
		builder.addContactPoints(contactPoints.split(","));
		Cluster cluster = builder.build();
		this.session = cluster.connect(keyspaceName);
    }
	
	public Counter getCounter(String name) {
		return new Counter(session, columnFamilyName, name);
	}
	

}
