package com.sais.utils.counting;

import com.sais.utils.cassandra.Keyspace;

public class CounterService {
	
	private Keyspace keyspace;
	private String columnFamilyName;

	public CounterService(String contactPoints, String keyspaceName, String columnFamilyName) {
		this.keyspace = new Keyspace(contactPoints, keyspaceName);
		this.columnFamilyName = columnFamilyName;
    }
	
	public Counter getCounter(String name) {
		return new Counter(keyspace, columnFamilyName, name);
	}
	

}
