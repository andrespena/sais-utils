package com.sais.utils.cassandra;

import java.util.LinkedList;
import java.util.List;

import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;

public class Mutator {

	private Keyspace keyspace;
	private String keyspaceName;
	private ConsistencyLevel defaultCL;
	private Integer defaultTTL;
	private NullPolicy defaultNullTreatmentPolicy;
	private boolean synchronous;
	private boolean atomic;

	private final List<String> statements = new LinkedList<String>();

	/**
	 * 
	 * @param keyspace
	 * @param defaultTTL
	 * @param defaultConsistencyLevel
	 * @param defaultNullTreatmentPolicy
	 * @param synchronous
	 * @param atomic
	 */
	public Mutator(Keyspace keyspace,
	               Integer defaultTTL,
	               ConsistencyLevel defaultConsistencyLevel,
	               NullPolicy defaultNullTreatmentPolicy,
	               boolean synchronous,
	               boolean atomic) {
		this.keyspace = keyspace;
		this.keyspaceName = keyspace.getName();
		this.defaultTTL = defaultTTL;
		this.defaultCL = defaultConsistencyLevel;
		this.defaultNullTreatmentPolicy = defaultNullTreatmentPolicy;
	}

	/**
	 * 
	 * @return
	 */
	public Keyspace getKeyspace() {
		return keyspace;
	}

	/**
	 * 
	 * @return
	 */
	public ConsistencyLevel getDefaultConsistencyLevel() {
		return defaultCL;
	}

	/**
	 * 
	 * @return
	 */
	public Integer getDefaultTTL() {
		return defaultTTL;
	}

	/**
	 * 
	 * @return
	 */
	public NullPolicy getDefaultNullTreatmentPolicy() {
		return defaultNullTreatmentPolicy;
	}

	public boolean isSynchronous() {
		return synchronous;
	}

	/**
	 * 
	 * @return
	 */
	public boolean isAtomic() {
		return atomic;
	}

	/**
	 * 
	 * @param tableName
	 * @param keyName
	 * @param keyValue
	 * @param columnName
	 * @param columnValue
	 * @return
	 */
	public Mutator insertColumn(String tableName, String keyName, Object keyValue, String columnName, Object columnValue) {
		return insertColumn(tableName, keyName, keyValue, columnName, columnValue, defaultTTL, defaultCL);
	}

	/**
	 * 
	 * @param tableName
	 * @param keyName
	 * @param keyValue
	 * @param columnName
	 * @param columnValue
	 * @param columnTTL
	 * @return
	 */
	public Mutator insertColumn(String tableName,
	                            String keyName,
	                            Object keyValue,
	                            String columnName,
	                            Object columnValue,
	                            Integer columnTTL) {
		return insertColumn(tableName, keyName, keyValue, columnName, columnValue, columnTTL, defaultCL);
	}

	/**
	 * 
	 * @param tableName
	 * @param keyName
	 * @param keyValue
	 * @param columnName
	 * @param columnValue
	 * @param columnTTL
	 * @param cl
	 * @return
	 */
	public Mutator insertColumn(String tableName,
	                            String keyName,
	                            Object keyValue,
	                            String columnName,
	                            Object columnValue,
	                            Integer columnTTL,
	                            ConsistencyLevel cl) {
		if (columnValue == null) {
			switch (defaultNullTreatmentPolicy) {
			case ERROR:
				throw new NullPointerException("Null column value not allowed by policy");
			case DELETE:
				deleteColumn(tableName, keyName, keyValue, columnName, cl);
			case IGNORE:
				break;
			default:
				throw new RuntimeException("Null treatment policy misunderstanded");
			}
		} else {
			Insert insert = QueryBuilder.insertInto(keyspaceName, tableName);
			insert.setConsistencyLevel(cl == null ? defaultCL.toCQLDriverCL() : cl.toCQLDriverCL());
			if (columnTTL != null) {
				insert.using(QueryBuilder.ttl(columnTTL));
			} else if (defaultTTL != null) {
				insert.using(QueryBuilder.ttl(defaultTTL));
			}
			insert.value(keyName, keyValue);
			insert.value(columnName, columnValue);
			statements.add(insert.getQueryString());
		}
		return this;
	}

	/**
	 * 
	 * @param tableName
	 * @param keyName
	 * @param keyValue
	 * @param columnName
	 * @return
	 */
	public Mutator deleteColumn(String tableName, String keyName, Object keyValue, String columnName) {
		return deleteColumn(tableName, keyName, keyValue, columnName, null);
	}

	/**
	 * 
	 * @param tableName
	 * @param keyName
	 * @param keyValue
	 * @param columnName
	 * @param cl
	 * @return
	 */
	public Mutator deleteColumn(String tableName,
	                            String keyName,
	                            Object keyValue,
	                            String columnName,
	                            ConsistencyLevel cl) {
		String[] columns = new String[] { columnName };
		Delete delete = QueryBuilder.delete(columns).from(tableName);
		delete.setConsistencyLevel(cl == null ? defaultCL.toCQLDriverCL() : cl.toCQLDriverCL());
		Clause clause = QueryBuilder.eq(keyName, keyValue);
		delete.where(clause);
		statements.add(delete.getQueryString());
		return this;
	}

	/**
	 * 
	 * @param tableName
	 * @param keyName
	 * @param keyValue
	 * @return
	 */
	public Mutator deleteRow(String tableName, String keyName, Object keyValue) {
		return deleteRow(tableName, keyName, keyValue, null);
	}

	/**
	 * 
	 * @param tableName
	 * @param keyName
	 * @param keyValue
	 * @param cl
	 * @return
	 */
	public Mutator deleteRow(String tableName, String keyName, Object keyValue, ConsistencyLevel cl) {
		Delete delete = QueryBuilder.delete().from(tableName);
		delete.setConsistencyLevel(cl == null ? defaultCL.toCQLDriverCL() : cl.toCQLDriverCL());
		Clause clause = QueryBuilder.eq(keyName, keyValue);
		delete.where(clause);
		statements.add(delete.getQueryString());
		return this;
	}

	/**
	 * 
	 * @param tableName
	 * @param keyName
	 * @param keyValue
	 * @param counterName
	 * @param counterValue
	 * @return
	 */
	public Mutator incrementCounterColumn(String tableName,
	                                      String keyName,
	                                      Object keyValue,
	                                      String counterName,
	                                      Long counterValue) {
		return incrementCounterColumn(tableName, keyName, keyValue, counterName, counterValue, defaultCL);
	}

	/**
	 * 
	 * @param tableName
	 * @param keyName
	 * @param keyValue
	 * @param counterName
	 * @param counterValue
	 * @param cl
	 * @return
	 */
	public Mutator incrementCounterColumn(String tableName,
	                                      String keyName,
	                                      Object keyValue,
	                                      String counterName,
	                                      Long counterValue,
	                                      ConsistencyLevel cl) {
		Update update = QueryBuilder.update(keyspaceName, tableName);
		update.setConsistencyLevel(cl == null ? defaultCL.toCQLDriverCL() : cl.toCQLDriverCL());
		Clause clause = QueryBuilder.eq(keyName, keyValue);
		update.where(clause);
		update.with(QueryBuilder.incr(counterName, counterValue));
		statements.add(update.getQueryString());
		return this;
	}

	/**
	 * 
	 * @param tableName
	 * @param keyName
	 * @param keyValue
	 * @param counterName
	 * @param counterValue
	 * @return
	 */
	public Mutator decrementCounterColumn(String tableName,
	                                      String keyName,
	                                      Object keyValue,
	                                      String counterName,
	                                      Long counterValue) {
		return decrementCounterColumn(tableName, keyName, keyValue, counterName, counterValue, defaultCL);
	}

	/**
	 * 
	 * @param tableName
	 * @param keyName
	 * @param keyValue
	 * @param counterName
	 * @param counterValue
	 * @param cl
	 * @return
	 */
	public Mutator decrementCounterColumn(String tableName,
	                                      String keyName,
	                                      Object keyValue,
	                                      String counterName,
	                                      Long counterValue,
	                                      ConsistencyLevel cl) {
		Update update = QueryBuilder.update(keyspaceName, tableName);
		update.setConsistencyLevel(cl == null ? defaultCL.toCQLDriverCL() : cl.toCQLDriverCL());
		Clause clause = QueryBuilder.eq(keyName, keyValue);
		update.where(clause);
		update.with(QueryBuilder.decr(counterName, counterValue));
		statements.add(update.getQueryString());
		return this;
	}

	/**
	 * 
	 * @return
	 */
	public List<String> getStatements() {
		return statements;
	}

	/**
	 * 
	 * @return
	 */
	public String getBatchStatement() {

		StringBuilder builder = new StringBuilder();
		if (atomic) {
			builder.append("BEGIN BATCH\n");
		} else {
			builder.append("BEGIN UNLOGGED BATCH\n");
		}
		for (String statement : getStatements()) {
			builder.append('\t');
			builder.append(statement);
			builder.append('\n');
		}
		builder.append("APPLY BATCH");
		return builder.toString();
	}

	/**
	 * 
	 */
	public void execute() {
		keyspace.execute(this);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return getBatchStatement();
	}

}
