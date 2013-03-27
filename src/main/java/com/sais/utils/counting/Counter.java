package com.sais.utils.counting;

import com.sais.utils.cassandra.Keyspace;

public class Counter {

	// The Cassandra keyspace
	private Keyspace keyspace;
	private String cfName;

	// The path's names in the analytic's hierarchical name tree
	private String name;

	/**
	 * Constructor for obtain the root counter.
	 * 
	 * @param keyspace
	 * @param name
	 */
	
	/**
	 * Constructor.
	 * 
	 * @param keyspace the {@link Keyspace} to be used
	 * @param cfName
	 * @param name
	 */
	Counter(Keyspace keyspace, String cfName, String name) {
		if (keyspace == null) {
			throw new IllegalArgumentException("A not null keyspace is required");
		}
		if (cfName == null || cfName.isEmpty()) {
			throw new IllegalArgumentException("A not null or empty column family name is required");
		}
		if (name == null || name.isEmpty()) {
			throw new IllegalArgumentException("A not null or empty counter name is required");
		}
		this.keyspace = keyspace;
		this.cfName = cfName;
		this.name = name;
	}

	/**
	 * 
	 * @return
	 */
	public String getName() {
		return name;
	}
	
//	/**
//	 * Increase this in one unit for the current date.
//	 * 
//	 * @param transaction the atomic {@link Transaction} to be used
//	 */
//	public void update(Transaction transaction) {
//		update(transaction, new Date(), null);		
//	}
//    
//	/**
//	 * Increase this in one unit for the specified date.
//	 * 
//	 * @param transaction the atomic {@link Transaction} to be used
//	 * @param date
//	 */
//	public void update(Transaction transaction, Date date) {
//		update(transaction, date, null);
//	}
//    
//	/**
//	 * Increase this in one unit for the current date using the specified event
//	 * value for means, deviations and variances.
//	 * 
//	 * @param transaction the atomic {@link Transaction} to be used
//	 * @param value the event's value for means, deviations and variances
//	 */
//	public void update(Transaction transaction, Long value) {
//		update(transaction, new Date(), value);
//	}
//
//	/**
//	 * Updates the value of this {@link Counter} and of all its ancestors using
//	 * the specified value and date.
//	 * 
//	 * @param transaction the atomic {@link Transaction} to be used
//	 * @param date the event's date
//	 * @param value the event's value for means, deviations and variances
//	 */
//	public void update(Date time, Long value) {
//		
//		Mutator mutator = keyspace.getMutator(ConsistencyLevel.QUORUM, null, NullPolicy.IGNORE);
//
//		// Build row key
//		String rowKey = getName();
//
//		// Iterate over all time granularities
//		for (TimeGranularity granularity : TimeGranularity.values()) {
//
//			// Increment counts counter
//			Composite countsColName = getColumnName(ValueType.COUNTS, granularity, time, true);
//			transaction.updateCounterColumn(cfName, rowKey, countsColName, 1);
//
//			// Increment value statistics only if a value is provided
//			if (value != null) {
//
//				// Increment sums counter
//				Composite sumsColName = getColumnName(ValueType.SUMS, granularity, time, true);
//				transaction.updateCounterColumn(cfName, rowKey, sumsColName, value);
//
//				// Increment squares counter
//				Composite squaresColName = getColumnName(ValueType.SQUARES, granularity, time, true);
//				transaction.updateCounterColumn(cfName, rowKey, squaresColName, value * value);
//
//			}
//		}
//	}
//
//	/**
//	 * Deletes this from database.
//	 * 
//	 * @param transaction the atomic {@link Transaction} to be used
//	 */
//	public void delete(Transaction transaction) {
//		String rowKey = getRowKey();
//		transaction.deleteRow(cfName, rowKey);
//	}
//
//	/**
//	 * Get the both column families row key for this node.
//	 * 
//	 * @return the both column families row key this node
//	 */
//	private String getRowKey() {
//		return name;
//	}
//
//	/**
//	 * Get the counter values column famliy's column name for the specified
//	 * {@link ValueType}, {@link TimeGranularity} and time..
//	 * 
//	 * @param valueType the {@link ValueType}
//	 * @param granularity the {@link TimeGranularity}
//	 * @param date the time expressed in milliseconds
//	 * @param normalize if the specified time must be normalized
//	 * @return the counter values column famliy's column name for the specified
//	 *         {@link ValueType}, {@link TimeGranularity} and time
//	 */
//	private Composite getColumnName(ValueType valueType, TimeGranularity granularity, Date date, boolean normalize) {
//		Composite columnName = new Composite();
//		columnName.add(valueType.getCode());
//		columnName.add(mapTimeGranularityName(granularity));
//		columnName.addComponent(normalize ? normalizeDate(granularity, date) : date, das);
//		return columnName;
//	}
//
//	/*
//	 * (non-Javadoc)
//	 * 
//	 * @see
//	 * com.g4m3.platform.analytics.Counter#getStats(com.g4m3.platform.analytics
//	 * .Counter.StatType, com.g4m3.platform.analytics.Counter.TimeGranularity,
//	 * java.util.Date, java.util.Date)
//	 */
//	@Override
//	public Map<Date, Double> getStats(StatType stat, TimeGranularity granularity, Date start, Date finish) {
//		switch (stat) {
//		case COUNTS:
//			return getCounts(granularity, start, finish);
//		case SUMS:
//			return getSums(granularity, start, finish);
//		case SQUARES:
//			return getSquares(granularity, start, finish);
//		case MEANS:
//			return getMeans(granularity, start, finish);
//		case DEVIATIONS:
//			return getDeviations(granularity, start, finish);
//		case VARIANCES:
//			return getVariances(granularity, start, finish);
//		default:
//			throw new RuntimeException();
//		}
//	}
//
//	private Map<Date, Double> getCounts(TimeGranularity granularity, Date start, Date finish) {
//		return queryValues(ValueType.COUNTS, granularity, start, finish);
//	}
//
//	private Map<Date, Double> getSums(TimeGranularity granularity, Date start, Date finish) {
//		return queryValues(ValueType.SUMS, granularity, start, finish);
//	}
//
//	private Map<Date, Double> getSquares(TimeGranularity granularity, Date start, Date finish) {
//		return queryValues(ValueType.SQUARES, granularity, start, finish);
//	}
//
//	private Map<Date, Double> getMeans(TimeGranularity granularity, Date start, Date finish) {
//		Map<Date, Double> counts = getCounts(granularity, start, finish);
//		Map<Date, Double> sums = getSums(granularity, start, finish);
//		Map<Date, Double> means = new HashMap<Date, Double>(counts.size());
//		for (Date time : counts.keySet()) {
//			double count = counts.get(time);
//			double sum = sums.get(time);
//			double mean = sum / count;
//			means.put(time, mean);
//		}
//		return means;
//	}
//
//	private Map<Date, Double> getDeviations(TimeGranularity granularity, Date start, Date finish) {
//		Map<Date, Double> counts = getCounts(granularity, start, finish);
//		Map<Date, Double> sums = getSums(granularity, start, finish);
//		Map<Date, Double> squares = getSquares(granularity, start, finish);
//		Map<Date, Double> deviations = new HashMap<Date, Double>(counts.size());
//		for (Date time : counts.keySet()) {
//			double count = counts.get(time);
//			double sum = sums.get(time);
//			double square = squares.get(time);
//			double deviation = 0.0;
//			if (count > 1) {
//				deviation = Math.sqrt((square - sum * sum / count) / (count - 1));
//			}
//			deviations.put(time, deviation);
//		}
//		return deviations;
//	}
//
//	private Map<Date, Double> getVariances(TimeGranularity granularity, Date start, Date finish) {
//		Map<Date, Double> deviations = getDeviations(granularity, start, finish);
//		Map<Date, Double> variances = new HashMap<Date, Double>(deviations.size());
//		for (Date time : deviations.keySet()) {
//			double deviation = deviations.get(time);
//			double variance = deviation * deviation;
//			variances.put(time, variance);
//		}
//		return variances;
//
//	}
//
//	private Map<Date, Double> queryValues(ValueType valueType, TimeGranularity granularity, Date start, Date finish) {
//
//		// Build row key
//		String rowKey = getRowKey();
//
//		// Build range column names
//		Composite startColName = getColumnName(valueType, granularity, start, true);
//		Composite finishColName = getColumnName(valueType, granularity, finish, false);
//
//		// Setup query
//		SliceCounterQuery<String, Composite> query = HFactory.createCounterSliceQuery(keyspace, ss, cs);
//		query.setKey(rowKey);
//		query.setColumnFamily(cfName);
//		query.setRange(startColName, finishColName, false, 1000);
//
//		// Run query
//		QueryResult<CounterSlice<Composite>> queryResult = query.execute();
//
//		// Parse query
//		CounterSlice<Composite> counterSlice = queryResult.get();
//		List<HCounterColumn<Composite>> columns = counterSlice.getColumns();
//		Map<Date, Double> result = new HashMap<Date, Double>(columns.size());
//		for (HCounterColumn<Composite> column : columns) {
//			Composite columnName = column.getName();
//			Date counterDate = columnName.get(2, das);
//			Long counterValue = column.getValue();
//			result.put(counterDate, counterValue.doubleValue());
//		}
//
//		// Return result
//		return result;
//	}
//
//	/*
//	 * (non-Javadoc)
//	 * 
//	 * @see com.g4m3.platform.analytics.Counter#toString()
//	 */
//	@Override
//	public String toString() {
//		StringBuilder builder = new StringBuilder();
//		builder.append(name);
//		return builder.toString();
//	}
//
//	/**
//	 * Enumerated type representing the type of a value.
//	 */
//	private static enum ValueType {
//
//		COUNTS("counts"), SUMS("sums"), SQUARES("squares");
//
//		private String code;
//
//		private ValueType(String code) {
//			this.code = code;
//		}
//
//		private String getCode() {
//			return code;
//		}
//	}
//
//	private String mapTimeGranularityName(TimeGranularity granularity) {
//		switch (granularity) {
//		case ALL:
//			return "all";
//		case MINUTELY:
//			return "minutelly";
//		case HOURLY:
//			return "hourly";
//		case DAILY:
//			return "daily";
//		case MONTHLY:
//			return "monthly";
//		case YEARLY:
//			return "yearly";
//		default:
//			throw new RuntimeException();
//		}
//	}
//
//	private Date normalizeDate(TimeGranularity granularity, Date date) {
//		DateTime dateTime = new DateTime(date);
//		switch (granularity) {
//		case MINUTELY:
//			return new DateTime(dateTime.getYear(),
//			                    dateTime.getMonthOfYear(),
//			                    dateTime.getDayOfMonth(),
//			                    dateTime.getHourOfDay(),
//			                    dateTime.getMinuteOfHour()).toDate();
//		case HOURLY:
//			return new DateTime(dateTime.getYear(),
//			                    dateTime.getMonthOfYear(),
//			                    dateTime.getDayOfMonth(),
//			                    dateTime.getHourOfDay(),
//			                    0).toDate();
//		case DAILY:
//			return new DateTime(dateTime.getYear(), dateTime.getMonthOfYear(), dateTime.getDayOfMonth(), 0, 0).toDate();
//		case MONTHLY:
//			return new DateTime(dateTime.getYear(), dateTime.getMonthOfYear(), 1, 0, 0).toDate();
//		case YEARLY:
//			return new DateTime(dateTime.getYear(), 1, 1, 0, 0).toDate();
//		case ALL:
//			return new DateTime(0).toDate();
//		default:
//			throw new RuntimeException();
//		}
//	}

	/**
	 * Enumerated type representing the type of a statistical indicator.
	 */
	static enum StatType {
		COUNTS, SUMS, SQUARES, MEANS, DEVIATIONS, VARIANCES;
	}

	/**
	 * 
	 * Enumerated type representing a time's granularity.
	 * 
	 */
	static enum TimeGranularity {
		ALL, MINUTELY, HOURLY, DAILY, MONTHLY, YEARLY;
	}

}
