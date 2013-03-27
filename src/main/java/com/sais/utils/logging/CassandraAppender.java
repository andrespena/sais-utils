package com.sais.utils.logging;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.spi.ThrowableInformation;

import com.sais.utils.cassandra.ConsistencyLevel;
import com.sais.utils.cassandra.Keyspace;
import com.sais.utils.cassandra.Mutator;
import com.sais.utils.cassandra.NullPolicy;

/**
 * Log4j {@link Appender} using Cassandra distributed database.
 * 
 * @author andres
 * 
 */
public class CassandraAppender extends AppenderSkeleton {

	public static final String DEFAULT_HOSTS = "localhost";
	public static final String DEFAULT_KEYSPACE_NAME = "logging";
	public static final String DEFAULT_COLUMN_FAMILY_NAME = "logs";
	public static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.ONE;
	public static final Integer DEFAULT_TTL_SECONDS = null;
	public static final int DEFAULT_BUFFER_SIZE = 1;
	public static final boolean DEFAULT_SYNCHRONICITY = false;

	/* Configuration attributes to be externally supplied */
	private String hosts = DEFAULT_HOSTS;
	private String keyspaceName = DEFAULT_KEYSPACE_NAME;
	private String columnFamilyName = DEFAULT_COLUMN_FAMILY_NAME;
	private ConsistencyLevel consistencyLevel = DEFAULT_CONSISTENCY_LEVEL;
	private Integer ttlSeconds = DEFAULT_TTL_SECONDS;
	private Integer bufferSize = DEFAULT_BUFFER_SIZE;
	private Boolean synchronicity = DEFAULT_SYNCHRONICITY;

	/* Inner attributes */
	private Keyspace keyspace;
	private String hostName;
	private String hostAddress;
	private final BlockingQueue<LoggingEvent> queue = new LinkedBlockingQueue<LoggingEvent>();
	private boolean initialized = false;

	/**
	 * Constructor.
	 */
	public CassandraAppender() throws UnknownHostException {
		hostName = InetAddress.getLocalHost().getHostName();
		hostAddress = InetAddress.getLocalHost().getHostAddress();
	}

	/**
	 * Sets the Cassandra's contact point hosts.
	 * 
	 * @param hosts the host addresses separated by commas
	 */
	public void setHosts(String hosts) {
		this.hosts = hosts;
	}

	/**
	 * Sets the name of the keyspace to be used.
	 * 
	 * @param keyspaceName the name of the keyspace to be used
	 */
	public void setKeyspaceName(String keyspaceName) {
		this.keyspaceName = keyspaceName;
	}

	/**
	 * Sets the name of the column family to be used.
	 * 
	 * @param columnFamilyName the name of the column family to be used
	 */
	public void setColumnFamilyName(String columnFamilyName) {
		this.columnFamilyName = columnFamilyName;
	}

	/**
	 * Sets the time to live in seconds of the written data.
	 * 
	 * If no TTL is specified then the writings are perennial.
	 * 
	 * @param ttlSeconds the time to live in seconds
	 */
	public void setTtlSeconds(String ttlSeconds) {
		this.ttlSeconds = ttlSeconds == null ? null : Integer.parseInt(ttlSeconds);
	}

	/**
	 * Sets the number of events to be buffered before writing to database.
	 * 
	 * @param bufferSize the number of events to be buffered before writing to
	 *            database
	 */
	public void setBufferSize(int bufferSize) {
		if (bufferSize <= 0)
			throw new IllegalArgumentException("The buffer size must be greater than zero");
		this.bufferSize = bufferSize;
	}

	/**
	 * @param synchronicity the synchronicity to set
	 */
	public void setSynchronicity(Boolean synchronicity) {
		this.synchronicity = synchronicity;
	}

	/**
	 * Sets the consistency level to be used in writes.
	 * 
	 * @param consistencyLevel the consistency level to be used in writes
	 */
	public void setConsistencyLevel(String consistencyLevel) {
		if (consistencyLevel.equalsIgnoreCase("one"))
			this.consistencyLevel = ConsistencyLevel.ONE;
		else if (consistencyLevel.equalsIgnoreCase("all"))
			this.consistencyLevel = ConsistencyLevel.ALL;
		else if (consistencyLevel.equalsIgnoreCase("quorum"))
			this.consistencyLevel = ConsistencyLevel.QUORUM;
		else if (consistencyLevel.equalsIgnoreCase("any"))
			this.consistencyLevel = ConsistencyLevel.ANY;
		else if (consistencyLevel.equalsIgnoreCase("two"))
			this.consistencyLevel = ConsistencyLevel.TWO;
		else if (consistencyLevel.equalsIgnoreCase("three"))
			this.consistencyLevel = ConsistencyLevel.THREE;
		else if (consistencyLevel.equalsIgnoreCase("local_quorum"))
			this.consistencyLevel = ConsistencyLevel.LOCAL_QUORUM;
		else if (consistencyLevel.equalsIgnoreCase("each_quorum"))
			this.consistencyLevel = ConsistencyLevel.EACH_QUORUM;
		else
			throw new IllegalArgumentException("Invalid consistency level parameter " + consistencyLevel);
	}

	/***
	 * Called once all the options have been set. This will set up the link to
	 * the audit engine.
	 */
	@Override
	public void activateOptions() {
	}

	private void initialize() {
		this.keyspace = new Keyspace(hosts, keyspaceName);
		this.initialized = true;
	}

	/***
	 * Actually do the logging. The AppenderSkeleton's doAppend() method calls
	 * append() to do the actual logging after it takes care of required
	 * housekeeping operations.
	 * 
	 * @param event The event to log.
	 */
	@Override
	protected void append(LoggingEvent event) {
		if (!initialized)
			initialize();
		queue.add(event);
		if (queue.size() >= bufferSize)
			flush();
	}

	/***
	 * Release any resources allocated within the appender such as file handles,
	 * network connections, etc.
	 */
	@Override
	public void close() {
		flush();
		keyspace.shutdown();
	}

	/***
	 * Configurators call this method to determine if the appender requires a
	 * layout.
	 */
	@Override
	public boolean requiresLayout() {
		return false;
	}

	private void flush() {
		List<LoggingEvent> events = new ArrayList<LoggingEvent>();
		int numEvents = queue.drainTo(events);
		if (numEvents <= 0)
			return;
		Mutator mutator = keyspace.getMutator(consistencyLevel, ttlSeconds, NullPolicy.IGNORE);
		for (LoggingEvent event : events)
			writeEvent(mutator, event);
		if (synchronicity)
			mutator.execute();
		else
			mutator.executeAsync();

	}

	private void writeEvent(Mutator m, LoggingEvent event) {

		// Setup event's key
		final String keyName = "key";
		com.eaio.uuid.UUID eaio = new com.eaio.uuid.UUID();
		UUID keyValue = UUID.fromString(eaio.toString());

		// Append general info
		m.insertColumn(columnFamilyName, keyName, keyValue, "logger_name", event.getLoggerName())
		 .insertColumn(columnFamilyName, keyName, keyValue, "log_level", event.getLevel().toString())
		 .insertColumn(columnFamilyName, keyName, keyValue, "log_timestamp", event.getTimeStamp())
		 .insertColumn(columnFamilyName, keyName, keyValue, "context_host_name", hostName)
		 .insertColumn(columnFamilyName, keyName, keyValue, "context_host_ip", hostAddress)
		 .insertColumn(columnFamilyName, keyName, keyValue, "context_app_start_time", LoggingEvent.getStartTime())
		 .insertColumn(columnFamilyName, keyName, keyValue, "context_ndc", event.getNDC())
		 .insertColumn(columnFamilyName, keyName, keyValue, "context_thread", event.getThreadName())
		 .insertColumn(columnFamilyName,
		               keyName,
		               keyValue,
		               "context_file",
		               event.getLocationInformation().getFileName())
		 .insertColumn(columnFamilyName,
		               keyName,
		               keyValue,
		               "context_class",
		               event.getLocationInformation().getClassName())
		 .insertColumn(columnFamilyName,
		               keyName,
		               keyValue,
		               "context_method",
		               event.getLocationInformation().getMethodName())
		 .insertColumn(columnFamilyName,
		               keyName,
		               keyValue,
		               "context_line",
		               event.getLocationInformation().getLineNumber());

		// Append message info
		Object message = event.getMessage();
		if (message == null) {
			m.insertColumn(columnFamilyName, keyName, keyValue, "message_exists", false);
		} else {
			m.insertColumn(columnFamilyName, keyName, keyValue, "message_exists", true)
			 .insertColumn(columnFamilyName, keyName, keyValue, "message_class", message.getClass().getName())
			 .insertColumn(columnFamilyName, keyName, keyValue, "message_rendered", event.getRenderedMessage());
		}

		// Append exception info
		ThrowableInformation throwableInformation = event.getThrowableInformation();
		Throwable throwable = throwableInformation == null ? null : throwableInformation.getThrowable();
		if (throwable == null) {
			m.insertColumn(columnFamilyName, keyName, keyValue, "throwable_exists", false);
		} else {
			String stacktrace = StringUtils.join(event.getThrowableStrRep(), '\n');
			m.insertColumn(columnFamilyName, keyName, keyValue, "throwable_exists", true)
			 .insertColumn(columnFamilyName, keyName, keyValue, "throwable_class", throwable.getClass().getName())
			 .insertColumn(columnFamilyName, keyName, keyValue, "throwable_message", throwable.getMessage())
			 .insertColumn(columnFamilyName, keyName, keyValue, "throwable_stacktrace", stacktrace);
		}
	}

}
