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

public class CassandraAppender extends AppenderSkeleton {
	
	private static final int MUTATION_SIZE = 10;

	private String hosts;
	private String keyspaceName;
	private String tableName;
	private ConsistencyLevel consistencyLevel;
	private Integer ttlSeconds;
	private String appName;

	private Keyspace keyspace;
	private String hostName;
	private String hostAddress;
	private final BlockingQueue<LoggingEvent> queue = new LinkedBlockingQueue<LoggingEvent>();

	private boolean initialized = false;

	/***
	 * Constructor.
	 */
	public CassandraAppender() throws UnknownHostException {
		hostName = InetAddress.getLocalHost().getHostName();
		hostAddress = InetAddress.getLocalHost().getHostAddress();
	}

	public void setHosts(String hosts) {
		this.hosts = hosts;
	}

	public void setKeyspaceName(String keyspaceName) {
		this.keyspaceName = keyspaceName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public void setTtlSeconds(String ttlSeconds) {
		this.ttlSeconds = Integer.parseInt(ttlSeconds);
	}

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

	public void setAppName(String appName) {
		this.appName = appName;
	}

	/***
	 * Called once all the options have been set. This will set up the link to
	 * the audit engine.
	 */
	@Override
	public void activateOptions() {
	}

	private void initialize() {
		System.out.println("INITIALIZING");
		this.keyspace = new Keyspace(hosts, keyspaceName);
		this.initialized = true;
		System.out.println("INITIALIZED");
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
		if (!initialized) initialize();
		queue.add(event);
		if (queue.size() >= MUTATION_SIZE) flush();
	}

	/***
	 * Release any resources allocated within the appender such as file handles,
	 * network connections, etc.
	 */
	@Override
	public void close() {
		System.out.println("CLOSING");
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
		queue.drainTo(events);
		if (events.size() > 0) System.out.println("WRITING " + events.size());
		Mutator mutator = keyspace.getMutator(consistencyLevel, ttlSeconds, NullPolicy.IGNORE, true, false);
		for (LoggingEvent event : events) {
			writeEvent(mutator, event);
		}
		mutator.execute();
	}
	
	private void writeEvent(Mutator m, LoggingEvent event) {
		
		// Setup event's key
		final String keyName = "key";
		com.eaio.uuid.UUID eaio = new com.eaio.uuid.UUID();
		UUID keyValue = UUID.fromString(eaio.toString());

		// Append general info
		m.insertColumn(tableName, keyName, keyValue, "log_level", event.getLevel().toString())
		 .insertColumn(tableName, keyName, keyValue, "log_timestamp", event.getTimeStamp())
		 .insertColumn(tableName, keyName, keyValue, "context_host_name", hostName)
		 .insertColumn(tableName, keyName, keyValue, "context_host_ip", hostAddress)
		 .insertColumn(tableName, keyName, keyValue, "context_app_name", appName)
		 .insertColumn(tableName, keyName, keyValue, "context_app_start_time", LoggingEvent.getStartTime())
		 .insertColumn(tableName, keyName, keyValue, "context_ndc", event.getNDC())
		 .insertColumn(tableName, keyName, keyValue, "context_thread", event.getThreadName())
		 .insertColumn(tableName, keyName, keyValue, "context_file", event.getLocationInformation().getFileName())
		 .insertColumn(tableName, keyName, keyValue, "context_class", event.getLocationInformation().getClassName())
		 .insertColumn(tableName, keyName, keyValue, "context_method", event.getLocationInformation().getMethodName())
		 .insertColumn(tableName, keyName, keyValue, "context_line", event.getLocationInformation().getLineNumber());

		// Append message info
		Object message = event.getMessage();
		if (message == null) {
			m.insertColumn(tableName, keyName, keyValue, "message_exists", false);
		} else {
			m.insertColumn(tableName, keyName, keyValue, "message_exists", true)
			 .insertColumn(tableName, keyName, keyValue, "message_class", message.getClass().getName())
		     .insertColumn(tableName, keyName, keyValue, "message_rendered", event.getRenderedMessage());
		}

		// Append exception info
		ThrowableInformation throwableInformation = event.getThrowableInformation();
		Throwable throwable = throwableInformation == null ? null
		        : throwableInformation.getThrowable();
		if (throwable == null) {
			m.insertColumn(tableName, keyName, keyValue, "throwable_exists", false);
		} else {
			m.insertColumn(tableName, keyName, keyValue, "throwable_exists", true)
			 .insertColumn(tableName, keyName, keyValue, "throwable_class", throwable.getClass().getName())
			 .insertColumn(tableName, keyName, keyValue, "throwable_message", throwable.getMessage())
			 .insertColumn(tableName, keyName, keyValue, "throwable_stacktrace", StringUtils.join(event.getThrowableStrRep(), '\n'));
		}
	}

}
