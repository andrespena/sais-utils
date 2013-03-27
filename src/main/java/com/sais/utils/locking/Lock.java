package com.sais.utils.locking;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.hazelcast.core.IMap;

/**
 * Class representing a globally distributed {@link Lock}.
 * 
 * @author andres
 * 
 */
public class Lock {

	private static final char NAME_SEPARATOR = '_';

	/** The Hazelcast's distributed map */
	private IMap<String, ?> map;

	/** The lock's identifying name */
	private String name;

	/** The locking start time in milliseconds */
	private long startTime;

	/** The output logger's name */
	private static final String LOGGER_NAME = "com.sais.utils.locking";

	/** The output logger */
	private Logger logger;

	/**
	 * Constructor.
	 * 
	 * @param imap the Hazecast's distributed {@link IMap} to be used
	 * @param prefix the prefix of lock's name
	 * @param arguments
	 */
	Lock(IMap<String, ?> imap, String prefix, Object... arguments) {

		// Build lock's name
		StringBuilder nameBuilder = new StringBuilder(prefix);
		for (Object argument : arguments) {
			nameBuilder.append(NAME_SEPARATOR);
			nameBuilder.append(argument);
		}
		name = nameBuilder.toString();

		// Get Hazelcast's distributed map
		this.map = imap;

		// Set start time for logging
		startTime = System.currentTimeMillis();

		// Setup logger
		logger = Logger.getLogger(LOGGER_NAME);
	}

	/**
	 * Acquires this lock.
	 * 
	 * If the lock is not available then the current thread becomes disabled for
	 * thread scheduling purposes and lies dormant until the lock has been
	 * acquired.
	 * 
	 * Locks are re-entrant so if the key is locked N times then it should be
	 * unlocked N times before another thread can acquire it.
	 */
	public void lock() {
		map.lock(name);
		log(Level.DEBUG, "LOCKED\t" + name);
	}

	/**
	 * Tries to acquire this lock.
	 * 
	 * If the lock is not available then the current thread doesn't wait and
	 * returns false immediately.
	 * 
	 * @return {@code true} if lock is acquired, {@code false} otherwise.
	 */
	public boolean tryLock() {

		// Try lock
		boolean locked = map.tryLock(name);

		// Log locking attempt result
		if (locked) {
			log(Level.DEBUG, "LOCKED\t" + name);
		} else {
			log(Level.DEBUG, "MISSED\t" + name);
		}

		// Return locking attempt result
		return locked;
	}

	/**
	 * Releases this lock. It never blocks and returns immediately. 
	 */
	public void unlock() {

		// Remove entry from distributed map
		map.remove(name);

		// Unlock distributed map's entry
		map.unlock(name);

		// Log locking time
		long lockingTime = System.currentTimeMillis() - startTime;
		log(Level.DEBUG, "FREED\t" + name + " " + lockingTime + "ms");
	}

	/**
	 * Writes log.
	 * 
	 * @param level the logging level
	 * @param message the message to be logged
	 */
	private void log(Level level, String message) {
		logger.log(level, message);
	}

}