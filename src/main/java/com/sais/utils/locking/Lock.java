package com.sais.utils.locking;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public class Lock {

	private static final char NAME_SEPARATOR = '_';

    /** The Hazelcast's distributed map's name */
    private static final String MAP_NAME = "locks";

    /** The Hazelcast's distributed map */
    private IMap<String, ?> map;

    /** The lock's identifying name */
    private String name;

    /** The locking start time in milliseconds */
    private long start;

    /** The output logger's name */
    private static final String LOGGER_NAME = "LOCKS";

    /** The output logger */
    private Logger logger;

    /**
     * Constructor.
     * 
     * @param hazelcastInstance the Hazecast's instance to be used
     * @param preffix the prefix of lock's name
     * @param arguments
     */
    public Lock(HazelcastInstance hazelcastInstance, String preffix, Object... arguments) {

        // Build lock's name
        StringBuilder nameBuilder = new StringBuilder(preffix);
        for (Object argument : arguments) {
            nameBuilder.append(NAME_SEPARATOR);
            nameBuilder.append(argument);
        }
        name = nameBuilder.toString();

        // Get Hazelcast's distributed map
        map = hazelcastInstance.getMap(MAP_NAME);

        // Set start time for logging
        start = System.currentTimeMillis();

        // Setup logger
        logger = Logger.getLogger(LOGGER_NAME);
    }

    public void lock() {
        map.lock(name);
        System.out.println("LOCKED    " + name);
        log(Level.DEBUG, "LOCKED    " + name);
    }

    public boolean tryLock() {

        // Try lock
        boolean locked = map.tryLock(name);

        // Log locking attempt result
        if (locked) {
            log(Level.DEBUG, "LOCKED    " + name);
        } else {
            log(Level.DEBUG, "MISSED    " + name);
        }

        // Return locking attempt result
        return locked;
    }

    public void unlock() {

        // Remove entry from distributed map
        map.remove(name);

        // Unlock distributed map's entry
        map.unlock(name);

        // Log locking time
        long time = System.currentTimeMillis() - start;
        log(Level.DEBUG, "FREED     " + name + " " + time + "ms");
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