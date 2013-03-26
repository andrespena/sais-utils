package com.sais.utils.locking;

import java.util.LinkedList;

import com.hazelcast.core.HazelcastInstance;

public class Locks {

    /** The Hazelcast instance */
    private HazelcastInstance hazelcast;
    
    /** The managed locks */
    private LinkedList<Lock> locks;

	public Locks(HazelcastInstance hazelcast) {
	    super();
	    this.hazelcast = hazelcast;
	    this.locks = new LinkedList<Lock>();
    }
    
	/**
	 * Adds to this transaction a distributed global lock identified by the
	 * specified prefix and optional arguments.
	 * 
	 * @param prefix the prefix of the lock's name
	 * @param arguments the optional arguments of the lock's name
	 */
	public void pushLock(String prefix, Object... arguments) {
		Lock lock = new Lock(hazelcast, prefix, arguments);
		lock.lock();
		locks.push(lock);
	}

	/**
	 * Unlocks and removes the last distributed global lock added to this
	 * transaction.
	 */
	public void popLock() {
		Lock lock = locks.pop();
		lock.unlock();
	}

	/**
	 * Unlocks and removes all the distributed global locks added to this
	 * transaction.
	 */
	public void releaseLocks() {
		while (!locks.isEmpty()) {
			popLock();
		}
	}

}
