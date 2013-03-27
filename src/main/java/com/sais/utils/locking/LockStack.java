package com.sais.utils.locking;

import java.util.LinkedList;

import com.hazelcast.core.IMap;

/**
 * Class representing a stack of globally distributed {@link Lock}s
 * 
 * @author andres
 *
 */
public class LockStack {

    private IMap<String, ?> map;
    
    /** The managed locks */
    private LinkedList<Lock> locks;

	LockStack(IMap<String, ?> map) {
	    super();
	    this.map = map;
	    this.locks = new LinkedList<Lock>();
    }
    
	/**
	 * Adds to this stack a distributed global lock identified by the
	 * specified prefix and optional arguments.
	 * 
	 * @param prefix the prefix of the lock's name
	 * @param arguments the optional arguments of the lock's name
	 */
	public void pushLock(String prefix, Object... arguments) {
		Lock lock = new Lock(map, prefix, arguments);
		lock.lock();
		locks.push(lock);
	}

	/**
	 * Unlocks and removes the last distributed global lock added to this
	 * stack.
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
