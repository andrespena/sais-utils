package com.sais.utils.locking;

import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.Join;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MulticastConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public class LockService {

	private static final String MAP_NAME = "locks";

	private HazelcastInstance hazelcastInstance;

	private IMap<String, ?> map;

	public LockService(String name,
	                   String password,
	                   int port,
	                   String multicastGroup,
	                   int multicastPort,
	                   int backupCount) {

		GroupConfig groupConfig = new GroupConfig();
		groupConfig.setName(name);
		groupConfig.setPassword(password);

		TcpIpConfig tcpIpConfig = new TcpIpConfig();
		tcpIpConfig.setEnabled(false);

		MulticastConfig multicastConfig = new MulticastConfig();
		multicastConfig.setEnabled(true);
		multicastConfig.setMulticastGroup(multicastGroup);
		multicastConfig.setMulticastPort(port);

		Join join = new Join();
		join.setTcpIpConfig(tcpIpConfig);
		join.setMulticastConfig(multicastConfig);

		NetworkConfig networkConfig = new NetworkConfig();
		networkConfig.setPort(port);
		networkConfig.setPortAutoIncrement(false);
		networkConfig.setJoin(join);

		MapConfig mapConfig = new MapConfig();
		mapConfig.setName(MAP_NAME);
		mapConfig.setBackupCount(backupCount);
		mapConfig.getMaxSizeConfig().setSize(0);
		mapConfig.setReadBackupData(false);
		mapConfig.setEvictionPolicy("NONE");
		mapConfig.setMergePolicy("hz.ADD_NEW_ENTRY");

		Config cfg = new Config();
		cfg.setGroupConfig(groupConfig);
		cfg.setNetworkConfig(networkConfig);
		cfg.addMapConfig(mapConfig);

		this.hazelcastInstance = Hazelcast.newHazelcastInstance(cfg);
		this.map = hazelcastInstance.getMap(MAP_NAME);
	}

	/**
	 * 
	 * @param prefix
	 * @param arguments
	 * @return
	 */
	public Lock getLock(String prefix, Object... arguments) {
		return new Lock(map, prefix, arguments);
	}

	/**
	 * 
	 * @return
	 */
	public LockStack getLockStack() {
		return new LockStack(map);
	}

}
