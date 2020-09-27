
/*
 * Autoscanling Program Simulation and Tuning.
 * Author: Wenyan Liu
 */

import java.util.concurrent.ConcurrentHashMap;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.RemoteException;

public class DB extends UnicastRemoteObject implements Cloud.DatabaseOps {
	private static Cloud.DatabaseOps defaultDB;
	private static ConcurrentHashMap<String, String> cache = new ConcurrentHashMap<String, String>();
	private static ServerLib SL = null;

	private int hit = 0;
	private int miss = 0;
    private static final Boolean DEBUG = false;

    /**
     * log function for debug.
     * if enabled by DEBUG parameter, the commandline will print log msg.
     * @param msg msg to print out
     */
	private static void log(String msg) {
		if (DEBUG) {
			long time = System.currentTimeMillis();
			System.err.println("[" + time + "]" + "<CacheDB>" + msg);
		}
	}

	/**
     * Construction function for DB.
     * get defualt DB.
     * @param host the host of Cloud RMI
     * @param port the port of Cloud RMI
     */
	public DB(String host, String port) throws RemoteException {
		SL = new ServerLib(host, Integer.parseInt(port));
		defaultDB = SL.getDB();
	}

	/**
     * get function for cache.
     * if enabled by DEBUG parameter, the commandline will print log msg.
     * @param key the key of the request to fetch from the cache
     * @return the item fetched from the database if cache miss, the item form catch if hit.
     */
	public String get(String key) throws RemoteException {
		if (cache.containsKey(key)) {
		  hit++;
		  log("cache hit: " + hit);
			return cache.get(key);
		} else {
		  miss++;
      log("cache miss: " + hit);
			String val = defaultDB.get(key);
			cache.put(key, val);
			return val;
		}
	}

	/**
     * set function for deatabase.
     * write directly to datqabase.
     * @param key key of the request
     * @param val val of the request
     * @param auth the auth of the request
     */
	public boolean set(String key, String val, String auth) throws RemoteException {
		return defaultDB.set(key, val, auth);
	}

	/**
     * transaction function for deatabase.
     * transaction directly to datqabase.
     * @param item item of the request
     * @param price price of the request
     * @param qty qty of the request
     */
	public boolean transaction(String item, float price, int qty) throws RemoteException {
		return defaultDB.transaction(item, price, qty);
	}
}