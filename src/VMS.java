/*
 * Autoscanling Program Simulation and Tuning.
 * Author: Wenyan Liu
 */
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;


public interface VMS extends Remote {
	public int getRole(int id) throws RemoteException;
	public void addRequest(Cloud.FrontEndOps.Request r) throws RemoteException;
	public void doneRequest(Cloud.FrontEndOps.Request r) throws RemoteException;
	public void setQueueLength(int vm_id, int newQueueLen)throws RemoteException;
	public Cloud.FrontEndOps.Request getRequest() throws RemoteException;
	public void registerVM(int id, int role) throws RemoteException;
	public void checkMiddleLoad(int id) throws RemoteException;
	public Cloud.DatabaseOps getCacheDB() throws RemoteException;
	public void stop() throws RemoteException;
}

