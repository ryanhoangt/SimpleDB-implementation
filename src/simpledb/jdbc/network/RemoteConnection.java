package simpledb.jdbc.network;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * The RMI remote interface corresponding to Connection.
 * The methods are identical to those of Connection,
 * except that they throw RemoteExceptions instead of SQLExceptions.
 */
public interface RemoteConnection extends Remote {
    RemoteStatement createStatement() throws RemoteException;
    void close() throws RemoteException;
}
