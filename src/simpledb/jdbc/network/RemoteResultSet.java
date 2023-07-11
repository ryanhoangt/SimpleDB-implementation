package simpledb.jdbc.network;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * The RMI remote interface corresponding to ResultSet.
 * The methods are identical to those of ResultSet,
 * except that they throw RemoteExceptions instead of SQLExceptions.
 */
public interface RemoteResultSet extends Remote {
    boolean next()                      throws RemoteException;
    int getInt(String fldName)          throws RemoteException;
    String getString(String fldName)    throws RemoteException;
    RemoteMetaData getMetaData()        throws RemoteException;
    void close()                        throws RemoteException;
}
