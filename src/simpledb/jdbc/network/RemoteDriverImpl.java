package simpledb.jdbc.network;

import simpledb.server.SimpleDB;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

/**
 * The RMI server-side implementation of RemoteDriver.
 */
public class RemoteDriverImpl extends UnicastRemoteObject
                                implements RemoteDriver {

    private SimpleDB db;

    public RemoteDriverImpl(SimpleDB db) throws RemoteException {
        this.db = db;
    }

    /**
     * Creates a new RemoteConnectionImpl object and
     * returns it.
     * @see simpledb.jdbc.network.RemoteDriver#connect()
     */
    @Override
    public RemoteConnection connect() throws RemoteException {
        return new RemoteConnectionImpl(db);
    }
}
