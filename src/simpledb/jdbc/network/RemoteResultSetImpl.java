package simpledb.jdbc.network;

import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.record.Schema;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

/**
 * The RMI server-side implementation of RemoteResultSet.
 */
public class RemoteResultSetImpl extends UnicastRemoteObject
        implements RemoteResultSet {

    private Scan sc;
    private Schema schema;
    private RemoteConnectionImpl rConn;

    public RemoteResultSetImpl(Plan plan, RemoteConnectionImpl rConn) throws RemoteException {
        this.sc = plan.open();
        this.schema = plan.getSchema();
        this.rConn = rConn;
    }

    @Override
    public boolean next() throws RemoteException {
        // TODO:
        return false;
    }

    @Override
    public int getInt(String fldName) throws RemoteException {
        // TODO:
        return 0;
    }

    @Override
    public String getString(String fldName) throws RemoteException {
        // TODO:
        return null;
    }

    @Override
    public RemoteMetaData getMetaData() throws RemoteException {
        return null;
    }

    @Override
    public void close() throws RemoteException {
        // TODO:
    }
}
