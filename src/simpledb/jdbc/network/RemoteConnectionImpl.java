package simpledb.jdbc.network;

import simpledb.plan.Planner;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

/**
 * The RMI server-side implementation of RemoteConnection.
 */
public class RemoteConnectionImpl extends UnicastRemoteObject
        implements RemoteConnection {

    private SimpleDB db;
    private Transaction currentTx;
    private Planner planner;

    /**
     * Creates a remote connection
     * and begins a new transaction for it.
     * @throws RemoteException
     */
    RemoteConnectionImpl(SimpleDB db) throws RemoteException {
        this.db = db;
        this.currentTx = db.newTx();
        this.planner = db.planner();
    }

    @Override
    public RemoteStatement createStatement() throws RemoteException {
        return new RemoteStatementImpl(this, planner);
    }

    @Override
    public void close() throws RemoteException {
        currentTx.commit();
    }

    // ===== METHODS USED BY SERVER-SIDE CLASSES =====

    /**
     * Returns the transaction currently associated with
     * this connection.
     * @return the transaction associated with this connection
     */
    Transaction getCurrentTx() {
        return currentTx;
    }

    /**
     * Commits the current transaction,
     * and begins a new one.
     */
    void commit() {
        currentTx.commit();
        currentTx = db.newTx();
    }

    /**
     * Rolls back the current transaction,
     * and begins a new one.
     */
    void rollback() {
        currentTx.rollback();
        currentTx = db.newTx();
    }
}
