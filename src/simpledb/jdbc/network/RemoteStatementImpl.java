package simpledb.jdbc.network;

import simpledb.plan.Plan;
import simpledb.plan.Planner;
import simpledb.tx.Transaction;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

/**
 * The RMI server-side implementation of RemoteStatement.
 */
public class RemoteStatementImpl extends UnicastRemoteObject
        implements RemoteStatement {

    private RemoteConnectionImpl rConn;
    private Planner planner;

    public RemoteStatementImpl(RemoteConnectionImpl rConn, Planner planner) throws RemoteException {
        this.rConn = rConn;
        this.planner = planner;
    }

    /**
     * Executes the specified SQL query string.
     * The method calls the query planner to create a plan
     * for the query. It then sends the plan to the
     * RemoteResultSetImpl constructor for processing.
     * @see simpledb.jdbc.network.RemoteStatement#executeQuery(java.lang.String)
     */
    @Override
    public RemoteResultSet executeQuery(String sql) throws RemoteException {
        try {
            Transaction tx = rConn.getCurrentTx();
            Plan plan = planner.createQueryPlan(sql, tx);
            return new RemoteResultSetImpl(plan, rConn);
        } catch (RuntimeException ex) {
            rConn.rollback();
            throw ex;
        }
    }

    /**
     * Executes the specified SQL update command.
     * The method sends the command to the update planner,
     * which executes it.
     * @see simpledb.jdbc.network.RemoteStatement#executeUpdate(java.lang.String)
     */
    @Override
    public int executeUpdate(String sql) throws RemoteException {
        try {
            Transaction tx = rConn.getCurrentTx();
            int result = planner.executeUpdate(sql, tx);
            rConn.commit();
            return result;
        } catch (RuntimeException ex) {
            rConn.rollback();
            throw ex;
        }
    }

    @Override
    public void close() throws RemoteException {
    }
}
