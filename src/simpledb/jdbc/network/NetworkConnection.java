package simpledb.jdbc.network;

import simpledb.jdbc.ConnectionAdapter;

import java.rmi.RemoteException;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * An adapter class that wraps RemoteConnection.
 * Its methods do nothing except transform RemoteExceptions
 * into SQLExceptions.
 */
public class NetworkConnection extends ConnectionAdapter {

    private RemoteConnection rconn;

    public NetworkConnection(RemoteConnection rconn) {
        this.rconn = rconn;
    }

    @Override
    public Statement createStatement() throws SQLException {
        try {
            RemoteStatement rstmt = rconn.createStatement();
            return new NetworkStatement(rstmt);
        } catch (RemoteException ex) {
            throw new SQLException(ex);
        }
    }

    @Override
    public void close() throws SQLException {
        try {
            rconn.close();
        } catch (RemoteException ex) {
            throw new SQLException(ex);
        }
    }
}
