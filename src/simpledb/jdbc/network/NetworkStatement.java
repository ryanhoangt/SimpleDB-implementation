package simpledb.jdbc.network;

import simpledb.jdbc.StatementAdapter;

import java.sql.ResultSet;
import java.sql.SQLException;


/**
 * An adapter class that wraps RemoteStatement.
 * Its methods do nothing except transform RemoteExceptions
 * into SQLExceptions.
 */
public class NetworkStatement extends StatementAdapter {

    private RemoteStatement rstmt;

    public NetworkStatement(RemoteStatement rstmt) {
        this.rstmt = rstmt;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        try {
            RemoteResultSet rrs = rstmt.executeQuery(sql);
            return new NetworkResultSet(rrs);
        }
        catch(Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        try {
            return rstmt.executeUpdate(sql);
        }
        catch(Exception e) {
            throw new SQLException(e);
        }
    }
}
