package simpledb.jdbc.network;

import simpledb.jdbc.ResultSetAdapter;

import java.rmi.RemoteException;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * An adapter class that wraps RemoteResultSet.
 * Its methods do nothing except transform RemoteExceptions
 * into SQLExceptions.
 */
public class NetworkResultSet extends ResultSetAdapter {

    private RemoteResultSet rrs;

    public NetworkResultSet(RemoteResultSet rrs) {
        this.rrs = rrs;
    }

    @Override
    public void close() throws SQLException {
        try {
            rrs.close();
        }
        catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        try {
            return rrs.getInt(columnLabel);
        }
        catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        try {
            RemoteMetaData rmd = rrs.getMetaData();
            return new NetworkMetaData(rmd);
        }
        catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        try {
            return rrs.getString(columnLabel);
        }
        catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public boolean next() throws SQLException {
        try {
            return rrs.next();
        } catch (RemoteException e) {
            throw new SQLException(e);
        }
    }
}
