package simpledb.jdbc.network;

import simpledb.jdbc.ResultSetMetaDataAdapter;

import java.sql.SQLException;

/**
 * An adapter class that wraps RemoteMetaData.
 * Its methods do nothing except transform RemoteExceptions
 * into SQLExceptions.
 */
public class NetworkMetaData extends ResultSetMetaDataAdapter {

    private RemoteMetaData rmd;

    public NetworkMetaData(RemoteMetaData rmd) {
        this.rmd = rmd;
    }

    @Override
    public int getColumnCount() throws SQLException {
        try {
            return rmd.getColumnCount();
        }
        catch(Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        try {
            return rmd.getColumnDisplaySize(column);
        }
        catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        try {
            return rmd.getColumnName(column);
        }
        catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        try {
            return rmd.getColumnType(column);
        }
        catch (Exception e) {
            throw new SQLException(e);
        }
    }
}
