package simpledb.jdbc.embedded;

import simpledb.jdbc.ResultSetAdapter;
import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.record.Schema;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * The embedded implementation of ResultSet.
 *
 * +-------------------+----------------+
 * | JDBC Interface    | SimpleDB Class |
 * +-------------------+----------------+
 * | ResultSet         | Scan           |
 * +-------------------+----------------+
 */
public class EmbeddedResultSet extends ResultSetAdapter {

    private Scan sc;
    private Schema schema;
    private EmbeddedConnection conn;

    public EmbeddedResultSet(Plan plan, EmbeddedConnection conn) {
        this.sc = plan.open();
        this.schema = plan.getSchema();
        this.conn = conn;
    }

    /**
     * Moves to the next record in the result set,
     * by moving to the next record in the saved scan.
     */
    @Override
    public boolean next() throws SQLException {
        try {
            return sc.next();
        } catch (RuntimeException ex) {
            conn.rollback();
            throw new SQLException(ex);
        }
    }

    /**
     * Returns the integer value of the specified field,
     * by returning the corresponding value on the saved scan.
     */
    @Override
    public int getInt(String columnLabel) throws SQLException {
        try {
            columnLabel = columnLabel.toLowerCase();
            return sc.getInt(columnLabel);
        } catch (RuntimeException ex) {
            conn.rollback();
            throw new SQLException(ex);
        }
    }

    /**
     * Returns the integer value of the specified field,
     * by returning the corresponding value on the saved scan.
     */
    @Override
    public String getString(String columnLabel) throws SQLException {
        try {
            columnLabel = columnLabel.toLowerCase();
            return sc.getString(columnLabel);
        } catch (RuntimeException ex) {
            conn.rollback();
            throw new SQLException(ex);
        }
    }

    /**
     * Returns the result set's metadata,
     * by passing its schema into the EmbeddedMetaData constructor.
     */
    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return new EmbeddedMetaData(schema);
    }

    /**
     * Closes the result set by closing its scan, and commits.
     */
    @Override
    public void close() throws SQLException {
        sc.close();
        conn.commit();
    }
}
