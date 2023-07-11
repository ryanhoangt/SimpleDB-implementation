package simpledb.jdbc.embedded;

import simpledb.jdbc.ConnectionAdapter;
import simpledb.plan.Planner;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

import java.sql.SQLException;

/**
 * The embedded implementation of Connection.
 *
 * +-------------------+----------------+
 * | JDBC Interface    | SimpleDB Class |
 * +-------------------+----------------+
 * | Connection        | Transaction    |
 * +-------------------+----------------+
 */
class EmbeddedConnection extends ConnectionAdapter {
    private SimpleDB simpleDB;
    private Transaction currentTx;
    private Planner planner;

    public EmbeddedConnection(SimpleDB simpleDB) {
        this.simpleDB = simpleDB;
        this.currentTx = simpleDB.newTx();
        this.planner = simpleDB.planner();
    }

    /**
     * Creates a new Statement for this connection.
     */
    @Override
    public EmbeddedStatement createStatement() throws SQLException {
        return new EmbeddedStatement(this, planner);
    }

    /**
     * Closes the connection by committing the current transaction.
     */
    @Override
    public void close() throws SQLException {
        currentTx.commit();
    }

    /**
     * Commits the current transaction and begins a new one.
     */
    @Override
    public void commit() throws SQLException {
        currentTx.commit();
        currentTx = simpleDB.newTx();
    }

    /**
     * Rolls back the current transaction and begins a new one.
     */
    @Override
    public void rollback() throws SQLException {
        currentTx.rollback();
        currentTx = simpleDB.newTx();
    }

    /**
     * Returns the transaction currently associated with
     * this connection. Not public. Called by other JDBC classes.
     * @return the transaction associated with this connection
     */
    Transaction getCurrentTx() {
        return currentTx;
    }
}
