package simpledb.jdbc.embedded;

import simpledb.jdbc.StatementAdapter;
import simpledb.plan.Plan;
import simpledb.plan.Planner;
import simpledb.tx.Transaction;

import java.sql.SQLException;

/**
 * The embedded implementation of Statement.
 *
 * +-------------------+----------------+
 * | JDBC Interface    | SimpleDB Class |
 * +-------------------+----------------+
 * | Statement         | Planner, Plan  |
 * +-------------------+----------------+
 */
class EmbeddedStatement extends StatementAdapter {
    private EmbeddedConnection conn;
    private Planner planner;

    public EmbeddedStatement(EmbeddedConnection conn, Planner planner) {
        this.conn = conn;
        this.planner = planner;
    }

    /**
     * Executes the specified SQL query string.
     * Calls the query planner to create a plan for the query,
     * and sends the plan to the ResultSet constructor for processing.
     * Rolls back and throws an SQLException if it cannot create the plan.
     */
    @Override
    public EmbeddedResultSet executeQuery(String sql) throws SQLException {
        try {
            Transaction tx = conn.getCurrentTx();
            Plan plan = planner.createQueryPlan(sql, tx);
            return new EmbeddedResultSet(plan, conn);
        } catch (RuntimeException ex) {
            conn.rollback();
            throw new SQLException(ex);
        }
    }

    /**
     * Executes the specified SQL update command by sending
     * the command to the update planner and then committing.
     * Rolls back and throws an SQLException on an error.
     */
    @Override
    public int executeUpdate(String sql) throws SQLException {
        try {
            Transaction tx = conn.getCurrentTx();
            int result = planner.executeUpdate(sql, tx);
            conn.commit();
            return result;
        } catch (RuntimeException ex) {
            conn.rollback();
            throw new SQLException(ex);
        }
    }

    @Override
    public void close() throws SQLException {
    }
}
