package simpledb.plan;

import simpledb.parse.*;
import simpledb.tx.Transaction;

/**
 * The object that executes SQL statements.
 */
public class Planner {

    private QueryPlanner queryPlanner;
    private UpdatePlanner updatePlanner;

    public Planner(QueryPlanner queryPlanner, UpdatePlanner updatePlanner) {
        this.queryPlanner = queryPlanner;
        this.updatePlanner = updatePlanner;
    }

    /**
     * Creates a plan for an SQL select statement, using the supplied planner.
     * @param query the SQL query string
     * @param tx the transaction
     * @return the scan corresponding to the query plan
     */
    public Plan createQueryPlan(String query, Transaction tx) {
        Parser parser = new Parser(query);
        QueryData queryData = parser.query();
        verifyQuery(queryData);
        return queryPlanner.createPlan(queryData, tx);
    }

    /**
     * Executes an SQL insert, delete, modify, or
     * create statement.
     * The method dispatches to the appropriate method of the
     * supplied update planner,
     * depending on what the parser returns.
     * @param cmd the SQL update string
     * @param tx the transaction
     * @return an integer denoting the number of affected records
     */
    public int executeUpdate(String cmd, Transaction tx) {
        Parser parser = new Parser(cmd);
        Object data = parser.updateCmd();
        verifyUpdate(data);
        if (data instanceof InsertData)
            return updatePlanner.executeInsert((InsertData) data, tx);
        else if (data instanceof DeleteData)
            return updatePlanner.executeDelete((DeleteData) data, tx);
        else if (data instanceof ModifyData)
            return updatePlanner.executeModify((ModifyData) data, tx);
        else if (data instanceof CreateTableData)
            return updatePlanner.executeCreateTable((CreateTableData) data, tx);
        else if (data instanceof CreateViewData)
            return updatePlanner.executeCreateView((CreateViewData) data, tx);
        else if (data instanceof CreateIndexData)
            return updatePlanner.executeCreateIndex((CreateIndexData) data, tx);
        else
            return 0;
    }

    // SimpleDB does not verify queries, though it should.
    private void verifyQuery(QueryData queryData) {
    }

    // SimpleDB does not verify updates, though it should.
    private void verifyUpdate(Object data) {
    }
}
