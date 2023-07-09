package simpledb.plan;

import simpledb.parse.QueryData;
import simpledb.tx.Transaction;

/**
 * The interface implemented by planners for
 * the SQL select statement.
 */
public interface QueryPlanner {

    /**
     * Creates a plan for the parsed query.
     * @param queryData the parsed representation of the query
     * @param tx the calling transaction
     * @return a plan for that query
     */
    public Plan createPlan(QueryData queryData, Transaction tx);
}
