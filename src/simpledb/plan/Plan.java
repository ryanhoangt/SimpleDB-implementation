package simpledb.plan;

import simpledb.query.Scan;
import simpledb.record.Schema;

public interface Plan {

    /**
     * Opens a scan corresponding to this plan.
     * The scan will be positioned before its first record.
     * @return a scan
     */
    Scan open();

    /**
     * Returns an estimate of the number of block accesses
     * that will occur when the scan is read to completion.
     * @return the estimated number of block accesses
     */
    int estimatedNumBlocks();

    /**
     * Returns an estimate of the number of records
     * in the query's output table.
     * @return the estimated number of output records
     */
    int estimatedNumRecordsOutput();

    /**
     * Returns an estimate of the number of distinct values
     * for the specified field in the query's output table.
     * @param fldName the name of a field
     * @return the estimated number of distinct field values in the output
     */
    int estimatedNumDistinctValues(String fldName);

    /**
     * Returns the schema of the query.
     * @return the query's schema
     */
    Schema getSchema();
}
