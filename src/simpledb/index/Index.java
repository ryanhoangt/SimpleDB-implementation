package simpledb.index;

import simpledb.query.Constant;
import simpledb.record.RID;

/**
 * This interface contains methods to traverse an index.
 */
public interface Index {

    /**
     * Positions the index before the first record
     * having the specified search key.
     * @param searchKey the search key value.
     */
    void beforeFirst(Constant searchKey);

    /**
     * Moves the index to the next record having the
     * search key specified in the beforeFirst method.
     * Returns false if there are no more such index records.
     * @return false if no other index records have the search key.
     */
    boolean next();

    /**
     * Returns the dataRID value stored in the current index record.
     * @return the dataRID stored in the current index record.
     */
    RID getDataRid();

    /**
     * Inserts an index record having the specified
     * dataval and dataRID values.
     * @param dataval the dataval in the new index record.
     * @param datarid the dataRID in the new index record.
     */
    void insert(Constant dataval, RID datarid);

    /**
     * Deletes the index record having the specified
     * dataval and dataRID values.
     * @param dataval the dataval of the deleted index record
     * @param datarid the dataRID of the deleted index record
     */
    void delete(Constant dataval, RID datarid);

    /**
     * Closes the index.
     */
    void close();
}
