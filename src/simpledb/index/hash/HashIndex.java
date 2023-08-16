package simpledb.index.hash;

import simpledb.index.Index;
import simpledb.query.Constant;
import simpledb.record.Layout;
import simpledb.record.RID;
import simpledb.record.TableScan;
import simpledb.tx.Transaction;

/**
 * A static hash implementation of the Index interface.
 * A fixed number of buckets is allocated (currently, 100),
 * and each bucket is implemented as a file of index records.
 */
public class HashIndex implements Index {
    public static int NUM_BUCKETS = 100;

    private Transaction tx;
    private String idxName;
    private Layout idxLayout;
    private Constant searchKey = null;
    private TableScan ts = null;

    public HashIndex(Transaction tx, String idxName, Layout idxLayout) {
        this.tx = tx;
        this.idxName = idxName;
        this.idxLayout = idxLayout;
    }

    /**
     * Positions the index before the first index record
     * having the specified search key.
     * The method hashes the search key to determine the bucket,
     * and then opens a table scan on the file
     * corresponding to the bucket.
     * The table scan for the previous bucket (if any) is closed.
     */
    @Override
    public void beforeFirst(Constant searchKey) {
        close();
        this.searchKey = searchKey;
        int bucket = searchKey.hashCode() % NUM_BUCKETS;
        String tblName = idxName + bucket;
        ts = new TableScan(tx, tblName, idxLayout);
    }

    /**
     * Moves to the next record having the search key.
     * The method loops through the table scan for the bucket,
     * looking for a matching record, and returning false
     * if there are no more such records.
     */
    @Override
    public boolean next() {
        while (ts.next())
            if (ts.getVal("dataval").equals(searchKey))
                return true;
        return false;
    }

    /**
     * Retrieves the dataRID from the current record
     * in the table scan for the bucket.
     */
    @Override
    public RID getDataRid() {
        int blkNum = ts.getInt("block");
        int slot = ts.getInt("slot");
        return new RID(blkNum, slot);
    }

    /**
     * Inserts a new record into the table scan for the bucket.
     */
    @Override
    public void insert(Constant dataval, RID datarid) {
        beforeFirst(dataval);
        ts.insert();
        ts.setInt("block", datarid.getBlkNum());
        ts.setInt("slot", datarid.getSlot());
        ts.setVal("dataval", dataval);
    }

    /**
     * Deletes the specified record from the table scan for
     * the bucket.  The method starts at the beginning of the
     * scan, and loops through the records until the
     * specified record is found.
     */
    @Override
    public void delete(Constant dataval, RID datarid) {
        beforeFirst(dataval);
        while (next()) {
            if (getDataRid().equals(datarid)) {
                ts.delete();
                return;
            }
        }
    }

    /**
     * Closes the index by closing the current table scan.
     */
    @Override
    public void close() {
        if (ts != null)
            ts.close();
    }

    public static int searchCost(int numBlks, int rpb) {
        return numBlks / NUM_BUCKETS;
    }
}
