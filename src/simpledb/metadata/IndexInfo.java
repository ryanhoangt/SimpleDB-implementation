package simpledb.metadata;

import simpledb.index.Index;
import simpledb.index.btree.BTreeIndex;
import simpledb.index.hash.HashIndex;
import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

import java.sql.Types;

public class IndexInfo {
    private String idxName, fldName;
    private Schema tblSchema;
    private Transaction tx;
    private StatInfo si;
    private Layout idxLayout;

    public IndexInfo(String idxName, String fldName, Schema schema, Transaction tx, StatInfo si) {
        this.idxName = idxName;
        this.fldName = fldName;
        this.tblSchema = schema;
        this.tx = tx;
        this.si = si;
        this.idxLayout = createIdxLayout();
    }

    public Index open() {
        return new HashIndex(tx, idxName, idxLayout);
//        return new BTreeIndex(tx, idxName, idxLayout);
    }

    /**
     * Estimate the number of block accesses required to find all
     * index records having a particular search key.
     * The method uses the table's metadata to estimate the size of
     * the index file and the number of index records per block.
     * It then passes this information to the traversalCost method of the
     * appropriate index type, which provides the estimate.
     * @return the number of block accesses required to traverse the index.
     */
    public int estimatedNumBlockAccesses() {
        int rpb = tx.blockSize() / idxLayout.getSlotSize();
        int numBlks = si.estimatedNumRecords() / rpb;
        return HashIndex.searchCost(numBlks, rpb);
//        return BTreeIndex.searchCost(numBlks, rpb);
    }

    /**
     * Return the estimated number of records having a search key.
     * This value is the same as doing a SELECT query; that is, the number
     * of records in the table divided by the number of distinct values of
     * the indexed field.
     */
    public int estimatedNumRecordsReturned() {
        return si.estimatedNumRecords() / si.estimatedNumDistinctValues(fldName);
    }

    /**
     * Return the distinct values for a specified field in the underlying
     * table, or 1 for the indexed field.
     * @return
     */
    public int estimatedNumDistinctValues(String fieldName) {
        return fldName.equals(fieldName) ? 1 : si.estimatedNumDistinctValues(fieldName);
    }

    /**
     * Return the layout of the index records.
     * The Schema consists of the data's RID (represented by two ints -
     * block number and slot number) and the data's value of the indexed
     * field.
     * Schema information about the indexed field on the original table
     * is obtained via the table's schema.
     * @return the layout of the index records.
     */
    private Layout createIdxLayout() {
        Schema schema = new Schema();
        schema.addIntField("blk_num");
        schema.addIntField("slot");
        if (tblSchema.type(fldName) == Types.INTEGER)
            schema.addIntField("data_val");
        else {
            int fldLen = tblSchema.length(fldName);
            schema.addStringField("data_val", fldLen);
        }
        return new Layout(schema);
    }
}
