package simpledb.index.btree;

import simpledb.index.Index;
import simpledb.record.Layout;
import simpledb.tx.Transaction;

public class BTreeIndex implements Index {
    private Transaction tx;
    private String idxName;
    private Layout idxLayout;

    public BTreeIndex(Transaction tx, String idxName, Layout idxLayout) {
        this.tx = tx;
        this.idxName = idxName;
        this.idxLayout = idxLayout;
    }

    public static int searchCost(int numBlks, int rpb) {
        // TODO:
        return -1;
    }
}
