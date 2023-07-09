package simpledb.plan;

import simpledb.metadata.MetaDataMgr;
import simpledb.metadata.StatInfo;
import simpledb.query.Scan;
import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.record.TableScan;
import simpledb.tx.Transaction;

public class TablePlan implements Plan {
    private Transaction tx;
    private String tblName;
    private Layout layout;
    private StatInfo si;

    public TablePlan(Transaction tx, String tblName, MetaDataMgr metaDataMgr) {
        this.tx = tx;
        this.tblName = tblName;
        this.layout = metaDataMgr.getLayout(tblName, tx);
        this.si = metaDataMgr.getStatInfo(tblName, layout, tx);
    }

    @Override
    public Scan open() {
        return new TableScan(tx, tblName, layout);
    }

    @Override
    public int estimatedNumBlocks() {
        return si.estimatedNumBlocks();
    }

    @Override
    public int estimatedNumRecordsOutput() {
        return si.estimatedNumRecords();
    }

    @Override
    public int estimatedNumDistinctValues(String fldName) {
        return si.estimatedNumDistinctValues(fldName);
    }

    @Override
    public Schema getSchema() {
        return layout.getSchema();
    }
}
