package simpledb.metadata;

import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

import java.util.Map;

/**
 * A Façade class providing simple API to access the four
 * underlying metadata manager.
 */
public class MetaDataMgr {
    private static TableMetaMgr tblMgr;
    private static ViewMetaMgr viewMgr;
    private static StatMetaMgr statMgr;
    private static IndexMetaMgr idxMgr;

    public MetaDataMgr(boolean isNew, Transaction tx) {
        tblMgr = new TableMetaMgr(isNew, tx);
        viewMgr = new ViewMetaMgr(isNew, tblMgr, tx);
        statMgr = new StatMetaMgr(tblMgr, tx);
        idxMgr = new IndexMetaMgr(isNew, tblMgr, statMgr, tx);
    }

    public void createTable(String tblName, Schema schema, Transaction tx) {
        tblMgr.createTable(tblName, schema, tx);
    }

    public Layout getLayout(String tblName, Transaction tx) {
        return tblMgr.getLayout(tblName, tx);
    }

    public void createView(String viewName, String viewDef, Transaction tx) {
        viewMgr.createView(viewName, viewDef, tx);
    }

    public String getViewDef(String viewName, Transaction tx) {
        return viewMgr.getViewDef(viewName, tx);
    }

    public void createIndex(String idxName, String tblName, String fldName, Transaction tx) {
        idxMgr.createIndex(idxName, tblName, fldName, tx);
    }

    public Map<String, IndexInfo> getIndexInfo(String tblName, Transaction tx) {
        return idxMgr.getIndexInfo(tblName, tx);
    }

    public StatInfo getStatInfo(String tblName, Layout layout, Transaction tx) {
        return statMgr.getStatInfo(tblName, layout, tx);
    }

}
