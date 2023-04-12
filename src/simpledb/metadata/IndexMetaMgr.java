package simpledb.metadata;

import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.record.TableScan;
import simpledb.tx.Transaction;

import java.util.HashMap;
import java.util.Map;

class IndexMetaMgr {

    private Layout layout;
    private TableMetaMgr tblMetaMgr;
    private StatMetaMgr statMetaMgr;

    public IndexMetaMgr(boolean isNew, TableMetaMgr tblMetaMgr, StatMetaMgr statMetaMgr, Transaction tx) {
        if (isNew) {
            Schema schema = new Schema();
            schema.addStringField("index_name", TableMetaMgr.MAX_NAME_LEN);
            schema.addStringField("table_name", TableMetaMgr.MAX_NAME_LEN);
            schema.addStringField("field_name", TableMetaMgr.MAX_NAME_LEN);
            tblMetaMgr.createTable("idx_cat", schema, tx);
        }
        this.tblMetaMgr = tblMetaMgr;
        this.statMetaMgr = statMetaMgr;
        this.layout = tblMetaMgr.getLayout("idx_cat", tx);
    }

    public void createIndex(String idxName, String tblName, String fldName, Transaction tx) {
        TableScan iScan = new TableScan(tx, "idx_cat", layout);
        iScan.insert();
        iScan.setString("index_name", idxName);
        iScan.setString("table_name", tblName);
        iScan.setString("field_name", fldName);
        iScan.close();
    }

    /**
     * Return a map containing the index info for all indexes
     * on the specified table.
     * @param tblName the name of the table
     * @param tx the calling tx
     * @return a map of IndexInfo objects, keyed by their field names.
     */
    public Map<String, IndexInfo> getIndexInfo(String tblName, Transaction tx) {
        Map<String, IndexInfo> res = new HashMap<>();
        TableScan iScan = new TableScan(tx, "idx_cat", layout);
        while (iScan.next()) {
            if (iScan.getString("table_name").equals(tblName)) {
                String idxName = iScan.getString("index_name");
                String fldName = iScan.getString("field_name");
                Layout tblLayout = tblMetaMgr.getLayout(tblName, tx);
                StatInfo tblSi = statMetaMgr.getStatInfo(tblName, tblLayout, tx);
                IndexInfo ii = new IndexInfo(idxName, fldName, tblLayout.getSchema(), tx, tblSi);
                res.put(fldName, ii);
            }
        }
        iScan.close();
        return res;
    }
}
