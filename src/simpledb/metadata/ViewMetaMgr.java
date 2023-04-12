package simpledb.metadata;

import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.record.TableScan;
import simpledb.tx.Transaction;

class ViewMetaMgr {
    private static final int MAX_VIEWDEF_LEN = 100; // max view def chars
    private TableMetaMgr tblMetaMgr;

    public ViewMetaMgr(boolean isNew, TableMetaMgr tblMetaMgr, Transaction tx) {
        this.tblMetaMgr = tblMetaMgr;

        if (isNew) {
            Schema schema = new Schema();
            schema.addStringField("view_name", TableMetaMgr.MAX_NAME_LEN);
            schema.addStringField("view_def", MAX_VIEWDEF_LEN);
            tblMetaMgr.createTable("view_cat", schema, tx);
        }
    }

    public void createView(String vName, String vDef, Transaction tx) {
        Layout layout = tblMetaMgr.getLayout("view_cat", tx);
        TableScan vScan = new TableScan(tx, "view_cat", layout);
        vScan.insert();
        vScan.setString("view_name", vName);
        vScan.setString("view_def", vDef);
        vScan.close();
    }

    /**
     * Get view definition for the view with a specified name.
     * @return view def if exists, null otherwise
     */
    public String getViewDef(String vName, Transaction tx) {
        String res = null;
        Layout layout = tblMetaMgr.getLayout("view_cat", tx);
        TableScan vScan = new TableScan(tx, "view_cat", layout);

        while (vScan.next()) {
            if (vScan.getString("view_name").equals(vName)) {
                res = vScan.getString("view_def");
                break;
            }
        }
        vScan.close();
        return res;
    }
}
