package simpledb.metadata;

import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.record.TableScan;
import simpledb.tx.Transaction;

import java.util.HashMap;
import java.util.Map;

class TableMetaMgr {
    public static final int MAX_NAME_LEN = 16;
    private Layout tcatLayout, fcatLayout;

    /**
     * Create a new TABLE catalog manager.
     * @param isNew whether to create two catalog tables
     * @param tx the startup transaction
     */
    public TableMetaMgr(boolean isNew, Transaction tx) {
        Schema tcatSchema = new Schema();
        tcatSchema.addStringField("tbl_name", MAX_NAME_LEN);
        tcatSchema.addIntField("slot_size");
        tcatLayout = new Layout(tcatSchema);

        Schema fcatSchema = new Schema();
        fcatSchema.addStringField("tbl_name", MAX_NAME_LEN);
        fcatSchema.addStringField("fld_name", MAX_NAME_LEN);
        fcatSchema.addIntField("fld_type");
        fcatSchema.addIntField("fld_length");
        fcatSchema.addIntField("fld_offset");
        fcatLayout = new Layout(fcatSchema);

        if (isNew) {
            createTable("tbl_cat", tcatSchema, tx);
            createTable("fld_cat", fcatSchema, tx);
        }
    }

    /**
     * Create a new table having the specified name and schema.
     */
    public void createTable(String tblName, Schema schema, Transaction tx) {
        Layout layout = new Layout(schema);

        // insert one record into tbl_cat
        TableScan tScan = new TableScan(tx, "tbl_cat", tcatLayout);
        tScan.insert();
        tScan.setString("tbl_name", tblName);
        tScan.setInt("slot_size", layout.getSlotSize());
        tScan.close();

        // insert one record into fld_cat for each field
        TableScan fScan = new TableScan(tx, "fld_cat", fcatLayout);
        for (String fldName: schema.fields()) {
            fScan.insert();
            fScan.setString("tbl_name", tblName);
            fScan.setString("fld_name", fldName);
            fScan.setInt("fld_type", schema.type(fldName));
            fScan.setInt("fld_length", schema.length(fldName));
            fScan.setInt("fld_offset", layout.offsetOfField(fldName));
        }
        fScan.close();
    }

    /**
     * Retrieve the layout of the specified table.
     * @param tblName name of table
     * @param tx current tx
     * @return the table's stored metadata
     */
    public Layout getLayout(String tblName, Transaction tx) {
        int slotSize = -1;
        TableScan tScan = new TableScan(tx, "tbl_cat", tcatLayout);
        while (tScan.next()) {
            if (tScan.getString("tbl_name").equals(tblName)) {
                slotSize = tScan.getInt("slot_size");
                break;
            }
        }
        tScan.close();

        Schema schema = new Schema();
        Map<String, Integer> offsets = new HashMap<>();
        TableScan fScan = new TableScan(tx, "fld_cat", fcatLayout);
        while (fScan.next()) {
            if (fScan.getString("tbl_name").equals(tblName)) {
                String fldName = fScan.getString("fld_name");
                int fldType = fScan.getInt("fld_type");
                int fldLen = fScan.getInt("fld_length");
                int offset = fScan.getInt("fld_offset");
                offsets.put(fldName, offset);
                schema.addField(fldName, fldType, fldLen);
            }
        }
        fScan.close();
        return new Layout(schema, offsets, slotSize);
    }

}
