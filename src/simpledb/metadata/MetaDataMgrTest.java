package simpledb.metadata;

import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.record.TableScan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

import java.sql.Types;
import java.util.Map;

public class MetaDataMgrTest {

    public static void main(String[] args) {
        SimpleDB db = new SimpleDB("metadatamgrtestdir", 400, 8);
        Transaction tx = db.newTx();
        MetaDataMgr mdm = new MetaDataMgr(true, tx);

        Schema sch = new Schema();
        sch.addIntField("A");
        sch.addStringField("B", 10);

        // Part 1: Table Metadata
        mdm.createTable("MyTable", sch, tx);
        Layout layout = mdm.getLayout("MyTable", tx);
        int size = layout.getSlotSize();

        Schema sch2 = layout.getSchema();
        System.out.println("MyTable has slot size: " + size);
        System.out.println("Its fields are: ");
        for (String fldName: sch2.fields()) {
            String type;
            if (sch2.type(fldName) == Types.INTEGER)
                type = "INT";
            else {
                int strLen = sch2.length(fldName);
                type = "VARCHAR(" + strLen + ")";
            }
            System.out.println(fldName + ": " + type);
        }

        // Part 2: Statistics Metadata
        TableScan ts = new TableScan(tx, "MyTable", layout);
        for (int i = 0; i < 50; i++) {
            ts.insert();
            int n = (int) Math.round(Math.random() * 50);
            ts.setInt("A", n);
            ts.setString("B", "rec " + n);
        }
        StatInfo si = mdm.getStatInfo("MyTable", layout, tx);
        System.out.println("B(MyTable) = " + si.estimatedNumBlocks());
        System.out.println("R(MyTable) = " + si.estimatedNumRecords());
        System.out.println("V(MyTable,A) = " + si.estimatedNumDistinctValues("A"));
        System.out.println("V(MyTable,B) = " + si.estimatedNumDistinctValues("B"));

        // Part 3: View Metadata
        String viewDef = "select B from MyTable where A = 1";
        mdm.createView("viewA", viewDef, tx);
        String v = mdm.getViewDef("viewA", tx);
        System.out.println("View def = " + v);

        // Part 4: Index Metadata
        mdm.createIndex("indexA", "MyTable", "A", tx);
        mdm.createIndex("indexB", "MyTable", "B", tx);
        Map<String, IndexInfo> idxMap = mdm.getIndexInfo("MyTable", tx);

        IndexInfo ii = idxMap.get("A");
        System.out.println("B(indexA) = " + ii.estimatedNumBlockAccesses());
        System.out.println("R(indexA) = " + ii.estimatedNumRecordsReturned());
        System.out.println("V(indexA,A) = " + ii.estimatedNumDistinctValues("A"));
        System.out.println("V(indexA,B) = " + ii.estimatedNumDistinctValues("B"));

        ii = idxMap.get("B");
        System.out.println("B(indexB) = " + ii.estimatedNumBlockAccesses());
        System.out.println("R(indexB) = " + ii.estimatedNumRecordsReturned());
        System.out.println("V(indexB,A) = " + ii.estimatedNumDistinctValues("A"));
        System.out.println("V(indexB,B) = " + ii.estimatedNumDistinctValues("B"));

        tx.commit();
    }
}
