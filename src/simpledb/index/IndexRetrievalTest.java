package simpledb.index;

import simpledb.metadata.IndexInfo;
import simpledb.metadata.MetaDataMgr;
import simpledb.plan.Plan;
import simpledb.plan.TablePlan;
import simpledb.query.Constant;
import simpledb.query.UpdateScan;
import simpledb.record.RID;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

import java.util.Map;

public class IndexRetrievalTest {

    public static void main(String[] args) {
        SimpleDB db = new SimpleDB("studentdb");
        Transaction tx = db.newTx();
        MetaDataMgr mdm = db.metaDataMgr();

        // Open a scan on the data table
        Plan studentPlan = new TablePlan(tx, "student", mdm);
        UpdateScan studentScan = (UpdateScan) studentPlan.open();

        // Open the index on MajorId
        Map<String, IndexInfo> indexes = mdm.getIndexInfo("student", tx);
        IndexInfo ii = indexes.get("majorid");
        Index idx = ii.open();

        // Retrieve all index records having a dataval of 20
        idx.beforeFirst(new Constant(20));
        while (idx.next()) {
            // Use the datarid to go to the corresponding STUDENT record
            RID datarid = idx.getDataRid();
            studentScan.moveToRID(datarid);
            System.out.println(studentScan.getString("sname"));
        }

        // Close the index and the data table
        idx.close();
        studentScan.close();
        tx.commit();
    }
}
