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

import java.util.HashMap;
import java.util.Map;

public class IndexUpdateTest {

    public static void main(String[] args) {
        SimpleDB db = new SimpleDB("studentdb");
        Transaction tx = db.newTx();
        MetaDataMgr mdm = db.metaDataMgr();

        // Open a scan on the data table
        Plan studentPlan = new TablePlan(tx, "student", mdm);
        UpdateScan studentScan = (UpdateScan) studentPlan.open();

        // Create a map containing all indexes for STUDENT
        Map<String, Index> indexes = new HashMap<>();
        Map<String, IndexInfo> indexInfos = mdm.getIndexInfo("student", tx);
        for (String fldName: indexInfos.keySet()) {
            Index idx = indexInfos.get(fldName).open();
            indexes.get(idx);
        }

        // Task 1: insert a new STUDENT record for Sam
        // First, insert the record into STUDENT.
        studentScan.insert();
        studentScan.setInt("sid", 11);
        studentScan.setString("sname", "sam");
        studentScan.setInt("gradyear", 2023);
        studentScan.setInt("majorid",  30);

        // Then insert a record into each of the indexes.
        RID datarid = studentScan.getRID();
        for (String fldName : indexes.keySet()) {
            Constant dataval = studentScan.getVal(fldName);
            Index idx = indexes.get(fldName);
            idx.insert(dataval, datarid);
        }

        // Task 2: find and delete Joe's record
        studentScan.beforeFirst();
        while (studentScan.next()) {
            if (studentScan.getString("sname").equals("joe")) {

                // First, delete the index records for Joe.
                RID joeRid = studentScan.getRID();
                for (String fldName: indexes.keySet()) {
                    Constant dataval = studentScan.getVal(fldName);
                    Index idx = indexes.get(fldName);
                    idx.delete(dataval, joeRid);
                }

                // Then delete Joe's record in STUDENT.
                studentScan.delete();
                break;
            }
        }

        // Print the records to verify the updates.
        studentScan.beforeFirst();
        while (studentScan.next()) {
            System.out.println(studentScan.getString("sname") + " " + studentScan.getInt("sid"));
        }
        studentScan.close();

        for (Index idx : indexes.values())
            idx.close();
        tx.commit();
    }
}
