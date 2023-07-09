package simpledb.query;

import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.record.TableScan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

import java.util.Arrays;
import java.util.List;

public class ScanTest1 {

    public static void main(String[] args) {
        SimpleDB db = new SimpleDB("scan1testdir");
        Transaction tx = db.newTx();

        Schema sch1 = new Schema();
        sch1.addIntField("A");
        sch1.addStringField("B", 9);
        Layout layout = new Layout(sch1);
        UpdateScan s1 = new TableScan(tx, "T", layout);

        s1.beforeFirst();
        int n = 200;
        System.out.println("Inserting " + n + " random records.");
        for (int i = 0; i < n; i++) {
            s1.insert();
            int k = (int) Math.round(Math.random() * 50);
            s1.setInt("A", k);
            s1.setString("B", "rec"+k);
        }
        s1.close();

        Scan s2 = new TableScan(tx, "T", layout);
        // selecting all records where A=10
        Constant c = new Constant(10);
        Term t = new Term(new Expression("A"), new Expression(c));
        Predicate pred = new Predicate(t);
        System.out.println("The predicate is " + pred);
        Scan s3 = new SelectScan(s2, pred);
        List<String> fields = Arrays.asList("B");
        Scan s4 = new ProjectScan(s3, fields);
        while (s4.next())
            System.out.println(s4.getString("B"));
        s4.close();
        tx.commit();
    }

}
