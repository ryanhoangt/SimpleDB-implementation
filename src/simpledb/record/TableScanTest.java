package simpledb.record;

import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class TableScanTest {

    public static void main(String[] args) {
        SimpleDB db = new SimpleDB("tablescantestdir", 400, 8);
        Transaction tx = db.newTx();

        Schema schema = new Schema();
        schema.addIntField("A");
        schema.addStringField("B", 10);
        Layout layout = new Layout(schema);
        TableScan ts = new TableScan(tx, "test", layout);

        for (int i = 0; i < 50; i++) {
            ts.insert();
            int n = (int) Math.round(Math.random() * 50);
            ts.setInt("A", n);
            ts.setString("B", "rec " + n);
        }

        System.out.println("Deleting these records, whose A-values are less than 25...");
        int count = 0;
        ts.beforeFirst();
        while (ts.next()) {
            int a = ts.getInt("A");
            String b = ts.getString("B");
            if (a < 25) {
                count++;
                System.out.println("Delete slot " + ts.getRID() + ": {" + a + ", " + b + "}");
                ts.delete();
            }
        }
        System.out.println(count + " values under 25 were deleted.\n");

        System.out.println("Here are the remaining records:");
        ts.beforeFirst();
        while (ts.next()) {
            int a = ts.getInt("A");
            String b = ts.getString("B");
            System.out.println("Slot " + ts.getRID() + ": {" + a + ", " + b + "}");
        }
        ts.close();
        tx.commit();
    }
}
