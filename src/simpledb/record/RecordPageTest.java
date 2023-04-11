package simpledb.record;

import simpledb.file.BlockId;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class RecordPageTest {

    public static void main(String[] args) {
        SimpleDB db = new SimpleDB("recordpagetestdir", 400, 8);
        Transaction tx = db.newTx();

        Schema schema = new Schema();
        schema.addIntField("A");
        schema.addStringField("B", 10);
        Layout layout = new Layout(schema);

        BlockId blockId = tx.append("testfile");
        tx.pin(blockId);
        RecordPage rp = new RecordPage(tx, blockId, layout);
        rp.format();

        System.out.println("Filling the page with random records...");
        int slot = rp.insertAfter(-1);
        while (slot >= 0) {
            int n = (int) Math.round(Math.random() * 50);
            rp.setInt(slot, "A", n);
            rp.setString(slot, "B", "rec " + n);
            System.out.println("Inserting into slot " + slot + ": {" + n + ", " + "rec " + n + "}");
            slot = rp.insertAfter(slot);
        }

        System.out.println("Deleting these records, whose A-values are less than 25.");
        int count = 0;
        slot = rp.nextAfter(-1);
        while (slot >= 0) {
            int a = rp.getInt(slot, "A");
            String b = rp.getString(slot, "B");
            if (a < 25) {
                count++;
                System.out.println("Deleting slot " + slot + ": {" + a + ", " + b + "}");
                rp.delete(slot);
            }
            slot = rp.nextAfter(slot);
        }
        System.out.println(count + " values under 25 were deleted.\n");

        System.out.println("Here are the remaining records:");
        slot = rp.nextAfter(-1);
        while (slot >= 0) {
            int a = rp.getInt(slot, "A");
            String b = rp.getString(slot, "B");
            System.out.println("Slot " + slot + ": {" + a + ", " + b + "}");
            slot = rp.nextAfter(slot);
        }
        tx.unpin(blockId);
        tx.commit();
    }
}
