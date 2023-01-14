package simpledb.tx;

import simpledb.buffer.BufferMgr;
import simpledb.file.BlockId;
import simpledb.file.FileMgr;
import simpledb.log.LogMgr;
import simpledb.server.SimpleDB;

public class TxTest {

    public static void main(String[] args) {
        SimpleDB db = new SimpleDB("txtestdir", 400, 8);
        FileMgr fm = db.fileMgr();
        LogMgr lm = db.logMgr();
        BufferMgr bm = db.bufferMgr();

        Transaction tx1 = new Transaction(fm, lm, bm);
        BlockId blkId = new BlockId("testfile", 1);
        tx1.pin(blkId);
        // Do not log initial block values.
        tx1.setInt(blkId, 80, 1, false);
        tx1.setString(blkId, 40, "one", false);
        tx1.commit();

        Transaction tx2 = new Transaction(fm, lm, bm);
        tx2.pin(blkId);
        int ival = tx2.getInt(blkId, 80);
        String sval = tx2.getString(blkId, 40);
        System.out.println("initial value at location 80 = " + ival);
        System.out.println("initial value at location 40 = " + sval);
        int newival = ival + 1;
        String newsval = sval + "!";
        tx2.setInt(blkId, 80, newival, true);
        tx2.setString(blkId, 40, newsval, true);
        tx2.commit();

        Transaction tx3 = new Transaction(fm, lm, bm);
        tx3.pin(blkId);
        System.out.println("new value at location 80 = " + tx3.getInt(blkId, 80));
        System.out.println("new value at location 40 = " + tx3.getString(blkId, 40));
        tx3.setInt(blkId, 80, 9999, true);
        System.out.println("pre-rollback value at location 80 = " + tx3.getInt(blkId, 80));
        tx3.rollback();

        Transaction tx4 = new Transaction(fm, lm, bm);
        tx4.pin(blkId);
        System.out.println("post-rollback at location 80 = " + tx4.getInt(blkId, 80));
        tx4.commit();
    }

}
