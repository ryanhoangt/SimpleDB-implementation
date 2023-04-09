package simpledb.tx.recovery;

import simpledb.buffer.BufferMgr;
import simpledb.file.BlockId;
import simpledb.file.FileMgr;
import simpledb.file.Page;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class RecoveryTest {

    public static FileMgr fm;
    public static BufferMgr bm;
    private static SimpleDB db;
    private static BlockId blk0, blk1;

    public static void main(String[] args) {
        db = new SimpleDB("recoverytestdir", 400, 8);
        fm = db.fileMgr();
        bm = db.bufferMgr();
        blk0 = new BlockId("testfile", 0);
        blk1 = new BlockId("testfile", 1);

        if (fm.lengthInBlocks("testfile") == 0) {
            initialize();
            modify();
        } else {
            recover();
        }
    }

    private static void initialize() {
        Transaction tx1 = db.newTx();
        Transaction tx2 = db.newTx();
        tx1.pin(blk0);
        tx2.pin(blk1);

        int pos = 0;
        for (int i = 0; i < 6; i++) {
            tx1.setInt(blk0, pos, pos, false);
            tx2.setInt(blk1, pos, pos, false);
            pos += Integer.BYTES;
        }
        tx1.setString(blk0, 30, "abc", false);
        tx2.setString(blk1, 30, "def", false);

        tx1.commit();
        tx2.commit();
        printValues("After initialization:");
    }

    private static void modify() {
        Transaction tx3 = db.newTx();
        Transaction tx4 = db.newTx();
        tx3.pin(blk0);
        tx4.pin(blk1);
        int pos = 0;
        for (int i = 0; i < 6; i++) {
            tx3.setInt(blk0, pos, pos+100, true);
            tx4.setInt(blk1, pos, pos+100, true);
            pos += Integer.BYTES;
        }
        tx3.setString(blk0, 30, "uvw", true);
        tx4.setString(blk1, 30, "xyz", true);
        bm.flushAll(3);
        bm.flushAll(4);
        printValues("After modification:");

        tx3.rollback();
        printValues("After rollback:");
        // tx3 stops without committing or rolling back
        // all its changes should be undone during recovery
    }

    private static void recover() {
        Transaction tx = db.newTx();
        tx.recover();
        printValues("After recovery:");
    }

    private static void printValues(String msg) {
        System.out.println(msg);

        Page p0 = new Page(fm.blockSize());
        Page p1 = new Page(fm.blockSize());
        fm.read(blk0, p0);
        fm.read(blk1, p1);

        int pos = 0;
        for (int i = 0; i < 6; i++) {
            System.out.print(p0.getInt(pos) + " ");
            System.out.print(p1.getInt(pos) + " ");
            pos += Integer.BYTES;
        }

        System.out.print(p0.getString(30) + " ");
        System.out.print(p1.getString(30) + " ");
        System.out.println();
    }
}
