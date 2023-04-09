package simpledb.tx.concurrency;

import simpledb.buffer.BufferMgr;
import simpledb.file.BlockId;
import simpledb.file.FileMgr;
import simpledb.log.LogMgr;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class ConcurrencyTest {

    private static FileMgr fm;
    private static LogMgr lm;
    private static BufferMgr bm;

    public static void main(String[] args) {
        SimpleDB db = new SimpleDB("concurrencytestdir", 400, 8);
        fm = db.fileMgr();
        lm = db.logMgr();
        bm = db.bufferMgr();

        A a = new A(); new Thread(a).start();
        B b = new B(); new Thread(b).start();
        C c = new C(); new Thread(c).start();
    }

    static class A implements Runnable {
        @Override
        public void run() {
            try {
                Transaction txA = new Transaction(fm, lm, bm);
                BlockId blockId1 = new BlockId("testfile", 1);
                BlockId blockId2 = new BlockId("testfile", 2);

                txA.pin(blockId1);
                txA.pin(blockId2);

                System.out.println("Tx A: requesting slock for block 1");
                txA.getInt(blockId1, 0);
                System.out.println("Tx A: received slock for block 1");

                Thread.sleep(1000);

                System.out.println("Tx A: requesting slock for block 2");
                txA.getInt(blockId2, 0);
                System.out.println("Tx A: received slock for block 2");

                txA.commit();
            } catch (InterruptedException e) { }
        }
    }

    static class B implements Runnable {
        @Override
        public void run() {
            try {
                Transaction txB = new Transaction(fm, lm, bm);
                BlockId blockId1 = new BlockId("testfile", 1);
                BlockId blockId2 = new BlockId("testfile", 2);

                txB.pin(blockId1);
                txB.pin(blockId2);

                System.out.println("Tx B: requesting xlock for block 2");
                txB.setInt(blockId2, 0, 0, false);
                System.out.println("Tx B: received xlock for block 2");

                Thread.sleep(1000);

                System.out.println("Tx B: requesting slock for block 1");
                txB.getInt(blockId1, 0);
                System.out.println("Tx B: received slock for block 1");

                txB.commit();
            } catch (InterruptedException e) { }
        }
    }

    static class C implements Runnable {
        @Override
        public void run() {
            try {
                Transaction txC = new Transaction(fm, lm, bm);
                BlockId blockId1 = new BlockId("testfile", 1);
                BlockId blockId2 = new BlockId("testfile", 2);

                txC.pin(blockId1);
                txC.pin(blockId2);

                System.out.println("Tx C: requesting xlock for block 1");
                txC.setInt(blockId1, 0, 0, false);
                System.out.println("Tx C: received xlock for block 1");

                Thread.sleep(1000);

                System.out.println("Tx C: requesting slock for block 2");
                txC.getInt(blockId2, 0);
                System.out.println("Tx C: received slock for block 2");

                txC.commit();
            } catch (InterruptedException e) { }
        }
    }

}
