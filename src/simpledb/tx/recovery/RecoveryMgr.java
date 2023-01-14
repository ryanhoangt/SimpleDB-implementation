package simpledb.tx.recovery;

import simpledb.buffer.Buffer;
import simpledb.buffer.BufferMgr;
import simpledb.log.LogMgr;
import simpledb.tx.Transaction;

public class RecoveryMgr {

    private Transaction tx;
    private int txnum;
    private LogMgr lm;
    private BufferMgr bm;

    public RecoveryMgr(Transaction tx, int txnum, LogMgr lm, BufferMgr bm) {
        this.tx = tx;
        this.txnum = txnum;
        this.lm = lm;
        this.bm = bm;
    }

    public void commit() {}

    public void rollback() {}

    public void recover() {}

    public int setInt(Buffer buffer, int offset, int newval) {

    }

    public int setString(Buffer buffer, int offset, String newval) {

    }
}
