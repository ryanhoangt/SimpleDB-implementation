package simpledb.tx.recovery;

import simpledb.file.BlockId;
import simpledb.file.Page;
import simpledb.log.LogMgr;
import simpledb.tx.Transaction;

public class SetStringRecord implements LogRecord {

    private int txnum, offset;
    private String val;
    private BlockId blk;

    /**
     * Create a new setstring log record.
     * @param p the bytebuffer containing the log values
     */
    public SetStringRecord(Page p) {
        int tpos = Integer.BYTES;
        this.txnum = p.getInt(tpos);
        int fpos = tpos + Integer.BYTES;
        String filename = p.getString(fpos);
        int bpos = fpos + Page.maxLength(filename.length());
        int blknum = p.getInt(bpos);
        this.blk = new BlockId(filename, blknum);
        int opos = bpos + Integer.BYTES;
        this.offset = p.getInt(opos);
        int vpos = opos + Integer.BYTES;
        this.val = p.getString(vpos);
    }

    @Override
    public int op() {
        return SETSTRING;
    }

    @Override
    public int txNumber() {
        return txnum;
    }

    @Override
    public void undo(Transaction tx) {
        tx.pin(blk);
        tx.setString(blk, offset, val, false); // don't log the undo!
        tx.unpin(blk);
    }

    @Override
    public String toString() {
        return "<SETSTRING " + txnum + " " + blk + " " + offset + " " + val + ">";
    }

    /**
     * A static method to write a setInt record to the log.
     * This log record contains the SETINT operator,
     * followed by the transaction id, the filename, number,
     * and offset of the modified block, and the previous
     * integer value at that offset.
     * @return the LSN of the last log value
     */
    public static int writeToLog(LogMgr lm, int txnum, BlockId blkId, int offset, String val) {
        int tpos = Integer.BYTES;
        int fpos = tpos + Integer.BYTES;
        int bpos = fpos + Page.maxLength(blkId.fileName().length());
        int opos = bpos + Integer.BYTES;
        int vpos = opos + Integer.BYTES;
        int reclen = vpos + Page.maxLength(val.length());
        byte[] rec = new byte[reclen];
        Page p = new Page(rec);
        p.setInt(0, SETSTRING);
        p.setInt(tpos, txnum);
        p.setString(fpos, blkId.fileName());
        p.setInt(bpos, blkId.number());
        p.setInt(opos, offset);
        p.setString(vpos, val);
        return lm.append(rec);
    }

}
