package simpledb.record;

import simpledb.file.BlockId;
import simpledb.query.Constant;
import simpledb.query.UpdateScan;
import simpledb.tx.Transaction;

import java.sql.Types;

/**
 * An abstraction of arbitrary large collection of records.
 */
public class TableScan implements UpdateScan {

    private Transaction tx;
    private String filename;
    private Layout layout;
    private RecordPage curRecordPage;
    private int curSlot;

    public TableScan(Transaction tx, String tableName, Layout layout) {
        this.tx = tx;
        this.filename = tableName + ".tbl";
        this.layout = layout;

        if (tx.size(filename) == 0)
            moveToNewBlock();
        else
            moveToBlock(0);
    }

    @Override
    public void close() {
        if (curRecordPage != null)
            tx.unpin(curRecordPage.getBlockId());
    }

    @Override
    public boolean hasField(String fieldName) {
        return layout.getSchema().hasField(fieldName);
    }

    // methods that establish the current record
    @Override
    public void beforeFirst() {
        moveToBlock(0);
    }

    @Override
    public boolean next() {
        curSlot = curRecordPage.nextAfter(curSlot);

        while (curSlot < 0) {
            if (atLastBlock())
                return false;

            moveToBlock(curRecordPage.getBlockId().number() + 1);
            curSlot = curRecordPage.nextAfter(curSlot);
        }
        return true;
    }

    @Override
    public void moveToRID(RID rid) {
        close();

        BlockId blockId = new BlockId(filename, rid.getBlkNum());
        curRecordPage = new RecordPage(tx, blockId, layout);
        curSlot = rid.getSlot();
    }

    @Override
    public void insert() {
        curSlot = curRecordPage.insertAfter(curSlot);

        while (curSlot < 0) {
            if (atLastBlock())
                moveToNewBlock();
            else
                moveToBlock(curRecordPage.getBlockId().number() + 1);

            curSlot = curRecordPage.insertAfter(curSlot);
        }
    }

    // methods that access the current record's content
    @Override
    public int getInt(String fieldName) {
        return curRecordPage.getInt(curSlot, fieldName);
    }

    @Override
    public String getString(String fieldName) {
        return curRecordPage.getString(curSlot, fieldName);
    }

    @Override
    public Constant getVal(String fieldName) {
        if (layout.getSchema().type(fieldName) == Types.INTEGER)
            return new Constant(getInt(fieldName));
        else
            return new Constant(getString(fieldName));
    }

    @Override
    public void setInt(String fieldName, int val) {
        curRecordPage.setInt(curSlot, fieldName, val);
    }

    @Override
    public void setString(String fieldName, String val) {
        curRecordPage.setString(curSlot, fieldName, val);
    }

    @Override
    public void setVal(String fieldName, Constant val) {
        if (layout.getSchema().type(fieldName) == Types.INTEGER)
            setInt(fieldName, val.asInt());
        else
            setString(fieldName, val.asString());
    }

    @Override
    public RID getRID() {
        return new RID(curRecordPage.getBlockId().number(), curSlot);
    }

    @Override
    public void delete() {
        curRecordPage.delete(curSlot);
    }

    // ===== PRIVATE HELPER METHODS =====

    private void moveToNewBlock() {
        close();

        BlockId newBlockId = tx.append(filename);
        curRecordPage = new RecordPage(tx, newBlockId, layout);
        curRecordPage.format();
        curSlot = -1;
    }

    private void moveToBlock(int blkNum) {
        close();

        BlockId blockId = new BlockId(filename, blkNum);
        curRecordPage = new RecordPage(tx, blockId, layout);
        curSlot = -1;
    }

    private boolean atLastBlock() {
        return curRecordPage.getBlockId().number() == tx.size(filename) - 1;
    }

}
