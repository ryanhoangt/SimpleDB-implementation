package simpledb.record;

import simpledb.file.BlockId;
import simpledb.tx.Transaction;

import java.sql.Types;

public class RecordPage {
    public static final int EMPTY = 0, USED = 1;

    private Transaction tx;
    private BlockId blockId;
    private Layout layout;

    public RecordPage(Transaction tx, BlockId blockId, Layout layout) {
        this.tx = tx;
        this.blockId = blockId;
        this.layout = layout;
        tx.pin(blockId);
    }

    public int getInt(int slot, String fieldName) {
        int fieldPos = bytesOffsetOfSlot(slot) + layout.offsetOfField(fieldName);
        return tx.getInt(blockId, fieldPos);
    }

    public String getString(int slot, String fieldName) {
        int fieldPos = bytesOffsetOfSlot(slot) + layout.offsetOfField(fieldName);
        return tx.getString(blockId, fieldPos);
    }

    public void setInt(int slot, String fieldName, int val) {
        int fieldPos = bytesOffsetOfSlot(slot) + layout.offsetOfField(fieldName);
        tx.setInt(blockId, fieldPos, val, true);
    }

    public void setString(int slot, String fieldName, String val) {
        int fieldPos = bytesOffsetOfSlot(slot) + layout.offsetOfField(fieldName);
        tx.setString(blockId, fieldPos, val, true);
    }

    public void delete(int slot) {
        setFlag(slot, EMPTY);
    }

    public BlockId getBlockId() {
        return blockId;
    }

    /**
     * Give default values to ALL record slots in the page.
     */
    public void format() {
        Schema schema = layout.getSchema();
        int curSlot = 0;
        while (isValidSlot(curSlot)) {
            tx.setInt(blockId, bytesOffsetOfSlot(curSlot), EMPTY, false);

            for (String fieldName: schema.fields()) {
                int fieldPos = bytesOffsetOfSlot(curSlot) + layout.offsetOfField(fieldName);
                if (schema.type(fieldName) == Types.INTEGER)
                    tx.setInt(blockId, fieldPos, 0, false);
                else
                    tx.setString(blockId, fieldPos, "", false);
            }
            curSlot++;
        }
    }

    /**
     * Search for the first USED slot that follows the specified slot.
     * @param slot slot to search after
     * @return -1 if all remaining slots are empty, slot number otherwise
     */
    public int nextAfter(int slot) {
        return searchAfter(slot, USED);
    }

    /**
     * Looks for the first EMPTY slot that follows the specified slot.
     * If found, set its flag to USED and return the slot number.
     * @param slot slot to search after
     * @return -1 if all remaining slots are used, slot number otherwise
     */
    public int insertAfter(int slot) {
        int newSlot = searchAfter(slot, EMPTY);
        if (newSlot >= 0)
            setFlag(newSlot, USED);

        return newSlot;
    }

    // ===== PRIVATE HELPER METHODS ===
    private int bytesOffsetOfSlot(int slot) {
        return slot * layout.getSlotSize();
    }

    private boolean isValidSlot(int slot) {
        return bytesOffsetOfSlot(slot+1) <= tx.blockSize();
    }

    private void setFlag(int slot, int flag) {
        tx.setInt(blockId, bytesOffsetOfSlot(slot), flag, true);
    }

    private int searchAfter(int slot, int flag) {
        slot++;
        while (isValidSlot(slot)) {
            if (tx.getInt(blockId, bytesOffsetOfSlot(slot)) == flag)
                return slot;

            slot++;
        }
        return -1;
    }
}
