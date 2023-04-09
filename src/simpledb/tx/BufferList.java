package simpledb.tx;

import simpledb.buffer.Buffer;
import simpledb.buffer.BufferMgr;
import simpledb.file.BlockId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class BufferList {

    private Map<BlockId, Buffer> blockIdToBuffer = new HashMap<>();
    private List<BlockId> blocksPinned = new ArrayList<>();
    private BufferMgr bm;

    BufferList(BufferMgr bm) {
        this.bm = bm;
    }

    Buffer getBuffer(BlockId blockId) {
        return blockIdToBuffer.get(blockId);
    }

    void pin(BlockId blockId) {
        Buffer buf = bm.pin(blockId);
        blockIdToBuffer.put(blockId, buf);
        blocksPinned.add(blockId);
    }

    void unpin(BlockId blockId) {
        Buffer buf = blockIdToBuffer.get(blockId);
        bm.unpin(buf);
        blocksPinned.remove(blockId);
        if (!blocksPinned.contains(blockId))
            blockIdToBuffer.remove(blockId);
    }

    void unpinAll() {
        for (BlockId blockId: blocksPinned) {
            Buffer buf = blockIdToBuffer.get(blockId);
            bm.unpin(buf);
        }
        blockIdToBuffer.clear();
        blocksPinned.clear();
    }

}
