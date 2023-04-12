package simpledb.metadata;

import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

import java.sql.Types;

public class TableMetaMgrTest {

    public static void main(String[] args) {
        SimpleDB db = new SimpleDB("tblmetamgrtestdir", 400, 8);
        Transaction tx = db.newTx();
        TableMetaMgr tmm = new TableMetaMgr(true, tx);

        Schema schema = new Schema();
        schema.addIntField("A");
        schema.addStringField("B", 10);
        tmm.createTable("MyTable", schema, tx);

        Layout layout = tmm.getLayout("MyTable", tx);
        int size = layout.getSlotSize();
        Schema schema2 = layout.getSchema();
        System.out.println("MyTable has slot size " + size);
        System.out.println("Its fields are: ");
        for (String fieldName: schema2.fields()) {
            String type;
            if (schema2.type(fieldName) == Types.INTEGER)
                type = "INT";
            else {
                int strLen = schema.length(fieldName);
                type = "VARCHAR(" + strLen + ")";
            }
            System.out.println(fieldName + ": " + type);
        }
        tx.commit();
    }
}
