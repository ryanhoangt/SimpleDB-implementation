package simpledb.record;

public class LayoutTest {

    public static void main(String[] args) {
        Schema schema = new Schema();
        schema.addStringField("B", 10);
        schema.addIntField("A");
        Layout layout = new Layout(schema);
        for (String fieldName: layout.getSchema().fields()) {
            int offset = layout.offsetOfField(fieldName);
            System.out.println(fieldName + " has offset " + offset);
        }
    }
}
