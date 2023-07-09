package simpledb.plan;

import simpledb.query.Scan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class PlannerStudentTest {

    public static void main(String[] args) {
        SimpleDB db = new SimpleDB("plannerstudenttestdir");
        Planner planner = db.planner();
        Transaction tx  = db.newTx();

        // part 0: Create table
        String cmd = "create table STUDENT(SId int, SName varchar(10), MajorId int, GradYear int)";
        int res = planner.executeUpdate(cmd, tx);
        System.out.println(res + " row(s) affected.");

        // part 1: Process a query
        String qry = "select sname, gradyear from student";
        Plan p = planner.createQueryPlan(qry, tx);
        Scan s = p.open();
        while (s.next())
            System.out.println(s.getString("sname") + " " +
                    s.getInt("gradyear"));
        s.close();

        // part 2: Process an update command
        cmd = "delete from STUDENT where MajorId = 30";
        int num = planner.executeUpdate(cmd, tx);
        System.out.println(num + " students were deleted");

        tx.commit();
    }

}
