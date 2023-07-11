package simpledb.jdbc.embedded;

import simpledb.jdbc.DriverAdapter;
import simpledb.server.SimpleDB;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * The RMI server-side implementation of RemoteDriver.
 *
 * +-------------------+----------------+
 * | JDBC Interface    | SimpleDB Class |
 * +-------------------+----------------+
 * | Driver            | SimpleDB       |
 * +-------------------+----------------+
 */
public class EmbeddedDriver extends DriverAdapter {

    /**
     * Creates a new RemoteConnectionImpl object and
     * returns it.
     * @see simpledb.jdbc.network.RemoteDriver#connect()
     */
    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        String dbName = url.replace("jdbc:simpledb:", "");
        SimpleDB simpleDB = new SimpleDB(dbName);
        return new EmbeddedConnection(simpleDB);
    }
}
