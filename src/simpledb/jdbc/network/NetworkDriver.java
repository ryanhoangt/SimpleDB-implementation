package simpledb.jdbc.network;

import simpledb.jdbc.DriverAdapter;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * The SimpleDB database driver.
 */
public class NetworkDriver extends DriverAdapter {

    /**
     * Connects to the SimpleDB server on the specified host.
     * The method retrieves the RemoteDriver stub from
     * the RMI registry on the specified host.
     * It then calls the connect method on that stub,
     * which in turn creates a new connection and
     * returns the RemoteConnection stub for it.
     * This stub is wrapped in a SimpleConnection object
     * and is returned.
     * <P>
     * The current implementation of this method ignores the
     * properties argument.
     * @see java.sql.Driver#connect(java.lang.String, Properties)
     */
    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        try {
            String host = url.replace("jdbc:simpledb://", "");
            Registry reg = LocateRegistry.getRegistry(host, 1099);
            RemoteDriver rdrv = (RemoteDriver) reg.lookup("simpledb");
            RemoteConnection rconn = rdrv.connect();
            return new NetworkConnection(rconn);

        } catch (Exception ex) {
            throw new SQLException(ex);
        }
    }
}
