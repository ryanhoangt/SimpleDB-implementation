package simpledb.server;

import simpledb.jdbc.network.RemoteDriver;
import simpledb.jdbc.network.RemoteDriverImpl;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class StartServer {

    public static void main(String[] args) throws RemoteException {
        // Configure and initialize the database
        String dirName = (args.length == 0) ? "studentdb" : args[0];
        SimpleDB db = new SimpleDB(dirName);

        // Create a registry specific for the server on the default port
        Registry registry = LocateRegistry.createRegistry(1099);

        // And post the server entry in it
        RemoteDriver d = new RemoteDriverImpl(db);
        registry.rebind("simpledb", d);

        System.out.println("Database server ready!");
    }

}
