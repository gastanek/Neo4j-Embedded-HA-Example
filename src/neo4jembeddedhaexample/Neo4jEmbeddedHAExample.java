/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package neo4jembeddedhaexample;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.tooling.GlobalGraphOperations;

public class Neo4jEmbeddedHAExample {

    private static HighlyAvailableGraphDatabase master;
    private static HighlyAvailableGraphDatabase slave;

    public static void main(String[] args) {
        System.out.println("Starting Master");
        createMaster();
        System.out.println("Starting Slave");
        createSlave();
        System.out.println("Writing to the master");
        masterWrite();
        System.out.println("Reading nodes from the slave");
        slaveRead();
        System.out.println("Shutting down");
        slave.shutdown();
        master.shutdown();
    }

    private static void createMaster() {
        
        master = (HighlyAvailableGraphDatabase) new HighlyAvailableGraphDatabaseFactory()
                .newHighlyAvailableDatabaseBuilder( "test/ha/master" )
                .setConfig( ClusterSettings.server_id, "1" )
                .setConfig( ClusterSettings.initial_hosts, "")
                .newGraphDatabase();
        while(!master.isAvailable(1000));
        System.out.println("The master server is up.");
    }
    
    private static void createSlave() {
        slave = (HighlyAvailableGraphDatabase) new HighlyAvailableGraphDatabaseFactory()
                .newHighlyAvailableDatabaseBuilder( "test/ha/slave" )
                .setConfig( ClusterSettings.server_id, "2" )
                .setConfig( ClusterSettings.initial_hosts, "localhost:5001")
                .newGraphDatabase();
       while(!slave.isAvailable(1000));
       System.out.println("The slave server is up.");
    }
    
    private static void masterWrite() {
        int x=0;
        Node newNode;
        
        while (x<100) {
         
            try ( Transaction masterTx = master.beginTx() ) {
                newNode = master.createNode();
                newNode.setProperty( "id", x );
                masterTx.success();
            }
            x++;
        }
        System.out.println("nodes created on master");
    }

    private static void slaveRead() {
        
        try ( Transaction tx = slave.beginTx() )
        {
            int count;
            count = IteratorUtil.count(GlobalGraphOperations.at(slave).getAllNodes());
            tx.success();
            System.out.println("Number of nodes in the slave are: " + count);
        }
    }
    
 
}
