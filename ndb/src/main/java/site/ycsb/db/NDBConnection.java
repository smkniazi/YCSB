package site.ycsb.db;

import com.mysql.clusterj.ClusterJException;
import com.mysql.clusterj.ClusterJHelper;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.SessionFactory;
import site.ycsb.DBException;

import java.util.Properties;

/**
 * NDB Connection object.
 */
public final class NDBConnection {
  private NDBConnection() {
  }

  static NDBConnection connect() throws DBException {
    NDBConnection connection = new NDBConnection();
    connection.setUpDBConnection();
    return connection;

  }

  private SessionFactory sessionFactory;

  public void setUpDBConnection() throws DBException {
    Properties props = new Properties();
    props.setProperty("com.mysql.clusterj.connectstring", "localhost");
    props.setProperty("com.mysql.clusterj.database", "ycsb");
    props.setProperty("com.mysql.clusterj.connect.retries", "4");
    props.setProperty("com.mysql.clusterj.connect.delay", "5");
    props.setProperty("com.mysql.clusterj.connect.verbose", "1");
    props.setProperty("com.mysql.clusterj.connect.timeout.before", "30");
    props.setProperty("com.mysql.clusterj.connect.timeout.after", "20");
    props.setProperty("com.mysql.clusterj.max.transactions", "1024");
    props.setProperty("com.mysql.clusterj.connection.pool.size", "1");
    props.setProperty("com.mysql.clusterj.max.cached.instances", "256");

    try {
      sessionFactory = ClusterJHelper.getSessionFactory(props);
    } catch (ClusterJException ex) {
      throw new DBException(ex);
    }
    System.out.println("Connected to NDB");
  }

  public void closeConnection() {
    sessionFactory.close();
  }

  public Session getSession(){
    return sessionFactory.getSession();
  }

  public void returnSession(Session session) {
    assert !session.isClosed(); // to make sure that we do not close session in NDBClient
    session.close();
  }
}
