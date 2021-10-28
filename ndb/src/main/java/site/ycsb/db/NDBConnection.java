package site.ycsb.db;

import com.mysql.clusterj.ClusterJException;
import com.mysql.clusterj.ClusterJHelper;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.SessionFactory;
import site.ycsb.DBException;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * NDB Connection object.
 */
public final class NDBConnection {

  private static Logger logger = LoggerFactory.getLogger(NDBConnection.class);

  public static final String HOST_PROPERTY = "ndb.host";
  public static final String PORT_PROPERTY = "ndb.port";
  public static final String SCHEMA_PROPERTY = "ndb.schema";

  private SessionFactory sessionFactory;

  private NDBConnection() {
  }

  static NDBConnection connect(Properties props) throws DBException {
    String port = props.getProperty(PORT_PROPERTY);
    if (port == null) {
      port = "1186";
    }
    String host = props.getProperty(HOST_PROPERTY);
    if (host == null) {
      host = "127.0.0.1";
    }
    String schema = props.getProperty(SCHEMA_PROPERTY);
    if (schema == null) {
      schema = "ycsb";
    }

    NDBConnection connection = new NDBConnection();
    connection.setUpDBConnection(host, port, schema);
    return connection;
  }

  public void setUpDBConnection(String host, String port, String schema) throws DBException {
    logger.info("Connecting to  schema: " + schema + " on " + host + ":" + port + ".");

    Properties props = new Properties();
    props.setProperty("com.mysql.clusterj.connectstring", host + ":" + port);
    props.setProperty("com.mysql.clusterj.database", schema);
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

  public Session getSession() {
    return sessionFactory.getSession();
  }

  public void returnSession(Session session) {
    assert !session.isClosed(); // to make sure that we do not close session in NDBClient
    session.close();
  }
}
