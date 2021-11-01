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
 * RonDB Connection object.
 */
public final class RonDBConnection {

  private static Logger logger = LoggerFactory.getLogger(RonDBConnection.class);

  public static final String HOST_PROPERTY = "rondb.host";
  public static final String PORT_PROPERTY = "rondb.port";
  public static final String SCHEMA_PROPERTY = "rondb.schema";

  private SessionFactory sessionFactory;
  private static ThreadLocal<Session> sessions = new ThreadLocal<>();


  private RonDBConnection() {
  }

  static synchronized RonDBConnection connect(Properties props) throws DBException {
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

    RonDBConnection connection = new RonDBConnection();
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
    System.out.println("Connected to RonDB");
  }

  public static synchronized  void closeConnection(RonDBConnection connection) {
//    connection.sessionFactory.close();
  }

  public Session getSession() {
    Session session = sessions.get();
    if (session == null) {
      session = sessionFactory.getSession();
      sessions.set(session);
    }

    return session;
  }

  public void returnSession(Session session) {
    // do not close the session. the same session will be
    // returned if needed again.
  }
}
