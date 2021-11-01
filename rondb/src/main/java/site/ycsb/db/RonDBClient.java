/**
 * Copyright (c) 2012 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 * <p>
 * RonDB client binding for YCSB.
 */

/**
 * RonDB client binding for YCSB.
 */

package site.ycsb.db;

import com.mysql.clusterj.ClusterJException;
import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

/**
 * YCSB binding for <a href="https://rondb.com/">RonDB</a>.
 */
public class RonDBClient extends DB {
  private static Logger logger = LoggerFactory.getLogger(RonDBClient.class);
  private static RonDBConnection connection;
  private static Object lock = new Object();

  /**
   * Initialize any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  public void init() throws DBException {
    synchronized (lock) {
      if (connection == null) {
        connection = RonDBConnection.connect(getProperties());
      }
      Session session = connection.getSession(); //initialize session for this thread
      connection.returnSession(session);
      System.out.println("Created a session for the thread");

    }
  }

  /**
   * Cleanup any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  public void cleanup() throws DBException {
    synchronized (lock) {
      if (connection != null) {
        RonDBConnection.closeSession(connection);
      }
    }
  }

  /**
   * Read a record from the database. Each field/value pair from the result will
   * be stored in a HashMap.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return The result of the operation.
   */
  @Override
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {
    Session session = connection.getSession();
    try {
      UserTable.UserTableDTO row = session.find(UserTable.UserTableDTO.class, key);
      if (row == null) {
        logger.info("Read. Key: " + key + " Not Found.");
        return Status.NOT_FOUND;
      }
      Set<String> toRead = fields != null ? fields : UserTable.ALL_FIELDS;
      for (String field : toRead) {
        result.put(field, UserTable.readFieldFromDTO(field, row));
      }
      releaseDTO(session, row);
      if (logger.isDebugEnabled()) {
        logger.debug("Read Key " + key);
      }
      return Status.OK;
    } catch (Exception e) {
      if (!isSessionClosing(e)) {
        logger.warn("Read Error: " + e);
        return Status.ERROR;
      }
      return Status.OK; // session is closing
    } finally {
      connection.returnSession(session);
    }
  }

  /**
   * Perform a range scan for a set of records in the database.
   * Each field/value pair from the result will be stored in a HashMap.
   *
   * @param table       The name of the table
   * @param startkey    The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields      The list of fields to read, or null for all of them
   * @param result      A Vector of HashMaps, where each HashMap is a set
   *                    field/value pairs for one record
   * @return The result of the operation.
   */
  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    Session session = connection.getSession();
    try {
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<UserTable.UserTableDTO> dobj =
          qb.createQueryDefinition(UserTable.UserTableDTO.class);
      Predicate pred1 = dobj.get(UserTable.KEY).greaterEqual(dobj.param(UserTable.KEY + "Param"));
      dobj.where(pred1);
      Query<UserTable.UserTableDTO> query = session.createQuery(dobj);
      query.setParameter(UserTable.KEY + "Param", startkey);
      query.setLimits(0, recordcount);
      List<UserTable.UserTableDTO> scanResults = query.getResultList();
      for (UserTable.UserTableDTO dto : scanResults) {
        result.add(UserTable.convertDTO(dto, fields != null ? fields : UserTable.ALL_FIELDS));
        releaseDTO(session, dto);
      }

      if (logger.isDebugEnabled()) {
        logger.debug("Scan. Rows returned: " + result.size());
      }
      return Status.OK;
    } catch (Exception e) {
      if (!isSessionClosing(e)) {
        logger.warn("Scan Error: " + e);
        return Status.ERROR;
      }
      return Status.OK;
    } finally {
      connection.returnSession(session);
    }
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified values
   * HashMap will be written into the record with the specified record key,
   * overwriting any existing values with the same field name.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record
   * @return The result of the operation.
   */
  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    Session session = connection.getSession();
    try {
//      UserTable.UserTableDTO row = session.find(UserTable.UserTableDTO.class, key);
      UserTable.UserTableDTO row = session.newInstance(UserTable.UserTableDTO.class, key);

      //update row
      for (String field : values.keySet()) {
        ByteIterator itr = values.get(field);
        UserTable.setFieldInDto(row, field, itr);
      }
      session.savePersistent(row);
      releaseDTO(session, row);
      if (logger.isDebugEnabled()) {
        logger.debug("Updated Key " + key);
      }
      return Status.OK;
    } catch (Exception e) {
      if (!isSessionClosing(e)) {
        logger.warn("Update Error: " + e);
        return Status.ERROR;
      }
      return Status.OK;
    } finally {
      connection.returnSession(session);
    }
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified values
   * HashMap will be written into the record with the specified record key.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   * @return The result of the operation.
   */
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    Session session = connection.getSession();
    try {
      UserTable.UserTableDTO row = UserTable.createDTO(session, values);
      row.setKey(key);
      session.savePersistent(row);
      releaseDTO(session, row);
      if (logger.isDebugEnabled()) {
        logger.debug("Inserted Key " + key);
      }
      return Status.OK;
    } catch (Exception e) {
      if (!isSessionClosing(e)) {
        logger.warn("Insert Error: " + e);
        return Status.ERROR;
      }
      return Status.OK;
    } finally {
      connection.returnSession(session);
    }
  }


  /**
   * Delete a record from the database.
   *
   * @param table The name of the table
   * @param key   The record key of the record to delete.
   * @return The result of the operation.
   */
  @Override
  public Status delete(String table, String key) {
    Session session = connection.getSession();
    try {
      UserTable.UserTableDTO row = session.newInstance(UserTable.UserTableDTO.class, key);
      session.deletePersistent(row);
      releaseDTO(session, row);
      return Status.OK;
    } catch (Exception e) {
      if (!isSessionClosing(e)) {
        logger.warn("Delete Error: " + e);
        return Status.ERROR;
      }
      return Status.OK;
    } finally {
      connection.returnSession(session);
    }
  }

  private void releaseDTO(Session session, UserTable.UserTableDTO dto) {
    session.releaseCache(dto, dto.getClass());
  }

  private boolean isSessionClosing(Exception e) {
    if (e instanceof ClusterJException && e.getMessage().contains("Db is closing")) {
      return true;
    }
    return false;
  }
}
