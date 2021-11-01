package site.ycsb.db;


import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * clusterj table definition for `usertable`.
 */
public final class UserTable {

  public static final String TABLE_NAME = "usertable";
  public static final String KEY = "key";
  public static final Integer MAX_FIELDS = 1;
  public static final String FIELD0 = "field0";

  public static final Set<String> ALL_FIELDS = new HashSet<>();

  static {
    for (int i = 0; i < MAX_FIELDS; i++) {
      ALL_FIELDS.add("field" + i);
    }
  }

  private UserTable() {

  }

  /**
   * clusterj table definition for usertable.
   */
  @PersistenceCapable(table = TABLE_NAME)
  public interface UserTableDTO {

    @PrimaryKey
    @Column(name = KEY)
    String getKey();

    void setKey(String key);

    @Column(name = FIELD0)
    byte[] getField0();

    void setField0(byte[] val);
  }

  static UserTable.UserTableDTO createDTO(Session session, Map<String, ByteIterator> values) {
    UserTable.UserTableDTO persistable = session.newInstance(UserTable.UserTableDTO.class);
    for (String field : values.keySet()) {
      ByteIterator itr = values.get(field);
      setFieldInDto(persistable, field, itr);
    }
    return persistable;
  }

  static void setFieldInDto(UserTableDTO dto, String field, ByteIterator bItr) {
    //TODO use reflections
    byte[] value = bItr.toArray();
    switch (field) {
    case UserTable.FIELD0:
      dto.setField0(value);
      break;
    default:
      throw new IllegalArgumentException("Data field not recognized. Field: " + field);
    }
  }

  static HashMap<String, ByteIterator> convertDTO(UserTable.UserTableDTO dto, Set<String> fields) {
    HashMap<String, ByteIterator> values = new HashMap<>();
    for(String field: fields){
      values.put(field, readFieldFromDTO(field, dto));
    }
    return values;
  }

  static ByteIterator readFieldFromDTO(String field, UserTable.UserTableDTO dto) {
    //TODO use reflections
    byte[] value = null;

    switch (field) {
    case UserTable.FIELD0:
      value = dto.getField0();
      break;
    default:
      throw new IllegalArgumentException("Data field not recognized. Field: " + field);
    }

    return new ByteArrayByteIterator(value, 0, value.length);
  }
}
