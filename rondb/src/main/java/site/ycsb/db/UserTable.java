package site.ycsb.db;


import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import site.ycsb.ByteIterator;
import site.ycsb.StringByteIterator;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * clusterj table definition for usertable.
 */
public final class UserTable {

  public static final String TABLE_NAME = "usertable";
  public static final String KEY = "key";
  public static final Integer MAX_FIELDS = 10;
  public static final String FIELD0 = "field0";
  public static final String FIELD1 = "field1";
  public static final String FIELD2 = "field2";
  public static final String FIELD3 = "field3";
  public static final String FIELD4 = "field4";
  public static final String FIELD5 = "field5";
  public static final String FIELD6 = "field6";
  public static final String FIELD7 = "field7";
  public static final String FIELD8 = "field8";
  public static final String FIELD9 = "field9";

  public static final Set<String> ALL_FIELDS = new HashSet<>();
  static  {
    for(int i = 0; i < MAX_FIELDS; i++){
      ALL_FIELDS.add("field"+i);
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
    String getField0();

    void setField0(String val);

    @Column(name = FIELD1)
    String getField1();

    void setField1(String val);

    @Column(name = FIELD2)
    String getField2();

    void setField2(String val);

    @Column(name = FIELD3)
    String getField3();

    void setField3(String val);

    @Column(name = FIELD4)
    String getField4();

    void setField4(String val);

    @Column(name = FIELD5)
    String getField5();

    void setField5(String val);

    @Column(name = FIELD6)
    String getField6();

    void setField6(String val);

    @Column(name = FIELD7)
    String getField7();

    void setField7(String val);

    @Column(name = FIELD8)
    String getField8();

    void setField8(String val);

    @Column(name = FIELD9)
    String getField9();

    void setField9(String val);
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
    String value = bItr.toString();
    switch (field) {
    case UserTable.FIELD0:
      dto.setField0(value);
      break;
    case UserTable.FIELD1:
      dto.setField1(value);
      break;
    case UserTable.FIELD2:
      dto.setField2(value);
      break;
    case UserTable.FIELD3:
      dto.setField3(value);
      break;
    case UserTable.FIELD4:
      dto.setField4(value);
      break;
    case UserTable.FIELD5:
      dto.setField5(value);
      break;
    case UserTable.FIELD6:
      dto.setField6(value);
      break;
    case UserTable.FIELD7:
      dto.setField7(value);
      break;
    case UserTable.FIELD8:
      dto.setField8(value);
      break;
    case UserTable.FIELD9:
      dto.setField9(value);
      break;
    default:
      throw new IllegalArgumentException("Data field not recognized. Field: " + field);
    }
  }

  static ByteIterator readFieldFromDTO(String field, UserTable.UserTableDTO dto) {
    //TODO use reflections
    String value = null;

    switch (field) {
    case UserTable.KEY:
      value = dto.getKey();
      break;
    case UserTable.FIELD0:
      value = dto.getField0();
      break;
    case UserTable.FIELD1:
      value = dto.getField1();
      break;
    case UserTable.FIELD2:
      value = dto.getField2();
      break;
    case UserTable.FIELD3:
      value = dto.getField3();
      break;
    case UserTable.FIELD4:
      value = dto.getField4();
      break;
    case UserTable.FIELD5:
      value = dto.getField5();
      break;
    case UserTable.FIELD6:
      value = dto.getField6();
      break;
    case UserTable.FIELD7:
      value = dto.getField7();
      break;
    case UserTable.FIELD8:
      value = dto.getField8();
      break;
    case UserTable.FIELD9:
      value = dto.getField9();
      break;
    default:
      throw new IllegalArgumentException("Data field not recognized. Field: " + field);
    }

    return new StringByteIterator(value);
  }
}
