// ORM class for table 'ecom_large'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sat Apr 20 20:35:22 MSK 2024
// For connector: org.apache.sqoop.manager.PostgresqlManager
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import org.apache.sqoop.lib.JdbcWritableBridge;
import org.apache.sqoop.lib.DelimiterSet;
import org.apache.sqoop.lib.FieldFormatter;
import org.apache.sqoop.lib.RecordParser;
import org.apache.sqoop.lib.BooleanParser;
import org.apache.sqoop.lib.BlobRef;
import org.apache.sqoop.lib.ClobRef;
import org.apache.sqoop.lib.LargeObjectLoader;
import org.apache.sqoop.lib.SqoopRecord;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class ecom_large extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("event_id", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        ecom_large.this.event_id = (Integer)value;
      }
    });
    setters.put("event_time", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        ecom_large.this.event_time = (java.sql.Timestamp)value;
      }
    });
    setters.put("event_type", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        ecom_large.this.event_type = (String)value;
      }
    });
    setters.put("product_id", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        ecom_large.this.product_id = (Integer)value;
      }
    });
    setters.put("category_id", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        ecom_large.this.category_id = (Long)value;
      }
    });
    setters.put("category_code", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        ecom_large.this.category_code = (String)value;
      }
    });
    setters.put("brand", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        ecom_large.this.brand = (String)value;
      }
    });
    setters.put("price", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        ecom_large.this.price = (Double)value;
      }
    });
    setters.put("user_id", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        ecom_large.this.user_id = (Integer)value;
      }
    });
    setters.put("user_session", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        ecom_large.this.user_session = (String)value;
      }
    });
  }
  public ecom_large() {
    init0();
  }
  private Integer event_id;
  public Integer get_event_id() {
    return event_id;
  }
  public void set_event_id(Integer event_id) {
    this.event_id = event_id;
  }
  public ecom_large with_event_id(Integer event_id) {
    this.event_id = event_id;
    return this;
  }
  private java.sql.Timestamp event_time;
  public java.sql.Timestamp get_event_time() {
    return event_time;
  }
  public void set_event_time(java.sql.Timestamp event_time) {
    this.event_time = event_time;
  }
  public ecom_large with_event_time(java.sql.Timestamp event_time) {
    this.event_time = event_time;
    return this;
  }
  private String event_type;
  public String get_event_type() {
    return event_type;
  }
  public void set_event_type(String event_type) {
    this.event_type = event_type;
  }
  public ecom_large with_event_type(String event_type) {
    this.event_type = event_type;
    return this;
  }
  private Integer product_id;
  public Integer get_product_id() {
    return product_id;
  }
  public void set_product_id(Integer product_id) {
    this.product_id = product_id;
  }
  public ecom_large with_product_id(Integer product_id) {
    this.product_id = product_id;
    return this;
  }
  private Long category_id;
  public Long get_category_id() {
    return category_id;
  }
  public void set_category_id(Long category_id) {
    this.category_id = category_id;
  }
  public ecom_large with_category_id(Long category_id) {
    this.category_id = category_id;
    return this;
  }
  private String category_code;
  public String get_category_code() {
    return category_code;
  }
  public void set_category_code(String category_code) {
    this.category_code = category_code;
  }
  public ecom_large with_category_code(String category_code) {
    this.category_code = category_code;
    return this;
  }
  private String brand;
  public String get_brand() {
    return brand;
  }
  public void set_brand(String brand) {
    this.brand = brand;
  }
  public ecom_large with_brand(String brand) {
    this.brand = brand;
    return this;
  }
  private Double price;
  public Double get_price() {
    return price;
  }
  public void set_price(Double price) {
    this.price = price;
  }
  public ecom_large with_price(Double price) {
    this.price = price;
    return this;
  }
  private Integer user_id;
  public Integer get_user_id() {
    return user_id;
  }
  public void set_user_id(Integer user_id) {
    this.user_id = user_id;
  }
  public ecom_large with_user_id(Integer user_id) {
    this.user_id = user_id;
    return this;
  }
  private String user_session;
  public String get_user_session() {
    return user_session;
  }
  public void set_user_session(String user_session) {
    this.user_session = user_session;
  }
  public ecom_large with_user_session(String user_session) {
    this.user_session = user_session;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ecom_large)) {
      return false;
    }
    ecom_large that = (ecom_large) o;
    boolean equal = true;
    equal = equal && (this.event_id == null ? that.event_id == null : this.event_id.equals(that.event_id));
    equal = equal && (this.event_time == null ? that.event_time == null : this.event_time.equals(that.event_time));
    equal = equal && (this.event_type == null ? that.event_type == null : this.event_type.equals(that.event_type));
    equal = equal && (this.product_id == null ? that.product_id == null : this.product_id.equals(that.product_id));
    equal = equal && (this.category_id == null ? that.category_id == null : this.category_id.equals(that.category_id));
    equal = equal && (this.category_code == null ? that.category_code == null : this.category_code.equals(that.category_code));
    equal = equal && (this.brand == null ? that.brand == null : this.brand.equals(that.brand));
    equal = equal && (this.price == null ? that.price == null : this.price.equals(that.price));
    equal = equal && (this.user_id == null ? that.user_id == null : this.user_id.equals(that.user_id));
    equal = equal && (this.user_session == null ? that.user_session == null : this.user_session.equals(that.user_session));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ecom_large)) {
      return false;
    }
    ecom_large that = (ecom_large) o;
    boolean equal = true;
    equal = equal && (this.event_id == null ? that.event_id == null : this.event_id.equals(that.event_id));
    equal = equal && (this.event_time == null ? that.event_time == null : this.event_time.equals(that.event_time));
    equal = equal && (this.event_type == null ? that.event_type == null : this.event_type.equals(that.event_type));
    equal = equal && (this.product_id == null ? that.product_id == null : this.product_id.equals(that.product_id));
    equal = equal && (this.category_id == null ? that.category_id == null : this.category_id.equals(that.category_id));
    equal = equal && (this.category_code == null ? that.category_code == null : this.category_code.equals(that.category_code));
    equal = equal && (this.brand == null ? that.brand == null : this.brand.equals(that.brand));
    equal = equal && (this.price == null ? that.price == null : this.price.equals(that.price));
    equal = equal && (this.user_id == null ? that.user_id == null : this.user_id.equals(that.user_id));
    equal = equal && (this.user_session == null ? that.user_session == null : this.user_session.equals(that.user_session));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.event_id = JdbcWritableBridge.readInteger(1, __dbResults);
    this.event_time = JdbcWritableBridge.readTimestamp(2, __dbResults);
    this.event_type = JdbcWritableBridge.readString(3, __dbResults);
    this.product_id = JdbcWritableBridge.readInteger(4, __dbResults);
    this.category_id = JdbcWritableBridge.readLong(5, __dbResults);
    this.category_code = JdbcWritableBridge.readString(6, __dbResults);
    this.brand = JdbcWritableBridge.readString(7, __dbResults);
    this.price = JdbcWritableBridge.readDouble(8, __dbResults);
    this.user_id = JdbcWritableBridge.readInteger(9, __dbResults);
    this.user_session = JdbcWritableBridge.readString(10, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.event_id = JdbcWritableBridge.readInteger(1, __dbResults);
    this.event_time = JdbcWritableBridge.readTimestamp(2, __dbResults);
    this.event_type = JdbcWritableBridge.readString(3, __dbResults);
    this.product_id = JdbcWritableBridge.readInteger(4, __dbResults);
    this.category_id = JdbcWritableBridge.readLong(5, __dbResults);
    this.category_code = JdbcWritableBridge.readString(6, __dbResults);
    this.brand = JdbcWritableBridge.readString(7, __dbResults);
    this.price = JdbcWritableBridge.readDouble(8, __dbResults);
    this.user_id = JdbcWritableBridge.readInteger(9, __dbResults);
    this.user_session = JdbcWritableBridge.readString(10, __dbResults);
  }
  public void loadLargeObjects(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void loadLargeObjects0(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void write(PreparedStatement __dbStmt) throws SQLException {
    write(__dbStmt, 0);
  }

  public int write(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeInteger(event_id, 1 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeTimestamp(event_time, 2 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(event_type, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeInteger(product_id, 4 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeLong(category_id, 5 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeString(category_code, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(brand, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeDouble(price, 8 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeInteger(user_id, 9 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeString(user_session, 10 + __off, 12, __dbStmt);
    return 10;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeInteger(event_id, 1 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeTimestamp(event_time, 2 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(event_type, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeInteger(product_id, 4 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeLong(category_id, 5 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeString(category_code, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(brand, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeDouble(price, 8 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeInteger(user_id, 9 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeString(user_session, 10 + __off, 12, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.event_id = null;
    } else {
    this.event_id = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.event_time = null;
    } else {
    this.event_time = new Timestamp(__dataIn.readLong());
    this.event_time.setNanos(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.event_type = null;
    } else {
    this.event_type = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.product_id = null;
    } else {
    this.product_id = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.category_id = null;
    } else {
    this.category_id = Long.valueOf(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.category_code = null;
    } else {
    this.category_code = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.brand = null;
    } else {
    this.brand = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.price = null;
    } else {
    this.price = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.user_id = null;
    } else {
    this.user_id = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.user_session = null;
    } else {
    this.user_session = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.event_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.event_id);
    }
    if (null == this.event_time) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.event_time.getTime());
    __dataOut.writeInt(this.event_time.getNanos());
    }
    if (null == this.event_type) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, event_type);
    }
    if (null == this.product_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.product_id);
    }
    if (null == this.category_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.category_id);
    }
    if (null == this.category_code) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, category_code);
    }
    if (null == this.brand) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, brand);
    }
    if (null == this.price) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.price);
    }
    if (null == this.user_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.user_id);
    }
    if (null == this.user_session) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, user_session);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.event_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.event_id);
    }
    if (null == this.event_time) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.event_time.getTime());
    __dataOut.writeInt(this.event_time.getNanos());
    }
    if (null == this.event_type) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, event_type);
    }
    if (null == this.product_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.product_id);
    }
    if (null == this.category_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.category_id);
    }
    if (null == this.category_code) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, category_code);
    }
    if (null == this.brand) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, brand);
    }
    if (null == this.price) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.price);
    }
    if (null == this.user_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.user_id);
    }
    if (null == this.user_session) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, user_session);
    }
  }
  private static final DelimiterSet __outputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
  public String toString() {
    return toString(__outputDelimiters, true);
  }
  public String toString(DelimiterSet delimiters) {
    return toString(delimiters, true);
  }
  public String toString(boolean useRecordDelim) {
    return toString(__outputDelimiters, useRecordDelim);
  }
  public String toString(DelimiterSet delimiters, boolean useRecordDelim) {
    StringBuilder __sb = new StringBuilder();
    char fieldDelim = delimiters.getFieldsTerminatedBy();
    __sb.append(FieldFormatter.escapeAndEnclose(event_id==null?"null":"" + event_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(event_time==null?"null":"" + event_time, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(event_type==null?"null":event_type, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(product_id==null?"null":"" + product_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(category_id==null?"null":"" + category_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(category_code==null?"null":category_code, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(brand==null?"null":brand, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(price==null?"null":"" + price, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(user_id==null?"null":"" + user_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(user_session==null?"null":user_session, delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(event_id==null?"null":"" + event_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(event_time==null?"null":"" + event_time, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(event_type==null?"null":event_type, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(product_id==null?"null":"" + product_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(category_id==null?"null":"" + category_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(category_code==null?"null":category_code, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(brand==null?"null":brand, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(price==null?"null":"" + price, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(user_id==null?"null":"" + user_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(user_session==null?"null":user_session, delimiters));
  }
  private static final DelimiterSet __inputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
  private RecordParser __parser;
  public void parse(Text __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharSequence __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(byte [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(char [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(ByteBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  private void __loadFromFields(List<String> fields) {
    Iterator<String> __it = fields.listIterator();
    String __cur_str = null;
    try {
    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.event_id = null; } else {
      this.event_id = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.event_time = null; } else {
      this.event_time = java.sql.Timestamp.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.event_type = null; } else {
      this.event_type = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.product_id = null; } else {
      this.product_id = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.category_id = null; } else {
      this.category_id = Long.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.category_code = null; } else {
      this.category_code = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.brand = null; } else {
      this.brand = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.price = null; } else {
      this.price = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.user_id = null; } else {
      this.user_id = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.user_session = null; } else {
      this.user_session = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.event_id = null; } else {
      this.event_id = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.event_time = null; } else {
      this.event_time = java.sql.Timestamp.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.event_type = null; } else {
      this.event_type = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.product_id = null; } else {
      this.product_id = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.category_id = null; } else {
      this.category_id = Long.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.category_code = null; } else {
      this.category_code = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.brand = null; } else {
      this.brand = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.price = null; } else {
      this.price = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.user_id = null; } else {
      this.user_id = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.user_session = null; } else {
      this.user_session = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    ecom_large o = (ecom_large) super.clone();
    o.event_time = (o.event_time != null) ? (java.sql.Timestamp) o.event_time.clone() : null;
    return o;
  }

  public void clone0(ecom_large o) throws CloneNotSupportedException {
    o.event_time = (o.event_time != null) ? (java.sql.Timestamp) o.event_time.clone() : null;
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("event_id", this.event_id);
    __sqoop$field_map.put("event_time", this.event_time);
    __sqoop$field_map.put("event_type", this.event_type);
    __sqoop$field_map.put("product_id", this.product_id);
    __sqoop$field_map.put("category_id", this.category_id);
    __sqoop$field_map.put("category_code", this.category_code);
    __sqoop$field_map.put("brand", this.brand);
    __sqoop$field_map.put("price", this.price);
    __sqoop$field_map.put("user_id", this.user_id);
    __sqoop$field_map.put("user_session", this.user_session);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("event_id", this.event_id);
    __sqoop$field_map.put("event_time", this.event_time);
    __sqoop$field_map.put("event_type", this.event_type);
    __sqoop$field_map.put("product_id", this.product_id);
    __sqoop$field_map.put("category_id", this.category_id);
    __sqoop$field_map.put("category_code", this.category_code);
    __sqoop$field_map.put("brand", this.brand);
    __sqoop$field_map.put("price", this.price);
    __sqoop$field_map.put("user_id", this.user_id);
    __sqoop$field_map.put("user_session", this.user_session);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
