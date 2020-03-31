/**
 * Autogenerated by Thrift Compiler (0.9.3)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.spark.sql.thriftserver.rpc.thrift;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.server.AbstractNonblockingServer.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import javax.annotation.Generated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked"})
@Generated(value = "Autogenerated by Thrift Compiler (0.9.3)")
public class TProgressUpdateResp implements org.apache.thrift.TBase<TProgressUpdateResp, TProgressUpdateResp._Fields>, java.io.Serializable, Cloneable, Comparable<TProgressUpdateResp> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TProgressUpdateResp");

  private static final org.apache.thrift.protocol.TField HEADER_NAMES_FIELD_DESC = new org.apache.thrift.protocol.TField("headerNames", org.apache.thrift.protocol.TType.LIST, (short)1);
  private static final org.apache.thrift.protocol.TField ROWS_FIELD_DESC = new org.apache.thrift.protocol.TField("rows", org.apache.thrift.protocol.TType.LIST, (short)2);
  private static final org.apache.thrift.protocol.TField PROGRESSED_PERCENTAGE_FIELD_DESC = new org.apache.thrift.protocol.TField("progressedPercentage", org.apache.thrift.protocol.TType.DOUBLE, (short)3);
  private static final org.apache.thrift.protocol.TField STATUS_FIELD_DESC = new org.apache.thrift.protocol.TField("status", org.apache.thrift.protocol.TType.I32, (short)4);
  private static final org.apache.thrift.protocol.TField FOOTER_SUMMARY_FIELD_DESC = new org.apache.thrift.protocol.TField("footerSummary", org.apache.thrift.protocol.TType.STRING, (short)5);
  private static final org.apache.thrift.protocol.TField START_TIME_FIELD_DESC = new org.apache.thrift.protocol.TField("startTime", org.apache.thrift.protocol.TType.I64, (short)6);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new TProgressUpdateRespStandardSchemeFactory());
    schemes.put(TupleScheme.class, new TProgressUpdateRespTupleSchemeFactory());
  }

  private List<String> headerNames; // required
  private List<List<String>> rows; // required
  private double progressedPercentage; // required
  private TJobExecutionStatus status; // required
  private String footerSummary; // required
  private long startTime; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    HEADER_NAMES((short)1, "headerNames"),
    ROWS((short)2, "rows"),
    PROGRESSED_PERCENTAGE((short)3, "progressedPercentage"),
    /**
     * 
     * @see TJobExecutionStatus
     */
    STATUS((short)4, "status"),
    FOOTER_SUMMARY((short)5, "footerSummary"),
    START_TIME((short)6, "startTime");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // HEADER_NAMES
          return HEADER_NAMES;
        case 2: // ROWS
          return ROWS;
        case 3: // PROGRESSED_PERCENTAGE
          return PROGRESSED_PERCENTAGE;
        case 4: // STATUS
          return STATUS;
        case 5: // FOOTER_SUMMARY
          return FOOTER_SUMMARY;
        case 6: // START_TIME
          return START_TIME;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __PROGRESSEDPERCENTAGE_ISSET_ID = 0;
  private static final int __STARTTIME_ISSET_ID = 1;
  private byte __isset_bitfield = 0;
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.HEADER_NAMES, new org.apache.thrift.meta_data.FieldMetaData("headerNames", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING))));
    tmpMap.put(_Fields.ROWS, new org.apache.thrift.meta_data.FieldMetaData("rows", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
                new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)))));
    tmpMap.put(_Fields.PROGRESSED_PERCENTAGE, new org.apache.thrift.meta_data.FieldMetaData("progressedPercentage", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.DOUBLE)));
    tmpMap.put(_Fields.STATUS, new org.apache.thrift.meta_data.FieldMetaData("status", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.EnumMetaData(org.apache.thrift.protocol.TType.ENUM, TJobExecutionStatus.class)));
    tmpMap.put(_Fields.FOOTER_SUMMARY, new org.apache.thrift.meta_data.FieldMetaData("footerSummary", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.START_TIME, new org.apache.thrift.meta_data.FieldMetaData("startTime", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TProgressUpdateResp.class, metaDataMap);
  }

  public TProgressUpdateResp() {
  }

  public TProgressUpdateResp(
    List<String> headerNames,
    List<List<String>> rows,
    double progressedPercentage,
    TJobExecutionStatus status,
    String footerSummary,
    long startTime)
  {
    this();
    this.headerNames = headerNames;
    this.rows = rows;
    this.progressedPercentage = progressedPercentage;
    setProgressedPercentageIsSet(true);
    this.status = status;
    this.footerSummary = footerSummary;
    this.startTime = startTime;
    setStartTimeIsSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TProgressUpdateResp(TProgressUpdateResp other) {
    __isset_bitfield = other.__isset_bitfield;
    if (other.isSetHeaderNames()) {
      List<String> __this__headerNames = new ArrayList<String>(other.headerNames);
      this.headerNames = __this__headerNames;
    }
    if (other.isSetRows()) {
      List<List<String>> __this__rows = new ArrayList<List<String>>(other.rows.size());
      for (List<String> other_element : other.rows) {
        List<String> __this__rows_copy = new ArrayList<String>(other_element);
        __this__rows.add(__this__rows_copy);
      }
      this.rows = __this__rows;
    }
    this.progressedPercentage = other.progressedPercentage;
    if (other.isSetStatus()) {
      this.status = other.status;
    }
    if (other.isSetFooterSummary()) {
      this.footerSummary = other.footerSummary;
    }
    this.startTime = other.startTime;
  }

  public TProgressUpdateResp deepCopy() {
    return new TProgressUpdateResp(this);
  }

  @Override
  public void clear() {
    this.headerNames = null;
    this.rows = null;
    setProgressedPercentageIsSet(false);
    this.progressedPercentage = 0.0;
    this.status = null;
    this.footerSummary = null;
    setStartTimeIsSet(false);
    this.startTime = 0;
  }

  public int getHeaderNamesSize() {
    return (this.headerNames == null) ? 0 : this.headerNames.size();
  }

  public java.util.Iterator<String> getHeaderNamesIterator() {
    return (this.headerNames == null) ? null : this.headerNames.iterator();
  }

  public void addToHeaderNames(String elem) {
    if (this.headerNames == null) {
      this.headerNames = new ArrayList<String>();
    }
    this.headerNames.add(elem);
  }

  public List<String> getHeaderNames() {
    return this.headerNames;
  }

  public void setHeaderNames(List<String> headerNames) {
    this.headerNames = headerNames;
  }

  public void unsetHeaderNames() {
    this.headerNames = null;
  }

  /** Returns true if field headerNames is set (has been assigned a value) and false otherwise */
  public boolean isSetHeaderNames() {
    return this.headerNames != null;
  }

  public void setHeaderNamesIsSet(boolean value) {
    if (!value) {
      this.headerNames = null;
    }
  }

  public int getRowsSize() {
    return (this.rows == null) ? 0 : this.rows.size();
  }

  public java.util.Iterator<List<String>> getRowsIterator() {
    return (this.rows == null) ? null : this.rows.iterator();
  }

  public void addToRows(List<String> elem) {
    if (this.rows == null) {
      this.rows = new ArrayList<List<String>>();
    }
    this.rows.add(elem);
  }

  public List<List<String>> getRows() {
    return this.rows;
  }

  public void setRows(List<List<String>> rows) {
    this.rows = rows;
  }

  public void unsetRows() {
    this.rows = null;
  }

  /** Returns true if field rows is set (has been assigned a value) and false otherwise */
  public boolean isSetRows() {
    return this.rows != null;
  }

  public void setRowsIsSet(boolean value) {
    if (!value) {
      this.rows = null;
    }
  }

  public double getProgressedPercentage() {
    return this.progressedPercentage;
  }

  public void setProgressedPercentage(double progressedPercentage) {
    this.progressedPercentage = progressedPercentage;
    setProgressedPercentageIsSet(true);
  }

  public void unsetProgressedPercentage() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __PROGRESSEDPERCENTAGE_ISSET_ID);
  }

  /** Returns true if field progressedPercentage is set (has been assigned a value) and false otherwise */
  public boolean isSetProgressedPercentage() {
    return EncodingUtils.testBit(__isset_bitfield, __PROGRESSEDPERCENTAGE_ISSET_ID);
  }

  public void setProgressedPercentageIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __PROGRESSEDPERCENTAGE_ISSET_ID, value);
  }

  /**
   * 
   * @see TJobExecutionStatus
   */
  public TJobExecutionStatus getStatus() {
    return this.status;
  }

  /**
   * 
   * @see TJobExecutionStatus
   */
  public void setStatus(TJobExecutionStatus status) {
    this.status = status;
  }

  public void unsetStatus() {
    this.status = null;
  }

  /** Returns true if field status is set (has been assigned a value) and false otherwise */
  public boolean isSetStatus() {
    return this.status != null;
  }

  public void setStatusIsSet(boolean value) {
    if (!value) {
      this.status = null;
    }
  }

  public String getFooterSummary() {
    return this.footerSummary;
  }

  public void setFooterSummary(String footerSummary) {
    this.footerSummary = footerSummary;
  }

  public void unsetFooterSummary() {
    this.footerSummary = null;
  }

  /** Returns true if field footerSummary is set (has been assigned a value) and false otherwise */
  public boolean isSetFooterSummary() {
    return this.footerSummary != null;
  }

  public void setFooterSummaryIsSet(boolean value) {
    if (!value) {
      this.footerSummary = null;
    }
  }

  public long getStartTime() {
    return this.startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
    setStartTimeIsSet(true);
  }

  public void unsetStartTime() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __STARTTIME_ISSET_ID);
  }

  /** Returns true if field startTime is set (has been assigned a value) and false otherwise */
  public boolean isSetStartTime() {
    return EncodingUtils.testBit(__isset_bitfield, __STARTTIME_ISSET_ID);
  }

  public void setStartTimeIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __STARTTIME_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case HEADER_NAMES:
      if (value == null) {
        unsetHeaderNames();
      } else {
        setHeaderNames((List<String>)value);
      }
      break;

    case ROWS:
      if (value == null) {
        unsetRows();
      } else {
        setRows((List<List<String>>)value);
      }
      break;

    case PROGRESSED_PERCENTAGE:
      if (value == null) {
        unsetProgressedPercentage();
      } else {
        setProgressedPercentage((Double)value);
      }
      break;

    case STATUS:
      if (value == null) {
        unsetStatus();
      } else {
        setStatus((TJobExecutionStatus)value);
      }
      break;

    case FOOTER_SUMMARY:
      if (value == null) {
        unsetFooterSummary();
      } else {
        setFooterSummary((String)value);
      }
      break;

    case START_TIME:
      if (value == null) {
        unsetStartTime();
      } else {
        setStartTime((Long)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case HEADER_NAMES:
      return getHeaderNames();

    case ROWS:
      return getRows();

    case PROGRESSED_PERCENTAGE:
      return getProgressedPercentage();

    case STATUS:
      return getStatus();

    case FOOTER_SUMMARY:
      return getFooterSummary();

    case START_TIME:
      return getStartTime();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case HEADER_NAMES:
      return isSetHeaderNames();
    case ROWS:
      return isSetRows();
    case PROGRESSED_PERCENTAGE:
      return isSetProgressedPercentage();
    case STATUS:
      return isSetStatus();
    case FOOTER_SUMMARY:
      return isSetFooterSummary();
    case START_TIME:
      return isSetStartTime();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof TProgressUpdateResp)
      return this.equals((TProgressUpdateResp)that);
    return false;
  }

  public boolean equals(TProgressUpdateResp that) {
    if (that == null)
      return false;

    boolean this_present_headerNames = true && this.isSetHeaderNames();
    boolean that_present_headerNames = true && that.isSetHeaderNames();
    if (this_present_headerNames || that_present_headerNames) {
      if (!(this_present_headerNames && that_present_headerNames))
        return false;
      if (!this.headerNames.equals(that.headerNames))
        return false;
    }

    boolean this_present_rows = true && this.isSetRows();
    boolean that_present_rows = true && that.isSetRows();
    if (this_present_rows || that_present_rows) {
      if (!(this_present_rows && that_present_rows))
        return false;
      if (!this.rows.equals(that.rows))
        return false;
    }

    boolean this_present_progressedPercentage = true;
    boolean that_present_progressedPercentage = true;
    if (this_present_progressedPercentage || that_present_progressedPercentage) {
      if (!(this_present_progressedPercentage && that_present_progressedPercentage))
        return false;
      if (this.progressedPercentage != that.progressedPercentage)
        return false;
    }

    boolean this_present_status = true && this.isSetStatus();
    boolean that_present_status = true && that.isSetStatus();
    if (this_present_status || that_present_status) {
      if (!(this_present_status && that_present_status))
        return false;
      if (!this.status.equals(that.status))
        return false;
    }

    boolean this_present_footerSummary = true && this.isSetFooterSummary();
    boolean that_present_footerSummary = true && that.isSetFooterSummary();
    if (this_present_footerSummary || that_present_footerSummary) {
      if (!(this_present_footerSummary && that_present_footerSummary))
        return false;
      if (!this.footerSummary.equals(that.footerSummary))
        return false;
    }

    boolean this_present_startTime = true;
    boolean that_present_startTime = true;
    if (this_present_startTime || that_present_startTime) {
      if (!(this_present_startTime && that_present_startTime))
        return false;
      if (this.startTime != that.startTime)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_headerNames = true && (isSetHeaderNames());
    list.add(present_headerNames);
    if (present_headerNames)
      list.add(headerNames);

    boolean present_rows = true && (isSetRows());
    list.add(present_rows);
    if (present_rows)
      list.add(rows);

    boolean present_progressedPercentage = true;
    list.add(present_progressedPercentage);
    if (present_progressedPercentage)
      list.add(progressedPercentage);

    boolean present_status = true && (isSetStatus());
    list.add(present_status);
    if (present_status)
      list.add(status.getValue());

    boolean present_footerSummary = true && (isSetFooterSummary());
    list.add(present_footerSummary);
    if (present_footerSummary)
      list.add(footerSummary);

    boolean present_startTime = true;
    list.add(present_startTime);
    if (present_startTime)
      list.add(startTime);

    return list.hashCode();
  }

  @Override
  public int compareTo(TProgressUpdateResp other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetHeaderNames()).compareTo(other.isSetHeaderNames());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetHeaderNames()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.headerNames, other.headerNames);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetRows()).compareTo(other.isSetRows());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetRows()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.rows, other.rows);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetProgressedPercentage()).compareTo(other.isSetProgressedPercentage());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetProgressedPercentage()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.progressedPercentage, other.progressedPercentage);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetStatus()).compareTo(other.isSetStatus());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStatus()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.status, other.status);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetFooterSummary()).compareTo(other.isSetFooterSummary());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetFooterSummary()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.footerSummary, other.footerSummary);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetStartTime()).compareTo(other.isSetStartTime());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStartTime()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.startTime, other.startTime);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("TProgressUpdateResp(");
    boolean first = true;

    sb.append("headerNames:");
    if (this.headerNames == null) {
      sb.append("null");
    } else {
      sb.append(this.headerNames);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("rows:");
    if (this.rows == null) {
      sb.append("null");
    } else {
      sb.append(this.rows);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("progressedPercentage:");
    sb.append(this.progressedPercentage);
    first = false;
    if (!first) sb.append(", ");
    sb.append("status:");
    if (this.status == null) {
      sb.append("null");
    } else {
      sb.append(this.status);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("footerSummary:");
    if (this.footerSummary == null) {
      sb.append("null");
    } else {
      sb.append(this.footerSummary);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("startTime:");
    sb.append(this.startTime);
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!isSetHeaderNames()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'headerNames' is unset! Struct:" + toString());
    }

    if (!isSetRows()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'rows' is unset! Struct:" + toString());
    }

    if (!isSetProgressedPercentage()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'progressedPercentage' is unset! Struct:" + toString());
    }

    if (!isSetStatus()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'status' is unset! Struct:" + toString());
    }

    if (!isSetFooterSummary()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'footerSummary' is unset! Struct:" + toString());
    }

    if (!isSetStartTime()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'startTime' is unset! Struct:" + toString());
    }

    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class TProgressUpdateRespStandardSchemeFactory implements SchemeFactory {
    public TProgressUpdateRespStandardScheme getScheme() {
      return new TProgressUpdateRespStandardScheme();
    }
  }

  private static class TProgressUpdateRespStandardScheme extends StandardScheme<TProgressUpdateResp> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, TProgressUpdateResp struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // HEADER_NAMES
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list190 = iprot.readListBegin();
                struct.headerNames = new ArrayList<String>(_list190.size);
                String _elem191;
                for (int _i192 = 0; _i192 < _list190.size; ++_i192)
                {
                  _elem191 = iprot.readString();
                  struct.headerNames.add(_elem191);
                }
                iprot.readListEnd();
              }
              struct.setHeaderNamesIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // ROWS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list193 = iprot.readListBegin();
                struct.rows = new ArrayList<List<String>>(_list193.size);
                List<String> _elem194;
                for (int _i195 = 0; _i195 < _list193.size; ++_i195)
                {
                  {
                    org.apache.thrift.protocol.TList _list196 = iprot.readListBegin();
                    _elem194 = new ArrayList<String>(_list196.size);
                    String _elem197;
                    for (int _i198 = 0; _i198 < _list196.size; ++_i198)
                    {
                      _elem197 = iprot.readString();
                      _elem194.add(_elem197);
                    }
                    iprot.readListEnd();
                  }
                  struct.rows.add(_elem194);
                }
                iprot.readListEnd();
              }
              struct.setRowsIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // PROGRESSED_PERCENTAGE
            if (schemeField.type == org.apache.thrift.protocol.TType.DOUBLE) {
              struct.progressedPercentage = iprot.readDouble();
              struct.setProgressedPercentageIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // STATUS
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.status = org.apache.spark.sql.thriftserver.rpc.thrift.TJobExecutionStatus.findByValue(iprot.readI32());
              struct.setStatusIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 5: // FOOTER_SUMMARY
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.footerSummary = iprot.readString();
              struct.setFooterSummaryIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 6: // START_TIME
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.startTime = iprot.readI64();
              struct.setStartTimeIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, TProgressUpdateResp struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.headerNames != null) {
        oprot.writeFieldBegin(HEADER_NAMES_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, struct.headerNames.size()));
          for (String _iter199 : struct.headerNames)
          {
            oprot.writeString(_iter199);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.rows != null) {
        oprot.writeFieldBegin(ROWS_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.LIST, struct.rows.size()));
          for (List<String> _iter200 : struct.rows)
          {
            {
              oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, _iter200.size()));
              for (String _iter201 : _iter200)
              {
                oprot.writeString(_iter201);
              }
              oprot.writeListEnd();
            }
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(PROGRESSED_PERCENTAGE_FIELD_DESC);
      oprot.writeDouble(struct.progressedPercentage);
      oprot.writeFieldEnd();
      if (struct.status != null) {
        oprot.writeFieldBegin(STATUS_FIELD_DESC);
        oprot.writeI32(struct.status.getValue());
        oprot.writeFieldEnd();
      }
      if (struct.footerSummary != null) {
        oprot.writeFieldBegin(FOOTER_SUMMARY_FIELD_DESC);
        oprot.writeString(struct.footerSummary);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(START_TIME_FIELD_DESC);
      oprot.writeI64(struct.startTime);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TProgressUpdateRespTupleSchemeFactory implements SchemeFactory {
    public TProgressUpdateRespTupleScheme getScheme() {
      return new TProgressUpdateRespTupleScheme();
    }
  }

  private static class TProgressUpdateRespTupleScheme extends TupleScheme<TProgressUpdateResp> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TProgressUpdateResp struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      {
        oprot.writeI32(struct.headerNames.size());
        for (String _iter202 : struct.headerNames)
        {
          oprot.writeString(_iter202);
        }
      }
      {
        oprot.writeI32(struct.rows.size());
        for (List<String> _iter203 : struct.rows)
        {
          {
            oprot.writeI32(_iter203.size());
            for (String _iter204 : _iter203)
            {
              oprot.writeString(_iter204);
            }
          }
        }
      }
      oprot.writeDouble(struct.progressedPercentage);
      oprot.writeI32(struct.status.getValue());
      oprot.writeString(struct.footerSummary);
      oprot.writeI64(struct.startTime);
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TProgressUpdateResp struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      {
        org.apache.thrift.protocol.TList _list205 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, iprot.readI32());
        struct.headerNames = new ArrayList<String>(_list205.size);
        String _elem206;
        for (int _i207 = 0; _i207 < _list205.size; ++_i207)
        {
          _elem206 = iprot.readString();
          struct.headerNames.add(_elem206);
        }
      }
      struct.setHeaderNamesIsSet(true);
      {
        org.apache.thrift.protocol.TList _list208 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.LIST, iprot.readI32());
        struct.rows = new ArrayList<List<String>>(_list208.size);
        List<String> _elem209;
        for (int _i210 = 0; _i210 < _list208.size; ++_i210)
        {
          {
            org.apache.thrift.protocol.TList _list211 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, iprot.readI32());
            _elem209 = new ArrayList<String>(_list211.size);
            String _elem212;
            for (int _i213 = 0; _i213 < _list211.size; ++_i213)
            {
              _elem212 = iprot.readString();
              _elem209.add(_elem212);
            }
          }
          struct.rows.add(_elem209);
        }
      }
      struct.setRowsIsSet(true);
      struct.progressedPercentage = iprot.readDouble();
      struct.setProgressedPercentageIsSet(true);
      struct.status = org.apache.spark.sql.thriftserver.rpc.thrift.TJobExecutionStatus.findByValue(iprot.readI32());
      struct.setStatusIsSet(true);
      struct.footerSummary = iprot.readString();
      struct.setFooterSummaryIsSet(true);
      struct.startTime = iprot.readI64();
      struct.setStartTimeIsSet(true);
    }
  }

}

