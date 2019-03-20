package io.reactiverse.myclient.impl.codec.datatype;

import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.collection.IntObjectMap;
import io.reactiverse.myclient.impl.protocol.backend.ColumnDefinition;
import io.reactiverse.pgclient.data.Numeric;

public enum DataType {
  INT2(ColumnDefinition.ColumnType.MYSQL_TYPE_SHORT, true, Short.class),
  INT3(ColumnDefinition.ColumnType.MYSQL_TYPE_INT24, true, Integer.class),
  INT4(ColumnDefinition.ColumnType.MYSQL_TYPE_LONG, true, Integer.class),
  INT8(ColumnDefinition.ColumnType.MYSQL_TYPE_LONGLONG, true, Long.class),
  DOUBLE(ColumnDefinition.ColumnType.MYSQL_TYPE_DOUBLE, true, Double.class),
  FLOAT(ColumnDefinition.ColumnType.MYSQL_TYPE_FLOAT, true, Float.class),
  NUMERIC(ColumnDefinition.ColumnType.MYSQL_TYPE_NEWDECIMAL, true, Numeric.class),
//  DECIMAL(ColumnDefinition.ColumnType.MYSQL_TYPE_NEWDECIMAL, true, Numeric.class), DECIMAL is a synonym for NUMERIC
  //TODO confirm VARCHAR data type map to 'MYSQL_TYPE_VAR_STRING'?
  VARCHAR(ColumnDefinition.ColumnType.MYSQL_TYPE_VAR_STRING, true, String.class);

  private static IntObjectMap<DataType> idToDataType = new IntObjectHashMap<>();

  static {
    for (DataType dataType : values()) {
      idToDataType.put(dataType.id, dataType);
    }
  }

  public final int id;
  public final boolean supportsBinary; // TODO do we need this for MySQL?
  public final Class<?> encodingType; // Not really used for now
  public final Class<?> decodingType;

  DataType(int id, boolean supportsBinary, Class<?> type) {
    this.id = id;
    this.supportsBinary = supportsBinary;
    this.decodingType = type;
    this.encodingType = type;
  }

  DataType(int id, boolean supportsBinary, Class<?> encodingType, Class<?> decodingType) {
    this.id = id;
    this.supportsBinary = supportsBinary;
    this.encodingType = encodingType;
    this.decodingType = decodingType;
  }

  public static DataType valueOf(int value) {
    DataType dataType = idToDataType.get(value);
    if (dataType == null) {
//      logger.warn("MySQL type =" + value + " not handled - using unknown type instead");
      //TODO need better handling
      return null;
    } else {
      return dataType;
    }
  }
}