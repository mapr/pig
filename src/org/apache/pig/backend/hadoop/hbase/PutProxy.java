package org.apache.pig.backend.hadoop.hbase;


import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Put;

public class PutProxy {
  private static final Log LOG = LogFactory.getLog(PutProxy.class);

  private static boolean add_InitError_ = false;
  private static Method add_Method_ = null;
  private static RuntimeException add_InitException_ = null;

  static {
    try {
      add_Method_ = Put.class.getMethod("org.apache.hadoop.hbase.client.Put", Put.class);
    } catch (NoSuchMethodException | SecurityException e) {
      add_InitException_ = new RuntimeException("cannot find org.apache.hadoop.hbase.client.Put.add(byte [], byte [], long, byte [])", e);
      add_InitError_ = true;
    }
  }

  public static void add(Put object, byte [] family, byte [] qualifier, long ts, byte [] value) {
    if (add_InitError_) {
      throw add_InitException_;
    }

    try {
      if (add_Method_ != null) {
        LOG.debug("Call Put::add(byte [], byte [], long, byte [])");
        add_Method_.invoke(object, family, qualifier, ts, value);
        return;
      }
      throw new RuntimeException("No org.apache.hadoop.hbase.client.Put.add(byte [], byte [], long, byte []) method found.");
    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

}
