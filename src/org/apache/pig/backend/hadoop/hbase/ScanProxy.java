package org.apache.pig.backend.hadoop.hbase;


import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;

public class ScanProxy {
  private static final Log LOG = LogFactory.getLog(ScanProxy.class);

  private static boolean setCacheBlocks_InitError_ = false;
  private static Method setCacheBlocks_Method_ = null;
  private static RuntimeException setCacheBlocks_InitException_ = null;

  private static boolean setCaching_InitError_ = false;
  private static Method setCaching_Method_ = null;
  private static RuntimeException setCaching_InitException_ = null;

  private static boolean setStartRow_InitError_ = false;
  private static Method setStartRow_Method_ = null;
  private static RuntimeException setStartRow_InitException_ = null;

  private static boolean setStopRow_InitError_ = false;
  private static Method setStopRow_Method_ = null;
  private static RuntimeException setStopRow_InitException_ = null;

  private static boolean setTimeRange_InitError_ = false;
  private static Method setTimeRange_Method_ = null;
  private static RuntimeException setTimeRange_InitException_ = null;

  private static boolean setTimeStamp_InitError_ = false;
  private static Method setTimeStamp_Method_ = null;
  private static RuntimeException setTimeStamp_InitException_ = null;

  private static boolean addColumn_InitError_ = false;
  private static Method addColumn_Method_ = null;
  private static RuntimeException addColumn_InitException_ = null;

  private static boolean addFamily_InitError_ = false;
  private static Method addFamily_Method_ = null;
  private static RuntimeException addFamily_InitException_ = null;

  private static boolean setFilter_InitError_ = false;
  private static Method setFilter_Method_ = null;
  private static RuntimeException setFilter_InitException_ = null;

  private static boolean setMaxResultsPerColumnFamily_InitError_ = false;
  private static Method setMaxResultsPerColumnFamily_Method_ = null;
  private static RuntimeException setMaxResultsPerColumnFamily_InitException_ = null;


  static {
    try {
      setCacheBlocks_Method_ = Scan.class.getMethod("setCacheBlocks", boolean.class);
    } catch (NoSuchMethodException | SecurityException e) {
      setCacheBlocks_InitException_ = new RuntimeException("cannot find org.apache.hadoop.hbase.client.Scan.setCacheBlocks(boolean) ", e);
      setCacheBlocks_InitError_ = true;
    }

    try {
      setCaching_Method_ = Scan.class.getMethod("setCaching", int.class);
    } catch (NoSuchMethodException | SecurityException e) {
      setCaching_InitException_ = new RuntimeException("cannot find org.apache.hadoop.hbase.client.Scan.setCaching(int) ", e);
      setCaching_InitError_ = true;
    }

    try {
      setStartRow_Method_ = Scan.class.getMethod("setStartRow", byte[].class);
    } catch (NoSuchMethodException | SecurityException e) {
      setStartRow_InitException_ = new RuntimeException("cannot find org.apache.hadoop.hbase.client.Scan.setStartRow(byte[]) ", e);
      setStartRow_InitError_ = true;
    }

    try {
      setStopRow_Method_ = Scan.class.getMethod("setStopRow", byte[].class);
    } catch (NoSuchMethodException | SecurityException e) {
      setStopRow_InitException_ = new RuntimeException("cannot find org.apache.hadoop.hbase.client.Scan.setStopRow(byte[]) ", e);
      setStopRow_InitError_ = true;
    }

    try {
      setTimeRange_Method_ = Scan.class.getMethod("setTimeRange", long.class, long.class);
    } catch (NoSuchMethodException | SecurityException e) {
      setTimeRange_InitException_ = new RuntimeException("cannot find org.apache.hadoop.hbase.client.Scan.setTimeRange(long, long) ", e);
      setTimeRange_InitError_ = true;
    }

    try {
      setTimeStamp_Method_ = Scan.class.getMethod("setTimeStamp", long.class);
    } catch (NoSuchMethodException | SecurityException e) {
      setTimeStamp_InitException_ = new RuntimeException("cannot find org.apache.hadoop.hbase.client.Scan.setTimeStamp(long) ", e);
      setTimeStamp_InitError_ = true;
    }

    try {
      addColumn_Method_ = Scan.class.getMethod("addColumn", byte[].class, byte[].class);
    } catch (NoSuchMethodException | SecurityException e) {
      addColumn_InitException_ = new RuntimeException("cannot find org.apache.hadoop.hbase.client.Scan.addColumn(byte[], byte[]) ", e);
      addColumn_InitError_ = true;
    }

    try {
      addFamily_Method_ = Scan.class.getMethod("addFamily", byte[].class);
    } catch (NoSuchMethodException | SecurityException e) {
      addFamily_InitException_ = new RuntimeException("cannot find org.apache.hadoop.hbase.client.Scan.addFamily(byte[]) ", e);
      addFamily_InitError_ = true;
    }

    try {
      setFilter_Method_ = Scan.class.getMethod("setFilter", Filter.class);
    } catch (NoSuchMethodException | SecurityException e) {
      setFilter_InitException_ = new RuntimeException("cannot find org.apache.hadoop.hbase.client.Scan.setFilter(Filter) ", e);
      setFilter_InitError_ = true;
    }

    try {
      setMaxResultsPerColumnFamily_Method_ = Scan.class.getMethod("setMaxResultsPerColumnFamily", int.class);
    } catch (NoSuchMethodException | SecurityException e) {
      setMaxResultsPerColumnFamily_InitException_ = new RuntimeException("cannot find org.apache.hadoop.hbase.client.Scan.setMaxResultsPerColumnFamily(int) ", e);
      setMaxResultsPerColumnFamily_InitError_ = true;
    }


  }

  public static void setCacheBlocks(Scan object, boolean cacheBlocks) {
    if (setCacheBlocks_InitError_) {
      throw setCacheBlocks_InitException_;
    }

    try {
      if (setCacheBlocks_Method_ != null) {
        LOG.debug("Call Scan::setCacheBlocks(boolean)");
        setCacheBlocks_Method_.invoke(object, cacheBlocks);
        return;
      }
      throw new RuntimeException("No org.apache.hadoop.hbase.client.Scan.setCacheBlocks(boolean) method found.");
    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }


  public static void setCaching(Scan object, int caching) {
    if (setCaching_InitError_) {
      throw setCaching_InitException_;
    }

    try {
      if (setCaching_Method_ != null) {
        LOG.debug("Call Scan::setCaching(int)");
        setCaching_Method_.invoke(object, caching);
        return;
      }
      throw new RuntimeException("No org.apache.hadoop.hbase.client.Scan.setCaching(int) method found.");
    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  public static void setStartRow(Scan object, byte[] startRow) {
    if (setStartRow_InitError_) {
      throw setStartRow_InitException_;
    }

    try {
      if (setStartRow_Method_ != null) {
        LOG.debug("Call Scan::setStartRow(byte[])");
        setStartRow_Method_.invoke(object, startRow);
        return;
      }
      throw new RuntimeException("No org.apache.hadoop.hbase.client.Scan.setStartRow(byte[]) method found.");
    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  public static void setStopRow(Scan object, byte[] stopRow) {
    if (setStopRow_InitError_) {
      throw setStopRow_InitException_;
    }

    try {
      if (setStopRow_Method_ != null) {
        LOG.debug("Call Scan::setStopRow(byte[])");
        setStopRow_Method_.invoke(object, stopRow);
        return;
      }
      throw new RuntimeException("No org.apache.hadoop.hbase.client.Scan.setStopRow(byte[]) method found.");
    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  public static void setTimeRange(Scan object, long minStamp, long maxStamp) {
    if (setTimeRange_InitError_) {
      throw setTimeRange_InitException_;
    }

    try {
      if (setTimeRange_Method_ != null) {
        LOG.debug("Call Scan::setTimeRange(long, long)");
        setTimeRange_Method_.invoke(object, minStamp, maxStamp);
        return;
      }
      throw new RuntimeException("No org.apache.hadoop.hbase.client.Scan.setTimeRange(long, long) method found.");
    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  public static void setTimeStamp(Scan object, long timestamp) {
    if (setTimeStamp_InitError_) {
      throw setTimeStamp_InitException_;
    }

    try {
      if (setTimeStamp_Method_ != null) {
        LOG.debug("Call Scan::setTimeStamp(long");
        setTimeStamp_Method_.invoke(object, timestamp);
        return;
      }
      throw new RuntimeException("No org.apache.hadoop.hbase.client.Scan.setTimeStamp(long) method found.");
    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  public static void addColumn(Scan object, byte [] family, byte [] qualifier) {
    if (addColumn_InitError_) {
      throw addColumn_InitException_;
    }

    try {
      if (addColumn_Method_ != null) {
        LOG.debug("Call Scan::addColumn(byte[], byte[])");
        addColumn_Method_.invoke(object, family, qualifier);
        return;
      }
      throw new RuntimeException("No org.apache.hadoop.hbase.client.Scan.addColumn(byte[], byte[]) method found.");
    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }


  public static void addFamily(Scan object, byte [] family) {
    if (addFamily_InitError_) {
      throw addFamily_InitException_;
    }

    try {
      if (addFamily_Method_ != null) {
        LOG.debug("Call Scan::addFamily(byte[])");
        addFamily_Method_.invoke(object, family);
        return;
      }
      throw new RuntimeException("No org.apache.hadoop.hbase.client.Scan.addFamily(byte[]) method found.");
    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }


  public static void setFilter(Scan object, Filter filter) {
    if (setFilter_InitError_) {
      throw setFilter_InitException_;
    }

    try {
      if (setFilter_Method_ != null) {
        LOG.debug("Call Scan::setFilter(Filter)");
        setFilter_Method_.invoke(object, filter);
        return;
      }
      throw new RuntimeException("No org.apache.hadoop.hbase.client.Scan.setFilter(Filter) method found.");
    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  public static void setMaxResultsPerColumnFamily(Scan object, int maxResultsPerColumnFamily) {
    if (setMaxResultsPerColumnFamily_InitError_) {
      throw setMaxResultsPerColumnFamily_InitException_;
    }

    try {
      if (setMaxResultsPerColumnFamily_Method_ != null) {
        LOG.debug("Call Scan::setMaxResultsPerColumnFamily(int)");
        setMaxResultsPerColumnFamily_Method_.invoke(object, maxResultsPerColumnFamily);
        return;
      }
      throw new RuntimeException("No org.apache.hadoop.hbase.client.Scan.setMaxResultsPerColumnFamily(int) method found.");
    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

}
