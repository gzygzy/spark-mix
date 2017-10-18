package hive_jdbc_handler;

/**
 * Created by 冯刚 on 2017/8/8.
 */
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.mapred.JobConf;
import java.io.IOException;
import java.util.Properties;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.Progressable;

import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;
import org.apache.hadoop.mapred.lib.db.DBWritable;

import java.lang.Exception;
public class HiveJdbcOutPutHandler<V> extends DBOutputFormat<DBWritable,V> implements HiveOutputFormat<DBWritable, V>{

    private static final Log LOG = LogFactory.getLog(HiveJdbcOutPutHandler.class);
    private org.apache.hadoop.mapreduce.RecordWriter recordWriter;
    private TaskAttemptContext taskContext;

    @Override
    public void checkOutputSpecs(FileSystem filesystem, JobConf job)
            throws IOException {
    }

    public RecordWriter getHiveRecordWriter(JobConf jobConf, Path finalOutPath,
                                            Class<? extends Writable> valueClass, boolean isCompressed,
                                            Properties tableProperties, Progressable progress)
            throws IOException {

        /*if (LOG.isDebugEnabled()) {
            LOG.debug("jobConf: " + jobConf);
            LOG.debug("tableProperties: " + tableProperties);
        }
        taskContext = ShimLoader.getHadoopShims().newTaskAttemptContext(
                jobConf, null);
        org.apache.hadoop.mapreduce.lib.db.DBOutputFormat delegate = new org.apache.hadoop.mapreduce.lib.db.DBOutputFormat();
        recordWriter = delegate.getRecordWriter(taskContext);*/
        // Wrapping DBRecordWriter in JdbcRecordWriter
        /*return new JdbcRecordWriter(
                (org.apache.hadoop.mapreduce.lib.db.DBOutputFormat.DBRecordWriter) recordWriter);*/
        return null;
    }

    public void isTrueTable() throws Exception{

            throw new Exception("can't insert data into a unavailable table");
    }
}
