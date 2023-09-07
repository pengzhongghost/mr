package com.redu.mapreduce.util;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.StrUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;

import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


/**
 * @author pengzhong
 * @since 2023/8/25
 */
public class MapJoinUtil {

    /**
     * mapper阶段mapjoin
     *
     * @param dirPath
     * @param configuration
     * @param aClass
     * @param <T>
     * @return
     * @throws Exception
     */
    public static <T> List<T> read(String dirPath, Configuration configuration, Class<T> aClass) throws Exception {
        List<T> result = null;
        List<Path> paths = HdfsUtil.ls(dirPath);
        for (Path path : paths) {
            Reader reader = null;
            RecordReader rows = null;
            try {
                // 生成reader
                reader = OrcFile.createReader(path, OrcFile.readerOptions(configuration));
                // 解析schema
                VectorizedRowBatch rowBatch = reader.getSchema().createRowBatch();
                // 流解析文件
                rows = reader.rows();
                result = new ArrayList<>();
                // 逐行读取之
                while (rows.nextBatch(rowBatch)) {
                    for (int i = 0; i < rowBatch.size; i++) {
                        T newObj = (T) aClass.newInstance();
                        for (int j = 0; j < rowBatch.cols.length; j++) {
                            ColumnVector col = rowBatch.cols[j];
                            Object val = null;
                            if (col instanceof LongColumnVector) {
                                LongColumnVector columnVector = (LongColumnVector) col;
                                val = columnVector.vector[i];
                            }
                            if (col instanceof BytesColumnVector) {
                                BytesColumnVector columnVector = (BytesColumnVector) col;
                                if (null != columnVector.vector[i]) {
                                    val = new String(columnVector.vector[i], columnVector.start[i], columnVector.length[i]);
                                }
                            }
                            if (col instanceof TimestampColumnVector) {
                                TimestampColumnVector columnVector = (TimestampColumnVector) col;
                                Timestamp timestamp = columnVector.getScratchTimestamp();
                                if (null != timestamp) {
                                    val = new Date(timestamp.getTime());
                                }
                            }
                            Field field = aClass.getDeclaredFields()[j];
                            field.setAccessible(true);
                            if (Integer.class.equals(field.getType())) {
                                field.set(newObj, Integer.parseInt(String.valueOf(val)));
                            } else if (Byte.class.equals(field.getType())) {
                                field.set(newObj, Byte.parseByte(String.valueOf(val)));
                            } else if (Date.class.equals(field.getType()) && val instanceof String) {
                                String value = (String) val;
                                if (StrUtil.isNotEmpty(value)) {
                                    field.set(newObj, DateUtil.parse(value, DatePattern.NORM_DATETIME_FORMAT));
                                }
                            } else {
                                if (null != val && !"".equals(val)) {
                                    field.set(newObj, val);
                                }
                            }
                        }
                        result.add(newObj);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            // 关reader
            rows.close();
            // 关流
            IOUtils.closeStream(reader);
        }
        return result;
    }

}
