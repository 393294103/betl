/**
 * @Email:zhanghelin@geotmt.com
 * @Author:zhl
 * @Date:2016年12月21日上午10:33:06
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.mysql.mr.reducer;

import java.io.IOException;
import java.lang.reflect.Field;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.betl.mysql.conf.ConfigHelper;
import com.betl.mysql.mr.model.v1.IModelRecord;
import com.betl.mysql.mr.model.v1.ModelRecordImplCode;

/**
 * @author zhl
 *
 */
public class HdfsToMysqlReducer extends Reducer<LongWritable, Text, IModelRecord, NullWritable> {
	private Logger logger = LoggerFactory.getLogger(getClass());
	@SuppressWarnings("rawtypes")
	private Class clazz;
	private ConfigHelper cf;

	@Override
	protected void setup(Reducer<LongWritable, Text, IModelRecord, NullWritable>.Context context) throws IOException, InterruptedException {
		super.setup(context);
		try {
			ConfigHelper cf = new ConfigHelper(context.getConfiguration());
			this.cf = cf;
			ModelRecordImplCode modelRecordImplCode = new ModelRecordImplCode(cf);
			String code = modelRecordImplCode.gengerate();
			String modelClassPath = modelRecordImplCode.compile(code);
			clazz = modelRecordImplCode.gengerateClass(modelClassPath);
		} catch (ClassNotFoundException e) {
			logger.error("[setup-Exception]{}", e.getMessage());
			e.printStackTrace();
		}
	}

	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for (Text val : values) {
			//logger.debug("[reduce-val]\t{}", val);
			String[] hdfsFields = val.toString().split(context.getConfiguration().get("hdfs.columns.split"));

			@SuppressWarnings("rawtypes")
			Class c = clazz;
			Field field;
			try {
				String[] mysqlFields = cf.mysqlColumns();
				int i = 0;
				Object obj = c.newInstance();// 实例化对象
				for (String mf : mysqlFields) {
					field = c.getDeclaredField(mf);
					field.set(obj, hdfsFields[i]);// 为字段赋值
					i++;
				}

				context.write((IModelRecord) obj, NullWritable.get());

			} catch (NoSuchFieldException | SecurityException | InstantiationException | IllegalAccessException e) {
				logger.error("[reduce-Exception]{}", e.getMessage());
				e.printStackTrace();
			}

		}

	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {

	}
}
