/**
 * @Email:zhanghelin@geotmt.com
 * @Author:zhl
 * @Date:2017年6月5日
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.common.mr.mapper;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.betl.common.mr.common.BasicConstants;

public class Orc2TextMapper extends Mapper<NullWritable, OrcStruct, Text, Text> {
private static final Logger logger = LoggerFactory.getLogger(Orc2TextMapper.class);
private static StructObjectInspector input;


protected void setup(Context context) throws IOException, InterruptedException {
	super.setup(context);
	String inputSchema=context.getConfiguration().get(BasicConstants.ORC_INPUT_SCHEMA);
	TypeInfo inTI = TypeInfoUtils.getTypeInfoFromTypeString(context.getConfiguration().get(inputSchema));
	input = (StructObjectInspector) OrcStruct.createObjectInspector(inTI);
}

@Override
public void map(NullWritable key, OrcStruct value, Context context)
		throws IOException, InterruptedException {
	String splitBy=context.getConfiguration().get(BasicConstants.HDFS_COLUMN_SPLIT,"\t");
	//按照orc的schema解析出字段	
	List<Object> colVal = input.getStructFieldsDataAsList(value);
	StringBuilder rowSb=new StringBuilder();
	for (Object obj : colVal) {
		rowSb.append(obj.toString()).append(splitBy);
	}
	logger.debug("key={}",rowSb.toString());
	context.write(new Text(rowSb.toString()), new Text());
}




}
