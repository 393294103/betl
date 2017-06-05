/**
 * @Email:zhanghelin@geotmt.com
 * @Author:zhl
 * @Date:2017年5月12日
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.hbase.mr.helper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PreSplitHBaseHelper {
	private static final Logger logger = LoggerFactory.getLogger(PreSplitHBaseHelper.class);
	private static String[] baseStr =new String[]{"0","1","2","3","4","5","6","7","8","9","a","b","c","d","e","f"};
	//最后的幂底数
	public static String POWER_LAST_BASE="powerLastBase";
	//幂底数
	public static String POWER_BASE="powerBase";
	//幂次方
	public static String POWER_NUM="powerNum";
	//返回幂次方是否成功
	public static String POWER_FLAG="powerFlag";
	
	
	/**
	 * 校验参数，reduce.num是否合法,只接受大于2，且是2的幂次方
	 * @param num
	 * @return boolean
	 */
	public static boolean checkNum(Integer num){
		boolean flag=true;
		int checkNum=num;
		if(num<=2){
		flag=false;
		}
		while(checkNum>0){
			if(checkNum%2!=0&&checkNum>1){
				flag=false;
			}
			checkNum=checkNum/2;
		}
		return flag;
	}
	
	
	
	/**
	 * 根据numPartitions来生成幂底和幂次方
	 * @return
	 */
	public static Map<String,Object> powerArgs(Integer num){
		Map<String,Object> result=new HashMap<String,Object>();
		//检查参数是否是2的幂次方
		int checkNum=num;
		boolean flag=checkNum(checkNum);
		if(!flag){
			result.put(POWER_FLAG, false);
			return result;
		}
		//幂底
		int powerBase=baseStr.length;
		//最后一个幂底
		int powerLastBase=num%powerBase;
		//例如，baseStr.length=16的幂次方eg:16*16*8
		int powerNum=0;
		while(num>0){
			powerLastBase=num;
			num=num/baseStr.length;
			powerNum++;
		}
		powerNum--;
		
		//如果最后的幂底数是1，那么就重新设置一下
		if(powerLastBase==1){
			powerNum--;
			powerLastBase=16;
		}
		
		result.put(POWER_BASE, baseStr.length);
		result.put(POWER_LAST_BASE,powerLastBase);
		result.put(POWER_NUM,powerNum);
		result.put(POWER_FLAG,true);
		logger.debug("powerArgs,num={},result={}",num,result);
		return result;
	}
	
	
	/**
	 * 发生join笛卡尔乘积
	 * @param left
	 * @param right
	 * @return List
	 */
	private static List<String> descartes(List<String> left,List<String> right){
		List<String> result=new ArrayList<String>(); 
		for (String l : left) {
			 for (String r : right) {
				 result.add(l+r);
			}
		}
			return result;
	}

	
	
	
	/**
	 * 根据reduce的个数进行穷举key的可能
	 * @param powerArgsMap
	 * @return list
	 */
	private static List<String> keyPreffixDescartes(Map<String,Object>  powerArgsMap){
				//array转成list，否则删除会有问题
				List<String> baseList=new ArrayList<String>();
				for (String bs : baseStr) {
					baseList.add(bs);
				}
				
				List<String> left=baseList;
				List<String> right=baseList;
				 
				int powerNum=(int) powerArgsMap.get(POWER_NUM);
				
				int i =0;
				while(i<powerNum){
					//判断是否最后一个powerLastBase
					if(i==(powerNum-1)){
						right=getPowerLastBaseList((int) powerArgsMap.get(POWER_LAST_BASE));
						//如果右侧大小为1，则跳出循环
						if(right.size()==1){
							break;
						}
					}
					logger.debug("keyPreffixDescartes,left={}",left);
					logger.debug("keyPreffixDescartes,right={}",right);
					left=descartes(left, right);
					i++;
				 } 
				
				 List<String> result=new ArrayList<String>();
				 if(powerNum<=0){
					 result=getPowerLastBaseList((int) powerArgsMap.get(POWER_LAST_BASE));
				 }else{
					 result=left; 
				 }
				 
		return result;
	}
	
	
	
	
	
	public static int getPartitionId(String partitionPreffixKey,Map<String,Object>  powerArgsMap){
	List<String> listPreffixKey=keyPreffixDescartes(powerArgsMap);
	StringBuilder logSb=new StringBuilder();
	for (int i=0;i<listPreffixKey.size();i++) {
		logSb.append(i).append("=").append(listPreffixKey.get(i)).append(",");
	}
	logger.debug("[getPartitionId],listPreffixKey={}",logSb);
	
	int result=listPreffixKey.size()-1;
	int partitionPreffixKeyInt=Integer.parseInt(partitionPreffixKey, 16);
	for (int i=0;i<listPreffixKey.size();i++) {
		String curListVal=listPreffixKey.get(i);
		int curListValInt=Integer.parseInt(curListVal, 16);
		
		if(partitionPreffixKey.length()!=curListVal.length()){
			logger.error("[getPartitionId],partitionPreffixKey={},curListVal={}",partitionPreffixKey,curListVal);
		}
		
		//如果相等直接返回
		if(curListValInt==partitionPreffixKeyInt){
			result=i;
			return result;
		}
		//如果大于取前一个值,直接返回
		if(curListValInt>partitionPreffixKeyInt){
			result=i-1;
			return result;
		}
	}
	return result;
}
	

	
	//根据最后的幂底数的大小,设置
	public static List<String> getPowerLastBaseList(int lastPowerBase){
		int lastPowerBaseListSize= baseStr.length/lastPowerBase;
		List<String> result=new  ArrayList<String>();
		for (int i = 0; i < baseStr.length; i++) {
			if((i%lastPowerBaseListSize)==0){
				result.add(baseStr[i]);
			}
		}
		return result;
	}
	
	
	
	
	/**
	 * 打印创建htab命令
	 * @param hTabName
	 * @param num
	 * @return String
	 */
	public static String showCreateHTab(String hTabName,int num){
		StringBuilder createHTabSb=new StringBuilder("create ");
		createHTabSb.append("'").append(hTabName).append("'").append(",");
		createHTabSb.append("{NAME => 'd', DATA_BLOCK_ENCODING => 'FAST_DIFF'}").append(",");
		createHTabSb.append("SPLITS => ");
		Map<String,Object>  powerArgsMap= powerArgs(num);
		//检查参数是否成功
		if((boolean)powerArgsMap.get(POWER_FLAG)){
		List<String> listPreffixKey=keyPreffixDescartes(powerArgsMap);
		 //删除最小和最大
		listPreffixKey.remove(0);
		listPreffixKey.remove(listPreffixKey.size()-1);
		
		int curMap=0;
		createHTabSb.append("[");
		for (String key : listPreffixKey) {
			createHTabSb.append("'");
			createHTabSb.append(key);
			createHTabSb.append("'");
			curMap++;
			if(curMap<listPreffixKey.size()){
				createHTabSb.append(",");
			}
		}
		createHTabSb.append("]");
		return createHTabSb.toString();
		}
		
		return  StringUtils.EMPTY;
	}
	

	
	
	
	public static void main(String[] args) {
		int num=2*2*2*2*2;
		System.out.println(num);
		Map<String,Object>  powerArgsMap= powerArgs(num);
		//检查参数是否成功
		if(!(boolean)powerArgsMap.get(POWER_FLAG)){
			System.out.println("参数有问题");
		}else{
		}
		
		/*String hTabCreateStr=showCreateHTab("gq_tag_7",num);
		System.out.println(hTabCreateStr);*/
		
		//Map<String,Object>  powerArgsMap= powerArgs(num);
		int res=getPartitionId("09",powerArgsMap);
		System.out.println(res);
	}
	

}
