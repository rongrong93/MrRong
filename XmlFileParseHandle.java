package com.newland.crm.cmc.biz.sync;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;

import com.newland.csf.frame.util.XmlUtils;


public class XmlFileParseHandle {
	private  Map<Integer,Integer> listSize = new HashMap<Integer,Integer>();
	private  boolean contineScan = true;
	private  Element rootE;
	public List<Integer> indexList;
	public boolean isFirstScan = true;
	public static void main(String[] args) throws DocumentException  {
		new Test().handle();
	}

	public void handle() throws DocumentException{
		StringBuffer sxml = new StringBuffer();
		sxml.append("<RolBakFile><SvcCont><![CDATA[<?xml version='1.0' encoding='GBK' ?><RolBakChk><ReqDay>20180808</ReqDay><RolBakList><ReqType>0</ReqType><PriMSISDN>13800000000</PriMSISDN><EID>666666</EID><AuxIMEI>dfkg4454548</AuxIMEI><AuxType>12</AuxType><ProfileStatus>01</ProfileStatus><names><name>zhangsan1</name><age>18</age></names><names><name>zhangsan2</name><age>18</age></names><names><name>zhangsan3</name><age>18</age></names><names><name>zhangsan4</name><age>18</age></names><CreateTime>20180808120000</CreateTime></RolBakList><RolBakList><names><name>zhangsan5</name><age>18</age></names><names><name>zhangsan6</name><age>18</age></names><names><name>zhangsan7</name><age>18</age></names><names><name>zhangsan8</name><age>18</age></names><ReqType>1</ReqType><PriMSISDN>13900000000</PriMSISDN><EID>666667</EID><AuxIMEI>dfkg22454548</AuxIMEI><AuxType>1</AuxType><ProfileStatus>0</ProfileStatus><CreateTime>20190808120000</CreateTime></RolBakList></RolBakChk>]]></SvcCont></RolBakFile>");
		//sxml.append("<RolBakFile><SvcCont><![CDATA[<?xml version='1.0' encoding='GBK' ?><RolBakChk><ReqDay>20180808</ReqDay><RolBakList><ReqType>0</ReqType><PriMSISDN>13800000000</PriMSISDN><EID>666666</EID><AuxIMEI>dfkg4454548</AuxIMEI><AuxType>12</AuxType><ProfileStatus>01</ProfileStatus><CreateTime>20180808120000</CreateTime></RolBakList><RolBakList><ReqType>1</ReqType><PriMSISDN>13900000000</PriMSISDN><EID>666667</EID><AuxIMEI>dfkg22454548</AuxIMEI><AuxType>1</AuxType><ProfileStatus>0</ProfileStatus><CreateTime>20190808120000</CreateTime></RolBakList></RolBakChk>]]></SvcCont></RolBakFile>");
		Map<String, String> rule_map = elementRule2Map();
		getParaMaps(rule_map,sxml.toString());
		System.out.println("---------------finish---------------");
	}
	
	/**
	 * 将xml元素规则存入map中
	 * @param element_rule
	 * @throws DocumentException
	 */
	public static Map<String,String> elementRule2Map() throws DocumentException{
		String element_rule = "isCDATA:true;CTADAPath:RolBakFile/SvcCont;"
				+ "mainElement:"
				+ "/RolBakChk/ReqDay,"
				+ "/RolBakChk/RolBakList/ReqType,"
				+ "/RolBakChk/RolBakList/PriMSISDN,"
				+ "/RolBakChk/RolBakList/EID,"
				+ "/RolBakChk/RolBakList/AuxIMEI,"
				+ "/RolBakChk/RolBakList/AuxType,"
				+ "/RolBakChk/RolBakList/ProfileStatus,"
				+ "/RolBakChk/RolBakList/CreateTime,"
				+ "/RolBakChk/RolBakList/names/name;";
		String[] rules = element_rule.split(";");
		Map<String,String> rule_map = new HashMap<String,String>();
		
		/**
		 * 获得xml格式配置规则存入map中
		 */
		for(String rule : rules){
			String[] key_value = rule.split(":");
			String key = key_value[0];
			String value = key_value[1];
			rule_map.put(key, value);
		}
		return rule_map;
	}
	//除根节点外,取值最多层数
	public int maxDep = 0;
	public void  getParaMaps(Map<String,String> rule_map,String xml) throws DocumentException{
		String[] main_element = rule_map.get("mainElement").split(",");
		Map<String,Boolean> paraMap = new HashMap<String,Boolean>();
		for(String para : main_element){
			paraMap.put(para, true);
			if(para.split("/").length - 2 > maxDep){
				maxDep = para.split("/").length - 2;
			}
		}
		Document doc = null;
		doc = XmlUtils.parseText(xml);
		rootE = doc.getRootElement();
		String sxml = ""; 
		boolean isCDATA = Boolean.parseBoolean(rule_map.get("isCDATA"));
		if(isCDATA){
			String[] CTADAPath = rule_map.get("CTADAPath").split("/");
			//获得CDATA的上一级元素
			for(int i = 1; i < CTADAPath.length; i ++){
				rootE = rootE.element(CTADAPath[i]);
			}
			sxml = rootE.getText();
		}else{
			sxml = xml;
		}
		doc = XmlUtils.parseText(sxml);
		rootE = doc.getRootElement();
		indexList = new ArrayList<Integer>();
		for(int i = 0; i < maxDep; i++){
			indexList.add(-1);
		}
		Map<String,String> resultMap;

		while(contineScan){
			resultMap = new HashMap<String,String>();
			System.out.println("==============================");
			//每次获得一个节点值
			for(String nodeName : main_element){
				String[] nodeNames = nodeName.split("/");
				repeatScanNode(rootE,nodeNames,resultMap,0);
			}
			
			isFirstScan = false;
			//入库
			
			
			contineScan = doNextList();
		}
	}
	
	@SuppressWarnings("unchecked")
	public void repeatScanNode(Element parentElement, String[] nodeNames, Map<String, String> resultMap,int i)
	{
		String nodeName = nodeNames[i + 2];
		List<Element> nextElements = parentElement.elements(nodeName);
		Element nextElement = null;
		
		if(nextElements.size() > 1){
			if(isFirstScan){
				nextElement = nextElements.get(0);
				listSize.put(i, nextElements.size());
				if(indexList.get(i) < 0){
					indexList.set(i, 0);
				}
			}else{
				nextElement = nextElements.get(indexList.get(i));
			}
			repeatScanNode(nextElement,nodeNames,resultMap,++i);
		}else if(parentElement.element(nodeName).isTextOnly()){//叶子节点
			System.out.println(parentElement.element(nodeName).getPath() + "=" + nextElements.get(0).getText());
			resultMap.put(parentElement.element(nodeName).getPath(),nextElements.get(0).getText());
		}else{//非list的中间节点
			nextElement = parentElement.element(nodeName);
			repeatScanNode(nextElement,nodeNames,resultMap,++i);
		}
	}

	/**
	 * @param parentElement 上级元素
	 * @param nodeNames  遍历节点层次
	 * @param resultMap  结果集合
	 * @param i 集合下标
	 * 第一次扫描xml数据
	 */
	/**
	 * 将最深一层list的索引+1，如果最深层遍历完，置0,上一层list+1
	 */
	public boolean doNextList(){
		for(int i = indexList.size() - 1 ; i >= 0; i--){
			int index = indexList.get(i);
			if(index >= 0){
				//最深一层list是否遍历完，如果没有,下标+1如果遍历完，置0，上一层list下标+1
				if(index < listSize.get(i) - 1){
					index ++;
					indexList.set(i, index);
					return true;
				}else{//如果index =  listSize.get(i) - 1，说明该层list已经遍历完，下标置0，上一层listxiabiao
					index = 0;
					indexList.set(i, index);
					int j = i - 1;
					//上一层list下标+1
					while(true){
						//如果没有上层list，退出程序
						if(j < 0)
							return false;
						int preIndex = indexList.get(j);
						if(preIndex >= 0){
							if(preIndex < listSize.get(j) - 1){
								preIndex ++;
								indexList.set(j, preIndex);
								return true;
							}else{
								preIndex = 0;
								indexList.set(j, preIndex);
								j--;
								continue;
							}
						}else{
							j--;
						}
					}
				}
			}else{
				continue;
			}
		}
		return false;
	}
}
