package com.newland.crm.cmc.daemon.file;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBException;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;

import com.newland.cbf.frame.task.TraceMessage;
import com.newland.cbf.frame.task.execute.ModuleCall;
import com.newland.cbf.frame.task.execute.ModuleCallMessage;
import com.newland.crm.cmc.bean.entity.file.CmcXmlFileTemplateBean;
import com.newland.crm.cmc.common.define.GlobalDefs;
import com.newland.crm.cmc.dao.flow.CmcXmlFileTemplateDao;
import com.newland.crm.cmc.dao.flow.ParseFileDao;
import com.newland.csf.common.config.manager.FileConfigManager;
import com.newland.csf.common.log.LogObj;
import com.newland.csf.common.log.LogObjFactory;
import com.newland.csf.frame.util.MtcThreadLocalMgr;
import com.newland.csf.frame.util.Util;
import com.newland.csf.frame.util.XmlUtils;

/**
 *
 * @author lirong 2018.08.23 描述:解析xml格式文件、入库
 */

public class XmlFileHandler implements ModuleCall {
	private static LogObj logCache = LogObjFactory.getLogObj(XmlFileHandler.class);
	private Map<Integer, Integer> listSize = new HashMap<Integer, Integer>();
	private Map<String, Object> smcFileOperation = new HashMap<String, Object>();
	private String file_name;
	private String file_template_id;
	private ParseFileDao parseFileDao = new ParseFileDao();
	private CmcXmlFileTemplateDao cmcXmlFileTemplate = new CmcXmlFileTemplateDao();
	private List<Object> indexList = new ArrayList<Object>();
	ThreadLocal<Boolean> doAdd = new ThreadLocal<Boolean>();

	@Override
	public void call(List<ModuleCallMessage> list, TraceMessage tracemessage) {
		logCache.debug(GlobalDefs.PLUGIN_LOG_KEY, "XmlFileHandler call begin....");
		init(list);
		try {
			pluginImpl();
		} catch (Exception e) {
			logCache.debug(GlobalDefs.PLUGIN_LOG_KEY, "文件解析失败！");
			logCache.error(GlobalDefs.PLUGIN_LOG_KEY, Util.getStackTraceMsg(e));
			list.get(0).setResult_code("1");
		} catch (Error e2) {
			logCache.debug(GlobalDefs.PLUGIN_LOG_KEY, "文件解析失败！");
			logCache.error(GlobalDefs.PLUGIN_LOG_KEY, "error:" + Util.getStackTraceMsg(e2));
			list.get(0).setResult_code("1");
		}

		finish();
	}

	@SuppressWarnings("unchecked")
	private void init(List<ModuleCallMessage> pluginOrder) {
		smcFileOperation = (Map<String, Object>) pluginOrder.get(0).getInput_message();
	}

	protected void pluginImpl() throws IOException, JAXBException, Exception {
		logCache.debug(GlobalDefs.PLUGIN_LOG_KEY, "XmlFileHandler pluginImpl() begin....");
		Integer file_template_id = Integer.valueOf((String) smcFileOperation.get("file_template_id"));
		isConfigurationSmcFileTemplate((String) smcFileOperation.get("file_template_id"));

		String file_path = (String) smcFileOperation.get("romote_dir");
		file_name = (String) smcFileOperation.get("file_name");
		setFile_template_id((String) smcFileOperation.get("file_template_id"));
		file_path = file_path.endsWith("/") ? file_path
				: (new StringBuilder()).append(file_path).append("/").toString();
		logCache.debug(GlobalDefs.PLUGIN_LOG_KEY,
				(new StringBuilder()).append("remote_dir=").append(file_path).append(",file_name=").append(file_name)
						.append("  file_template_id = ").append(file_template_id).toString());
		String fullPath = (new StringBuilder()).append(file_path).append(file_name).toString();

		CmcXmlFileTemplateBean read_file_cfg = getTransBean(file_template_id);
		String sql_id = ParseFileDao.RULE_SQL_RELA.get(file_template_id);
		logCache.debug(GlobalDefs.PLUGIN_LOG_KEY, "sql_id:" + sql_id);

		Map<String, String> rule_map = elementRule2Map(read_file_cfg.getElement_rule());
		logCache.debug(GlobalDefs.PLUGIN_LOG_KEY, "XmlFileHandler elementRule2Map()获取xml格式配置规则存入map中 :  " + rule_map);
		String xmlContent = readFile4xml(fullPath, rule_map);
		logCache.debug(GlobalDefs.PLUGIN_LOG_KEY, "XmlFileHandler readFile4xml调用返回文件内容 xmlContent :  " + xmlContent);
		getParaMaps(rule_map, xmlContent, sql_id);

	}

	/**
	 * remark：file_template_id 必须在smc.smc_file_template表中配置预占 避免模板id相同,导致取工单表冲突.
	 *
	 * @throws Exception
	 */
	private void isConfigurationSmcFileTemplate(String file_template_id) throws Exception {
		if (FileConfigManager.getTemplate(file_template_id) == null)
			throw new Exception("未在smc.smc_file_template表中配置预占...");
	}

	private CmcXmlFileTemplateBean getTransBean(Integer file_template_id) {
		CmcXmlFileTemplateBean readFileCfgBean = cmcXmlFileTemplate.getCmcXmlFileTemplate(file_template_id);
		if (readFileCfgBean == null)
			throw new RuntimeException("read cmc_xml_file_template table config not found exception");
		return readFileCfgBean;
	}

	private Map<String, String> elementRule2Map(String element_rule) throws DocumentException {
		String[] rules = element_rule.split(";");
		HashMap<String, String> rule_map = new HashMap<String, String>();
		// 获得xml格式配置规则存入map中
		for (String rule : rules) {
			String key = rule.split(":")[0];
			String value = rule.split(":")[1];
			rule_map.put(key, value);
		}
		return rule_map;
	}

	protected String readFile4xml(String full_path, Map<String, String> ruleMap) throws IOException, DocumentException {
		logCache.debug(GlobalDefs.PLUGIN_LOG_KEY, "XmlFileHandler readFile4xml begin....");
		File file = new File(full_path);
		FileInputStream fis = new FileInputStream(file);
		BufferedReader in = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
		StringBuffer sb = new StringBuffer();
		String tmp = in.readLine();

		while (tmp != null && !" ".equals(tmp)) {
			sb.append(tmp);
			tmp = in.readLine();
		}
		in.close();
		if (sb != null && sb.length() > 0) {
			return sb.toString().trim();
		} else {
			return null;
		}
	}

	// 除根节点外,取值最多层数
	private void getParaMaps(Map<String, String> rule_map, String xml, String sql_id) throws DocumentException {
		int maxDep = 0;// 记录最深一层次数
		String[] main_element = rule_map.get("mainElement").split(",");
		logCache.debug(GlobalDefs.PLUGIN_LOG_KEY, "文件解析主要节点main_element : " + Arrays.toString(main_element));
		Map<String, Boolean> paraMap = new HashMap<String, Boolean>();
		Document doc = XmlUtils.parseText(xml);
		Element rootE = doc.getRootElement();
		boolean isCDATA = Boolean.parseBoolean(rule_map.get("isCDATA"));
		logCache.debug(GlobalDefs.PLUGIN_LOG_KEY, "XmlFileHandler getParaMaps()是否XML节点是否在CDATA取：" + isCDATA);
		for (String para : main_element) {
			paraMap.put(para, true);
			if (para.split("/").length - 2 > maxDep) {
				maxDep = para.split("/").length - 2;
			}
		}
		if (isCDATA) {
			String[] cdata_path = rule_map.get("CDATAPath").split("/");
			// 获得CDATA的上一级元素
			for (int i = 1; i < cdata_path.length; i++) {
				rootE = rootE.element(cdata_path[i]);
			}
			xml = rootE.getTextTrim();
		}

		doc = XmlUtils.parseText(xml);
		rootE = doc.getRootElement();
		indexList = Arrays.asList(Collections.nCopies(maxDep, -1).toArray());

		do {
			doAdd.set(true);
			Map<String, String> resultMap = new HashMap<String, String>();
			for (String nodeName : main_element) {
				String[] nodeNames = nodeName.split("/");
				repeatScanNode(rootE, nodeNames, resultMap, 0);
			}
			if(doAdd.get()){
				resultMap = externPara(resultMap);
				// 入库
				parseFileDao.addRecord(resultMap, sql_id);
				MtcThreadLocalMgr.getSession().commitConnWithoutClose();
			}
		} while (doNextList());
	}

	private void repeatScanNode(Element parentElement, String[] nodeNames, Map<String, String> resultMap, int i) {
		String nodeName = nodeNames[i + 2];
		if (parentElement.element(nodeName) == null) {
			return;
		}
		@SuppressWarnings("unchecked")
		List<Element> nextElements = parentElement.elements(nodeName);
		Element nextElement = null;

		if (nextElements.size() > 1  || listSize.containsKey(i)) {
			if (!listSize.containsKey(i)) {
				nextElement = nextElements.get(0);
				listSize.put(i, nextElements.size());
				indexList.set(i, 0);
			} else {
				if(nextElements.size() <= (Integer)indexList.get(i)){
					doAdd.set(false);
					return ;
				}
				nextElement = nextElements.get((Integer) indexList.get(i));
			}
			repeatScanNode(nextElement, nodeNames, resultMap, ++i);
		} else if (parentElement.element(nodeName).isTextOnly()) {// 叶子节点
			resultMap.put(parentElement.element(nodeName).getPath(), nextElements.get(0).getText());
		} else {// 非list的中间节点
			nextElement = parentElement.element(nodeName);
			repeatScanNode(nextElement, nodeNames, resultMap, ++i);
		}
	}
	private boolean doNextList() {
		// 取最底层list的个数
		int i = indexList.size() - 1;

		while (i >= 0) {
			int index = (int) indexList.get(i);// 上一次去list的下标
			if (index >= 0) {
				// 最深一层list是否遍历完，如果没有,下标+1如果遍历完置0,上一层list下标+1
				if (index < listSize.get(i) - 1) {
					index++;
					indexList.set(i, index);
					return true;
				} else {
					index = 0;
					indexList.set(i, index);
					i--;
				}
			} else {
				i--;
			}
		}
		return false;
	}

	protected Map<String, String> externPara(Map<String, String> externPara) {
		return externPara;
	}

	private void finish() {
		logCache.debug(GlobalDefs.PLUGIN_LOG_KEY, "XmlFileHandler call end ...");
	}

	public Map<String, Object> getSmcFileOperation() {
		return smcFileOperation;
	}

	public void setSmcFileOperation(Map<String, Object> smcFileOperation) {
		this.smcFileOperation = smcFileOperation;
	}

	public String getFile_name() {
		return file_name;
	}

	public void setFile_name(String file_name) {
		this.file_name = file_name;
	}

	public String getFile_template_id() {
		return file_template_id;
	}

	public void setFile_template_id(String file_template_id) {
		this.file_template_id = file_template_id;
	}

}
