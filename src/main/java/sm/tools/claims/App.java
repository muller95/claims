package sm.tools.claims;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import sm.tools.veda_client.Individual;
import sm.tools.veda_client.Resource;
import sm.tools.veda_client.Resources;
import sm.tools.veda_client.Type;
import sm.tools.veda_client.VedaConnection;

/**
 * Hello world!
 *
 */
public class App 
{
	public static String getActualDocument(String baUri, Connection docDbConn) throws SQLException {
		String queryStr = "SELECT content, recordid, timestamp FROM objects WHERE objectId = ? AND actual = 1 AND isDraft = 0";

		PreparedStatement ps = docDbConn.prepareStatement(queryStr);
		ps.setString(1, baUri);
		// ps.setLong(2, timestamp);
		ResultSet rs = ps.executeQuery();
		String res = "";
		if (rs.next()) {
			res = rs.getString(1);
			String recordid = rs.getString(2);
			recordid.length();
		/*	doc = (XmlDocument) XmlUtil.unmarshall(xml);
			res = new Pair<XmlDocument, Long>(doc, rs.getLong(3));*/
			
		} 
		
		rs.close();
		ps.close();
		return res;
	}
    public static void main( String[] args )
    {
        try {
        	Class.forName("com.mysql.jdbc.Driver");
        	HashMap<String, String> config = new HashMap<String, String>();
        	
        	BufferedReader br = new BufferedReader(new FileReader("config.conf"));
    		int count = 1;
    	    for(String line; (line = br.readLine()) != null; count++) {
    	    	int idx = line.indexOf("#");
    	    	if (idx >= 0) {
    	    		line = line.substring(0, idx);
    	    	}
    	    
    	    	line = line.trim();
    	    	if (line.length() == 0)
    	    		continue;
    	    	
    	        idx = line.indexOf("=");
    	        if (idx < 0) {
    	        	System.err.println(String.format("ERR! Invalid line %d, "
    	        			+ "'=' was not found: %s", count, line));
    	        	return;
    	        }
    	        

    	        String paramName = line.substring(0, idx);
    	        String paramVal = line.substring(idx + 1);
    	        
    	        config.put(paramName, paramVal);
    	    }
    	    
    	    String veda_url;

       		if (!config.containsKey("veda")) {
       			System.err.println("ERR! Config key 'veda' is not set");
       			return;
       		}
       		veda_url = config.get("veda");
        	
       		VedaConnection veda = null;
        	
        	veda = new VedaConnection(veda_url, "ImportDMSToVeda",
        		"a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3");
        	
        	Connection docDbConn;
        	String docDbUser, docDbPassword, docDbUrl;

       		docDbUser = config.get("doc_db_user");
       		if (!config.containsKey("doc_db_user")) {
       			System.err.println("ERR! Config key 'doc_db_user' is not set");
       			return;
       		}
        	
        	if (!config.containsKey("doc_db_password")) {
        		System.err.println("ERR! Config key 'doc_db_password' is not set");
        		return;
        	}
        	docDbPassword = config.get("doc_db_password");

        	
        	if (!config.containsKey("doc_db_url")) {
    			System.err.println("ERR! Config key 'doc_db_url' is not set");
    			return;
    		}
        	docDbUrl = config.get("doc_db_url");
        	docDbConn = DriverManager.getConnection("jdbc:mysql://" + docDbUrl, docDbUser, docDbPassword);
        	
        	
        	int maxUris = Integer.MAX_VALUE;
        	if (config.containsKey("max_uris")) {
        		maxUris = Integer.parseInt(config.get("max_uris"));
        	}
        	
        	String[] uris = veda.query("( 'rdf:type'==='mnd-s:Claim' ) && "
        			+ "( 'v-s:sender.rdf:type'=='mnd-s:Correspondent' ) && "
        			+ "( 'v-s:sender.v-s:correspondentOrganization'=='d:org_RU1121003135' ) && "
        			+ "( 'v-s:recipient.rdf:type'=='mnd-s:Correspondent' ) && "
        			+ "( 'v-s:recipient.v-s:correspondentOrganization'=='d:org_RU1121003135' )");
        	
        	XPathFactory factory =  XPathFactory.newInstance();
    		XPath xpath = factory.newXPath();
    		XPathExpression expr = null;
    		Object result = null;
    		NodeList list = null;
        	for (int i = 0; i < uris.length && i < maxUris; i++) {
        		String organization = null, organization2 = null;
        		String baUri = uris[i].substring(2);	
        		System.out.println(baUri);
    			String xml = getActualDocument(baUri, docDbConn);
    			   			
    			DocumentBuilder docBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        		Document doc = null;
        		try {
        			doc = docBuilder.parse(new ByteArrayInputStream(xml.getBytes()));
        		} catch (Exception e) {
        			e.printStackTrace();
        			continue;
        		}
        			
        		expr = xpath.compile("//xmlDocument/xmlAttributes/xmlAttribute/code[text()='customer']");
        		result = expr.evaluate(doc, XPathConstants.NODESET);
        		list = (NodeList)result;
        		if (list.getLength() > 0) {
        			list = list.item(0).getParentNode().getChildNodes();
        			
        			for (int j= 0; j < list.getLength(); j++) {
        				if (list.item(j).getNodeName().equals("linkValue"))
        					organization = list.item(j).getTextContent();
        			}
        		}
        		
        		expr = xpath.compile("//xmlDocument/xmlAttributes/xmlAttribute/code[text()='executor']");
        		result = expr.evaluate(doc, XPathConstants.NODESET);
        		list = (NodeList)result;
        		if (list.getLength() > 0) {
        			list = list.item(0).getParentNode().getChildNodes();
        			
        			for (int j= 0; j < list.getLength(); j++) {
        				if (list.item(j).getNodeName().equals("linkValue"))
        					organization2 = list.item(j).getTextContent();
        			}
        		}
        		
        		expr = xpath.compile("//xmlDocument/xmlAttributes/xmlAttribute/code[text()='contacts']");
        		result = expr.evaluate(doc, XPathConstants.NODESET);
        		list = (NodeList)result;
        		ArrayList<String> contacts = new ArrayList<String>();
        		if (list.getLength() > 0) {
        			for (int j = 0; j < list.getLength(); j++) {
	        			NodeList list2 = list.item(j).getParentNode().getChildNodes();
	        			for (int k = 0; k < list2.getLength(); k++) {
	        				if (list2.item(k).getNodeName().equals("textValue"))
	        					contacts.add(list2.item(k).getTextContent());
	        			}
        			}
        		}
        		
        		expr = xpath.compile("//xmlDocument/xmlAttributes/xmlAttribute/code[text()='contacts2']");
        		result = expr.evaluate(doc, XPathConstants.NODESET);
        		list = (NodeList)result;
        		ArrayList<String> contacts2 = new ArrayList<String>();
        		if (list.getLength() > 0) {
        			for (int j = 0; j < list.getLength(); j++) {
	        			NodeList list2 = list.item(j).getParentNode().getChildNodes();
	        			for (int k = 0; k < list2.getLength(); k++) {
	        				if (list2.item(k).getNodeName().equals("organizationValue"))
	        					contacts2.add(list2.item(k).getTextContent());
	        			}
        			}
        		}
        		
        		expr = xpath.compile("//xmlDocument/xmlAttributes/xmlAttribute/code[text()='department']");
        		result = expr.evaluate(doc, XPathConstants.NODESET);
        		list = (NodeList)result;
        		ArrayList<String> department = new ArrayList<String>();
        		if (list.getLength() > 0) {
        			for (int j = 0; j < list.getLength(); j++) {
	        			NodeList list2 = list.item(j).getParentNode().getChildNodes();
	        			for (int k = 0; k < list2.getLength(); k++) {
	        				if (list2.item(k).getNodeName().equals("organizationValue"))
	        					department.add(list2.item(k).getTextContent());
	        			}
        			}
        		} 
        		
        		Individual indiv = veda.getIndividual(uris[i]);
        		if (organization != null) {
        			String corrOrgUri = indiv.getResources("v-s:sender").resources.get(0).getData();
        			Individual correspondent_organization = new Individual();
        			correspondent_organization.setUri(indiv.getUri() + "_correspondent_organization_sender");
        			if (organization.equals("9076b375bf4a468289dbd0c2db886256")) {
        				correspondent_organization.addProperty("v-s:correspondentOrganization", new Resource("d:org_RU1121003135", Type._Uri));
        				for (String r : department) 
        					correspondent_organization.addProperty("v-s:correspondentDepartment", 
        							new Resource(r, Type._Uri));
        				
        				for (String r : contacts2)
        					correspondent_organization.addProperty("v-s:correspondentPerson", 
        							new Resource(r, Type._Uri));
        			} else {
        				Individual org_indiv = veda.getIndividual("d:" + organization);
        				if (org_indiv != null)
        					correspondent_organization.addProperty("v-s:correspondentOrganization", org_indiv.getResources("v-s:backwardTarget"));
        				
        				for (String r : contacts)
        					correspondent_organization.addProperty("v-s:correspondentPersonDescription", 
        						new Resource(r, Type._String));
        			}
        			
        			correspondent_organization.addProperty("rdf:type", new Resource("mnd-s:Correspondent", Type._Uri));
        			correspondent_organization.addProperty("v-s:parent", 
        				new Resource(indiv.getUri(), Type._Uri));
        			correspondent_organization.addProperty("v-s:creator", 
        				indiv.getResources("v-s:creator"));
        			correspondent_organization.addProperty("v-s:created", 
        				indiv.getResources("v-s:created"));
        			
        			indiv.setProperty("v-s:sender", new Resource(correspondent_organization.getUri(), Type._Uri));
        			veda.putIndividual(correspondent_organization, true, 0);
        		}
        		
        		if (organization2 != null) {
        			Individual correspondent_organization = new Individual();
        			correspondent_organization.setUri(indiv.getUri() + "_correspondent_organization_recipient");
        			if (!organization.equals("9076b375bf4a468289dbd0c2db886256")) {
        				correspondent_organization.addProperty("v-s:correspondentOrganization", new Resource("d:org_RU1121003135", Type._Uri));
        				for (String r : department) 
        					correspondent_organization.addProperty("v-s:correspondentDepartment", new Resource(r, Type._Uri));
        				
        				for (String r : contacts2)
        					correspondent_organization.addProperty("v-s:correspondentPerson", new Resource(r, Type._Uri));
        			} else {
        				Individual org_indiv = veda.getIndividual("d:" + organization2);
        				if (org_indiv != null)
        					correspondent_organization.addProperty("v-s:correspondentOrganization", org_indiv.getResources("v-s:backwardTarget"));
        				
        				for (String r : contacts)
        					correspondent_organization.addProperty("v-s:correspondentPersonDescription", new Resource(r, Type._String));
        			}
        			
        			correspondent_organization.addProperty("rdf:type", new Resource("mnd-s:Correspondent", Type._Uri));
        			correspondent_organization.addProperty("v-s:parent", 
        				new Resource(indiv.getUri(), Type._Uri));
        			correspondent_organization.addProperty("v-s:creator", 
        				indiv.getResources("v-s:creator"));
        			correspondent_organization.addProperty("v-s:created", 
        				indiv.getResources("v-s:created"));
        			
        			indiv.addProperty("v-s:recipient", new Resource(correspondent_organization.getUri(), Type._Uri));
        			veda.putIndividual(correspondent_organization, true, 0);
        		}
        		
        		int a = 2 + 2;
        		a = 2 + a;
        	}
        } catch (Exception e) {
        	e.printStackTrace();
        }
    }
}
