package com.hadooptraining.extra.recommender;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.XML;

public class AtomDataAsJson {
    private static final Logger logger = Logger.getLogger(AtomDataAsJson.class);
    public enum SASSLOCATION { DEV, PROD };
	private final String SASS_PROD_BASE = "http://sass.highwire.org";
	private final String SASS_DEV_BASE = "http://sass-dev.highwire.org";

	private String atomPath;
    private JSONObject jsonRepresentation;
	private StringBuffer xmlRepresentation = new StringBuffer(); 
    private JSONObject atomEntry;

	public AtomDataAsJson(String atompath, SASSLOCATION loc) {
		this.atomPath = atompath;
		
		URL extractorPath = null;
		try {
			switch (loc) {
				case DEV:
				extractorPath = new URL(SASS_DEV_BASE + this.atomPath);
				break;
				case PROD:
				extractorPath = new URL(SASS_PROD_BASE + this.atomPath);
				break;
			}
			BufferedReader in = new BufferedReader(new InputStreamReader(extractorPath.openStream()));
			String inputLine;
			
			while ((inputLine = in.readLine()) != null)
				xmlRepresentation.append(inputLine);
			
			in.close();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			jsonRepresentation =  XML.toJSONObject(xmlRepresentation.toString());
			atomEntry =  jsonRepresentation.getJSONObject("atom:entry");
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	public AtomDataAsJson(String atompath) {
		this(atompath, SASSLOCATION.PROD);
	}

	public String getParent() {
		try {
			JSONArray atomLinks = atomEntry.getJSONArray("atom:link");
			for (int i=0; i<atomLinks.length(); i++) {
				JSONObject link = atomLinks.getJSONObject(i);
				String rel = link.getString("rel");
				if (rel.equals("http://schema.highwire.org/Compound#parent")) {
					return link.getString("href");
				}
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return "";
	}
	
	public String toString() {
		return jsonRepresentation.toString();
	}
}
