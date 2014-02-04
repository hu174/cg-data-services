package org.cg.representation;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * 
 * @author yingkaihu
 *
 */
public class ResponseTuple {
	private ArrayList<String> response;
	
	public ResponseTuple(){
		response = new ArrayList<String>();
	}
	
	public void add(String value){
		response.add(value);
	}
	
	public Iterator<String> createIterator(){
		return response.iterator();
	}

	public ArrayList<String> getResponse() {
		return response;
	}
	
	
}
