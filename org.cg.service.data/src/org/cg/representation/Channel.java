package org.cg.representation;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

public class Channel {
	private int channelId;
	private int chunkNum;
	private boolean isOpen;
	//private HashMap<Integer,String> metaTable = new HashMap<Integer,String>();
	public Channel(int _channelId){
		this.channelId=_channelId;
	}
	public boolean isOpen(){
		return this.isOpen;
	}
	
	public void close(){
		this.isOpen=false;
	}
	
	public static Date getDate(long timestamp){
		DateFormat format = new SimpleDateFormat("MMddyyHHmmss");
		Date date = null;
		try {
			date = format.parse(String.valueOf(timestamp));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return date;
	}
	public static void main(String[] args){
		long timestamp = 1382317413569L;
		System.out.println(getDate(timestamp));
		timestamp+=(24*60*60*1000);
		System.out.println(getDate(timestamp));
		//System.out.println(days);
	}
}
