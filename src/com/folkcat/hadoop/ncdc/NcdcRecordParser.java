package com.folkcat.hadoop.ncdc;

import org.apache.hadoop.io.Text;



public class NcdcRecordParser {
	private static final int MISSINT_TEMPERATURE=9999;
	private String mYear;
	private int mAirTemperature;
	private String mAirQuality;
	
	public void parse(String record){
		mYear=record.substring(15, 19);
		String airTemperatureStr;
		//Remove leading plus sign as parseInt dosen't like them
		if(record.charAt(87)=='+'){
			airTemperatureStr=record.substring(88,92);
		}else{
			airTemperatureStr=record.substring(87,92);
		}
		mAirTemperature=Integer.parseInt(airTemperatureStr);
		mAirQuality=record.substring(92,93);

		
	}

	public void parse(Text record) {
		parse(record.toString());
	}
	public boolean isValidTemperature(){
			return mAirTemperature!=MISSINT_TEMPERATURE&&mAirQuality.matches("[01459]");
	}
	public String getYear(){
		return mYear;
	}
	public int getAirTemperature(){
		return mAirTemperature;
	}
	public String getAirQuality(){
		return mAirQuality;
	}
	
	
	public static void main(String[] args){
		String record="0149010010999991990010218004+70933-008667FM-12+0009ENJA V0201701N00311000301CN0005001N9-00021-00031100921ADDAA199003091AG10000AY161061AY251061GA1091+999999999GF109991091999999999999999KA1120M+00221MD1610091+9999MW1601OA149901131REMSYN017333   10022 91122";
		NcdcRecordParser parser=new NcdcRecordParser();
		parser.parse(record);
		int airTemp=parser.getAirTemperature();
		String quality=parser.getAirQuality();
		String year=parser.getYear();
		if(parser.isValidTemperature()){
			System.out.println("结果    Year:"+year+"   airTemperature:"+airTemp+"  quality:"+quality);
		}else{
			System.out.println("错");
		}
		
	}
	

}
