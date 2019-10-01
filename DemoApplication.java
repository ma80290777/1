package net.codejava.demo;

import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.io.File;
import java.io.FileNotFoundException;
import java.time.Instant;

import org.influxdb.*;
import org.influxdb.annotation.Column;
import org.influxdb.annotation.Measurement;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;


@Controller
@SpringBootApplication


public class DemoApplication {
	
	public String mean1;
	public String mean2;
	public String dbName = "AirQuality";
	

	@Measurement(name = "result")
	public class Cpu {
	    @Column(name = "time")
	    private Instant time;
	    @Column(name = "mean", tag = true)
	    private Double mean;
	    // getters (and setters if you need)
	}

	
	@RequestMapping("load")
	@ResponseBody
	
	public void load() throws FileNotFoundException, ParseException {

		InfluxDB influxDB = InfluxDBFactory.connect("http://localhost:8086", "root", "root");
		
		influxDB.query(new Query("CREATE DATABASE " + dbName));
		String rpName = "aRetentionPolicy";
		influxDB.query(new Query("CREATE RETENTION POLICY " + rpName + " ON " + dbName + " DURATION 30h REPLICATION 2 DEFAULT"));
		
		boolean dataCount = false;
		
		File csv = new File("C:/GSPIA/Database/AirQualityUCI/AirQualityUCI.csv");
		Scanner scanner = new Scanner (csv);
		
		String line;
		String[] array = new String[15];

		
		while(scanner.hasNextLine())
		{
			
			line = scanner.nextLine();
			if(line.substring(0, 1).equals(";"))
			{
				break;
			}
			
			
			if(dataCount == true)
			{				
				array = line.split(";");
				String tempDate = array[0];
				String[] temp = tempDate.split("/");
				String newDate = temp[0] + "-" + temp[1] + "-" + temp[2];
				array[0] = newDate;
				
				String tempNum1 = array[1];
				array[1] = tempNum1.replace('.', ':');
				
				String dateAndTime = newDate + " " + array[1];
				
				
				for(int i = 2; i <= 14; i ++)
				{
					String tempNum = array[i];
					array[i] = tempNum.replace(',', '.');
				}
				
				SimpleDateFormat sdf = new SimpleDateFormat("dd-M-yyyy hh:mm:ss");
				Date date = sdf.parse(dateAndTime);
				long time = date.getTime();
				//long time = System.currentTimeMillis();
				
				Point point = Point.measurement("result")
					    .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
					    .addField("DateAndTime", time)
					    .addField("CO(GT)", Double.parseDouble(array[2]))
					    .addField("PT08.S1(CO)", Double.parseDouble(array[3]))
					    .addField("NMHC(GT)", Double.parseDouble(array[4]))
					    .addField("C6H6(GT)", Double.parseDouble(array[5]))
					    .addField("PT08.S2(NMHC)", Double.parseDouble(array[6]))
					    .addField("NOx(GT)", Double.parseDouble(array[7]))
					    .addField("PT08.S3(NOx)", Double.parseDouble(array[8]))
					    .addField("NO2(GT)", Double.parseDouble(array[9]))
					    .addField("PT08.S4(NO2)", Double.parseDouble(array[10]))
					    .addField("PT08.S5(O3)", Double.parseDouble(array[11]))
					    .addField("T", Double.parseDouble(array[12]))
					    .addField("RH", Double.parseDouble(array[13]))
					    .addField("AH", Double.parseDouble(array[14]))
					    .addField("YEAR", Integer.parseInt(temp[2]))
					    .addField("MONTH", Integer.parseInt(temp[1]))
					    .addField("DAY", Integer.parseInt(temp[0]))
					    .build();
				
				influxDB.write(dbName, rpName, point);
				

			}
			else
			{
				dataCount = true;
			}
		}
		
		scanner.close();		
	}
	
	
	@RequestMapping(value="/loadcsv", method=RequestMethod.GET)
	public String load(Model model)
	{
		return "Loadcsv";
	}
	
	
	@RequestMapping(value="/mean", method=RequestMethod.GET)
	public String returnMean(Model model)
	{
		InfluxDB influxDB = InfluxDBFactory.connect("http://localhost:8086", "root", "root");
		QueryResult queryResult = influxDB.query(new Query("SELECT mean(\"CO(GT)\") FROM " + "result" + " WHERE YEAR = 2004", dbName));
		mean1 = queryResult.getResults().get(0).getSeries().get(0).getValues().get(0).get(1).toString();
		model.addAttribute("mean1", mean1);
		
		queryResult = influxDB.query(new Query("SELECT mean(\"NO2(GT)\") FROM " + "result" + " WHERE YEAR = 2004", dbName));
		mean2 = queryResult.getResults().get(0).getSeries().get(0).getValues().get(0).get(1).toString();
		model.addAttribute("mean2", mean2);
		
		return "MeanTable";
	}
	
	
	public static void main(String[] args) throws FileNotFoundException 
	{
		SpringApplication.run(DemoApplication.class, args);
	}

}
