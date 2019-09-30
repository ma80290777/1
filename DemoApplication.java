package net.codejava.demo;

import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.io.File;
import java.io.FileNotFoundException;
import java.time.Instant;

import org.influxdb.*;
import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.annotation.Column;
import org.influxdb.annotation.Measurement;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

@Controller
@SpringBootApplication


public class DemoApplication {
	
	@RequestMapping("load")
	@ResponseBody
	
	public void load() throws FileNotFoundException {

		InfluxDB influxDB = InfluxDBFactory.connect("http://localhost:8086", "root", "root");
		
		String dbName = "test";
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
			if(dataCount == true)
			{
				line = scanner.nextLine();
				array = line.split(";");
				String tempDate = array[0];
				String[] temp = tempDate.split("/");
				String newDate = temp[2] + "/" + temp[0] + "/" + temp[1];
				array[0] = newDate;
				
				String tempNum1 = array[1];
				array[1] = tempNum1.replace('.', ':');
				
				for(int i = 2; i <= 14; i ++)
				{
					String tempNum = array[i];
					array[i] = tempNum.replace(',', '.');
				}
				
				
				// tring to figure out the date format, not complete, but does not influence the result here
				String tempTime = array[1] + " " + array[1];
				//Date date = formatter.parse(tempTime);
				//long dateInLong = tempTime.getTime();
				
				Point point = Point.measurement("cpu")
					    .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
					    .addField("CO(GT)", Double.parseDouble(array[2]))
					    .addField("PT08.S1(CO)", Double.parseDouble(array[3]))
					    .addField("NMHC(GT)", Double.parseDouble(array[4]))
					    .addField("C6H6(GT)", Double.parseDouble(array[5]))
					    .addField("PT08.S2(NMHC)=", Double.parseDouble(array[6]))
					    .addField("NOx(GT)=", Double.parseDouble(array[7]))
					    .addField("PT08.S3(NOx)", Double.parseDouble(array[8]))
					    .addField("NO2(GT)", Double.parseDouble(array[9]))
					    .addField("PT08.S4(NO2)", Double.parseDouble(array[10]))
					    .addField("PT08.S5(O3)", Double.parseDouble(array[11]))
					    .addField("T", Double.parseDouble(array[12]))
					    .addField("RH", Double.parseDouble(array[13]))
					    .addField("AH", Double.parseDouble(array[14]))
					    .build();
				
				influxDB.write(dbName, rpName, point);
				
				influxDB.query(new Query("SELECT * FROM cpu")); 
			}
			else
			{
				scanner.nextLine();
				dataCount = true;
			}
			
		}
		
		scanner.close();
		
		
	}
	
	private long time(Object object) {
		// TODO Auto-generated method stub
		return 0;
	}

	public static void main(String[] args) throws FileNotFoundException 
	{
		SpringApplication.run(DemoApplication.class, args);
	}

}
