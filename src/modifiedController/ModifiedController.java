package modifiedController;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.StringTokenizer;

import java.io.BufferedReader;  
import java.io.FileReader;  
import java.io.IOException;  

public class ModifiedController {

	public static void main(String[] args) throws ClassNotFoundException, IOException {
		// TODO Auto-generated method stub
		System.out.println("hello");
		try {
			Connection con1 = DriverManager.getConnection("jdbc:mysql://localhost:3306/data5408", "root", "2100");
			System.out.println("Connected Local Database");
			/*
			 * Con1 suggest connection with Local Database.
			 * Con2 suggest connection with Remote Database.
			 * */			
			String url = "jdbc:mysql://34.122.78.27:3306/Data5408?cloudSqlInstance=csci-5408-w21-305213:us-central1:data-mwa-db";
		    Class.forName("com.mysql.cj.jdbc.Driver");
		    Connection con2 = DriverManager.getConnection(url, "root", "2100");
		    System.out.println("Connected Remote Database");
		    String[] S = new String[5];
		    
		    String line = "";
		    int l=0,r=0;
		    String[] Loc = new String[2];
		    String[] Rem = new String[7];
		    
		    /*
		     * Usage of Data Dictionary in the form of csv file which contains 
		     * information about tables in local and remote database.
		     * Buffered reader is being used to parse csv file.
		     * which reads each line in csv and then tokenize them.
		     * With the help of tokens the recieved response is stored in Loc or Rem accordingly.
		     * Now array of String Loc[] and Rem[] have table names which are there in the csv file.
		     * */
		    
		    BufferedReader br = new BufferedReader(new FileReader("F:\\Dalhousie\\CSCI 5408\\Assignment 2\\GDD.csv"));  
		    while ((line = br.readLine()) != null)   //returns a Boolean value  
		    {  
		    	String[] resp = line.split(",");
		    	
		    	if (resp[1].equals("Local"))
		    	{
		    		Loc[l] = resp[0];
		    	//	System.out.println("In Local " + Loc[l]);
		    		l++;	
		    	}
		    	if(resp[1].equals("Remote"))
		    	{
		    		Rem[r] = resp[0];
		    	//	System.out.println("In Remote " + Rem[r]);
		    		r++;
		    	}
		    }
		 
		    
		    S[0] = "UPDATE olist_customers_dataset1 SET customer_city = 'T1 City' WHERE customer_zip_code_prefix = '01008';";
		    S[1] = "UPDATE olist_geolocation_dataset SET geolocation_state = 'NS' WHERE geolocation_zip_code_prefix = 1037;";
		    S[2] = "UPDATE olist_products_dataset SET product_category_name = 'Perfumess' WHERE product_id = '1e9e8ef04dbcff4541ed26657ea517e5';";
		    S[3] = "DELETE from olist_sellers_dataset WHERE seller_id = 'c240c4061717ac1806ae6ee72be3533b';";
		    S[4] = "INSERT INTO product_category_name_translation VALUES ('XYZ','xyz');";
		    
		    String[] T = new String[5];
		    T[0] = "INSERT INTO olist_order_payments_dataset VALUES ('ABC',1,'credit',8,21.1)";
		    T[1] = "UPDATE olist_orders_dataset SET order_status = 'Invoice' Where order_id = 'a4591c265e18cb1dcee52889e2d8acc3'";
		    T[2] = "DELETE from olist_order_reviews_dataset where order_id = '705402bc1d956067338873d414158d09'";
		    
		    T[3] = "UPDATE olist_customers_dataset1 SET customer_city = 'abc' WHERE customer_zip_code_prefix = '01018';";
		    T[4] = "UPDATE olist_geolocation_dataset SET geolocation_state = 'NS' WHERE geolocation_zip_code_prefix = 1037;";
		    
		    /*
		     * As given in assignment task, 5 queries for each transaction are given which ultimately updates, delete, insert accordingly in local and remote Database.
		     * 
		     * In Array of Query, no where it is mentioned which database to be used, hereby code decides which database to be selected.
		     * */
		    
		    for(int j=0;j<S.length;j++) 
		    {
		    	int local = 0,select = 0;
		    	String[] tokens = S[j].split(" "); // String of Query is tokenized.
			    for (int i=0;i<tokens.length;i++)
			    {			
			        if(tokens[i].equals("olist_customers_dataset1") || tokens[i].equals("olist_geolocation_dataset"))
			        {
			        	System.out.println("Found");
			        	local=1;					//Parameter for Local been set.
			        	
			        }
			    }
		    System.out.println("Local Status: " + local);
		    
		    if(local == 1)
		    {
		    	PreparedStatement s1 = con1.prepareStatement(S[j]);
		    	s1.executeUpdate();
		    	System.out.println(S[j]);	
		    }
		    
		    else 
		    {
		    	PreparedStatement s2 = con2.prepareStatement(S[j]);
		    	s2.executeUpdate();
		    	System.out.println(S[j]);
		    }
		    local = 0;
		    
	    	String[] tok = T[j].split(" "); // String of Query is tokenized.
		    for (int i=0;i<tok.length;i++)
		    {			
		        if(tok[i].equals("olist_customers_dataset1") || tok[i].equals("olist_geolocation_dataset"))
		        {
		        	System.out.println("Found");
		        	local=1;					//Parameter for Local been set.
		        	
		        }
		    }
	    System.out.println("Local Status: " + local);
	    
	    if(local == 1)
	    {
	    	PreparedStatement s1 = con1.prepareStatement(T[j]);
	    	s1.executeUpdate();
	    	System.out.println(T[j]);	
	    }
	    
	    else 
	    {
	    	PreparedStatement s2 = con2.prepareStatement(T[j]);
	    	s2.executeUpdate();
	    	System.out.println(T[j]);
	    }

		    
		    }	
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

