/*Starting with Sequence 1, both read query of T1 and T2 got executed. 
 * Now, in sequence 2 we had an update operation from T1. 
 * Execute query was done but it was not yet committed as T1 transaction gets commit in sequence 4. 
 * Now, when any other operation is executed on the given table occupied by T1, it throws exception as 
 * table has got locked by T1 and won’t allow any other operation.
 * Now, the thing is, the query in other transaction will wait for T1 to commit, but here T1 doesn’t commit 
 * so, it goes into indefinite wait and gets abort and halts the controller by throwing exception.
*/

package Controller;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ControllerEmbeddTrans {

	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub
		
		Connection con1, con2, con3;
		try {
			con1 = DriverManager.getConnection("jdbc:mysql://localhost:3306/data5408", "root", "2100");
			System.out.println("Connected 1");

			con2 = DriverManager.getConnection("jdbc:mysql://localhost:3306/data5408", "root", "2100");
			System.out.println("Connected 2");

			con3 = DriverManager.getConnection("jdbc:mysql://localhost:3306/data5408", "root", "2100");
			System.out.println("Connected 3");

			con1.setAutoCommit(false);
			con2.setAutoCommit(false);
			con3.setAutoCommit(false);

			String[][] T = new String[4][6];
			
			/*2D array has been used to store the query which will get executed on the time of their sequence execution step.
			 * T[Transaction][sequence] form is considered
			 * */

			T[1][1] = "SELECT * FROM olist_customers_dataset1 WHERE customer_zip_code_prefix = '01008';";
			T[1][2] = "UPDATE olist_customers_dataset1 SET customer_city = 'T1 City' WHERE customer_zip_code_prefix = '01008';";
			T[1][3] = null;
			T[1][4] = "Commit";
			T[1][5] = null;

			T[2][1] = "SELECT * FROM olist_customers_dataset1 WHERE customer_zip_code_prefix = '01008';";
			T[2][2] = null;
			T[2][3] = "UPDATE olist_customers_dataset1 SET customer_city = 'T1 City' WHERE customer_zip_code_prefix = '01008';";
			T[2][4] = null;
			T[2][5] = "Commit";

			T[3][1] = null;
			T[3][2] = "SELECT * FROM olist_customers_dataset1 WHERE customer_zip_code_prefix = '01008';";
			T[3][3] = "UPDATE olist_customers_dataset1 SET customer_city = 'T1 City' WHERE customer_zip_code_prefix = '01008';";
			T[3][4] = "Commit";
			T[3][5] = null;

			int seq = 1,l=1; // Sequence wise transaction is done for 3 transaction
			
			if( seq == 1 ) {
					System.out.println("In seq 1:");
				PreparedStatement s1 = con1.prepareStatement(T[1][1]);
					
				ResultSet rs = s1.executeQuery();
				while (rs.next()) {
					System.out.println("T1 " + rs.getString(1) + " " + rs.getString(2) + " " + rs.getString(3) + " "
							+ rs.getString(4) + " " + rs.getString(5));
				}System.out.println("Done 1");
			

				if(l==1) {
				PreparedStatement s2 = con2.prepareStatement(T[2][1]);
				System.out.println("2nd read");
				ResultSet rs2 = s2.executeQuery();
				while (rs2.next()) {
					System.out.println("T2 " + rs2.getString(1) + " " + rs2.getString(2) + " " + rs2.getString(3) + " "
							+ rs2.getString(4) + " " + rs2.getString(5));
				}}

				seq++;
			}

			if (seq == 2) {
				PreparedStatement s3 = con1.prepareStatement(T[1][2]);
				s3.executeUpdate();
				System.out.println("Update done in T1 seq 2");
								
				PreparedStatement s4 = con3.prepareStatement(T[3][2]);

				ResultSet rs = s4.executeQuery();
				if(!rs.next()) {
					System.out.println("T2 " + rs.getString(1) + " " + rs.getString(2) + " " + rs.getString(3) + " "
							+ rs.getString(4) + " " + rs.getString(5));
				}
				seq++;
			}

			if (seq == 3) {
				PreparedStatement s5 = con2.prepareStatement(T[2][3]);
				s5.executeUpdate();

				PreparedStatement s6 = con3.prepareStatement(T[3][3]);
				s6.executeUpdate();

				seq++;
			}

			if (seq == 4) {
				if (T[1][4].equals("Commit")) {
					con1.commit();
				}
				if (T[3][4].equals("Commit")) {
					con3.commit();
				}
				seq++;
			}

			if (seq == 5) {
				if (T[2][5].equals("Commit")) {
					con2.commit();
				}
			}

			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}



	