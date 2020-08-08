package updateLogAndConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;

import connectionDatabase.BaseConnection;

public class UpdateLog {
	public void updateLogWhenSuccess(int id,int numcol) {
		try {
			Connection con = BaseConnection.getMySQLConnection();
			String sql = "UPDATE `control_database`.`logtab` SET `status_file` = 'success_to_staging', `time_load_staging` = current_date(), `numCol_have_load` = '"+numcol+"' WHERE (`idlogTab` = '"+id+"');";
			PreparedStatement ps = con.prepareStatement(sql);
			ps.executeUpdate();
			con.close();
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
	}
	public void updateLogWhenFail(int id) {
		try {
			Connection con = BaseConnection.getMySQLConnection();
			String sql = "UPDATE `control_database`.`logtab` SET `status_file` = 'fail_to_staging', `time_load_staging` = current_date(), `numCol_have_load` = '0' WHERE (`idlogTab` = '"+id+"');";
			PreparedStatement ps = con.prepareStatement(sql);
			ps.executeUpdate();
			con.close();
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
	}
	public void updateLogWhenFailLoadWarehouse(int id) {
		try {
			Connection con = BaseConnection.getMySQLConnection();
			String sql = "UPDATE `control_database`.`logtab` SET `status_file` = 'fail_to_warehouse', `time_load_warehouse` = current_date(), `numCol_have_load` = '0' WHERE (`idlogTab` = '"+id+"');";
			PreparedStatement ps = con.prepareStatement(sql);
			ps.executeUpdate();
			con.close();
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
	}
	public void updateLogWhenSuccessLoadWarehouse(int id) {
		try {
			Connection con = BaseConnection.getMySQLConnection();
			String sql = "UPDATE `control_database`.`logtab` SET `status_file` = 'success_to_warehouse', `time_load_warehouse` = current_date(), `numCol_have_load` = '0' WHERE (`idlogTab` = '"+id+"');";
			PreparedStatement ps = con.prepareStatement(sql);
			ps.executeUpdate();
			con.close();
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
	}
	public static void main(String[] args) {
		String datetime = LocalDateTime.now().toString();
		System.out.println(datetime);
	}
}
