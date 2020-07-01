package updateLogAndConfig;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;

import connectionDatabase.BaseConnection;

public class UpdateLog {
	public void updateLogWhenSuccess(int id,int numcol) {
		try {
			Connection con = BaseConnection.getMySQLConnection();
			String datetime = LocalDateTime.now().toString();
			String sql = "UPDATE `control_database`.`logtab` SET `status_file` = 'success_toLoadWarehouse', `time_load_staging` = '"+datetime+"', `numCol_have_load` = '"+numcol+"' WHERE (`idlogTab` = '"+id+"');";
			System.out.println(sql);
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
			String datetime = LocalDateTime.now().toString();
			String sql = "UPDATE `control_database`.`logtab` SET `status_file` = 'fail_toLoadStaging', `time_load_staging` = '"+datetime+"', `numCol_have_load` = '0' WHERE (`idlogTab` = '"+id+"');";
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
