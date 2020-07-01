package preprocessingInStaging;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Preprocessing {
	public void replaceMissingValue() {
		String jdbcURLRoot = "jdbc:mysql://localhost:3306/warehouse?useSSL=false&characterEncoding=utf8";
        String usernameRoot = "root";
        String passwordRoot = "1234";
        try {
			Connection connection_user = DriverManager.getConnection(jdbcURLRoot, usernameRoot, passwordRoot);
			connection_user.setAutoCommit(false);
			PreparedStatement stat = connection_user.prepareStatement("UPDATE `data2` SET `MSSV`='no_value' WHERE `MSSV` is null;\r\n" + 
					"UPDATE `data2` SET `Holot`='no_value' WHERE `Holot` is null;\r\n" + 
					"UPDATE `data2` SET `Ten`='no_value' WHERE `Ten` is null;\r\n" + 
					"UPDATE `data2` SET `Ngaysinh`='no_value' WHERE `Ngaysinh` is null;\r\n" + 
					"UPDATE `data2` SET `Malop`='no_value' WHERE `Malop` is null;\r\n" + 
					"UPDATE `data2` SET `lop`='no_value' WHERE `lop` is null;\r\n" + 
					"UPDATE `data2` SET `sodienthoai`='no_value' WHERE `sodienthoai` is null;\r\n" + 
					"UPDATE `data2` SET `email`='no_value' WHERE `email` is null;\r\n" + 
					"UPDATE `data2` SET `Quequan`='no_value' WHERE `Quequan` is null;\r\n" + 
					"UPDATE `data2` SET `ghichu`='no_value' WHERE `ghichu` is null;");
			stat.executeUpdate();
			connection_user.close();
		} catch (SQLException e1) {
			
			e1.printStackTrace();
		}
	}
	public void standardize() {
		
	}
	public void combineValue(String config) {
		String[] partConfig = config.split("\t");
		String columnStaging = partConfig[7];
		String warehouseRequired = partConfig[8];
		
	}
}
