package download;

import java.io.File;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.chilkatsoft.CkGlobal;
import com.chilkatsoft.CkScp;
import com.chilkatsoft.CkSsh;
import com.jscape.ftcl.b.a.M;
import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;
import com.mysql.jdbc.Statement;

import connectionDatabase.BaseConnection;

public class DownloadFileFromServer {
	// Load chilkat library
//	static {
//		try {
//			System.loadLibrary("chilkat");
//		} catch (UnsatisfiedLinkError e) {
//			System.err.println("Native code library failed to load.\n" + e);
//			System.exit(1);
//		}
//	}

	public static void main(String[] args) throws ClassNotFoundException {
		DownloadFileFromServer dff = new DownloadFileFromServer();
//		dff.writeLog(1, "ready_to_staging");
		dff.appInstall();
//		dff.getConfig();

	}

	public Map<String, String> getConfig() {
		Map<String, String> map = new HashMap<String, String>();

		int idconfig = 0;
		String user = "";
		String password = "";
		String remote_Dir = "";
		String port = "";
		String file_pattern = "";
		String local_dir = "";

		try {
			// Open a connection
			System.out.println("Connecting to database...");
			Connection conn = (Connection) BaseConnection.getMySQLConnection();
			System.out.println("Connected database successfully!");

			// Execute a query
			System.out.println("Creating statement...");
			Statement stmt = (Statement) conn.createStatement();
			String sql = "select * from configtable";
			System.out.println("Executing statement...");
			stmt.execute(sql);
			ResultSet rs = stmt.getResultSet();

			// Extract data from result set
			while (rs.next()) {
				// Retrieve by column name
				idconfig = rs.getInt("idconfig");
				user = rs.getString("user");
				password = rs.getString("password");
				remote_Dir = rs.getString("remote_Dir");
				port = rs.getString("port");
				file_pattern = rs.getString("file_pattern");
				local_dir = rs.getString("local_dir");

				map.put("idconfig", String.valueOf(idconfig));
				map.put("user", user);
				map.put("password", password);
				map.put("remote_Dir", remote_Dir);
				map.put("port", port);
				map.put("file_pattern", file_pattern);
				map.put("local_dir", local_dir);

			}
			// Disconnect
			rs.close();
			conn.close();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return map;
	}

	public void appInstall() throws ClassNotFoundException {
		Map<String, String> mapConfig = getConfig();
		
		String idlogtab = mapConfig.get("idlogtab");
		String idconfig = mapConfig.get("idconfig");
		String remotedir = mapConfig.get("remote_Dir");
		String localdir = mapConfig.get("local_dir");
		String port = mapConfig.get("port");
		String file_pattern = mapConfig.get("file_pattern");
		int portNum = Integer.parseInt(port);
//		int idLogTab = Integer.parseInt(idlogtab);
		int idConfig = Integer.parseInt(idconfig);
//		String filePathLocal = mapConfig.get("filePathLocal");

		// drive.ecepvn.org,guest_access,123456,/volume1/ECEP/song.nguyen/DW_2020/data
		// Cắt remotedir để lấy các phần tử hostname, username, password, remotedir, file_path
		String[] s = remotedir.split(",");
		String hn = s[0];
		String us = s[1];
		String pw = s[2];
		String rd = s[3];
		// Download file with hostname, username, password, remotedir, port, localdir,
		// file_pattern
		download(hn, us, pw, rd, portNum, localdir, file_pattern);

		// Kiểm tra xem trong thư mục local có file không
		File file = new File("local_dir");
		// Nếu có thì thông báo thành công và ghi vào log là sẵn sàng để load vào
		// staging
		if (file.exists()) {
			System.out.println("Downloaded successfully!");
			writeLog(idConfig, "ready_to_staging");
			// Nếu không thì thông báo thất bại và ghi vào log là tải xuống thất bại
		} else {
			System.out.println("Download failed!");
			writeLog(idConfig, "failed_to_download");
		}
	}

	public static void download(String hostname, String username, String password, String remotedir, int port,
			String localdir, String file_pattern) {
		// Load Chilkat library
		try {
			System.loadLibrary("chilkat");
		} catch (UnsatisfiedLinkError e) {
			System.err.println("Native code library failed to load.\n" + e);
			System.exit(1);
		}
		
		// Mở khóa Bundle
		CkGlobal glob = new CkGlobal();
		glob.UnlockBundle("Waiting...");

		// Kết nối với SSH server
		CkSsh ssh = new CkSsh();
		ssh.Connect(hostname, port);

		// Chờ tối đa 5s khi đọc phản hồi
		ssh.put_IdleTimeoutMs(5000);

		// Xác thực bằng tài khoản/mặt khẩu
		boolean authenticatePw = ssh.AuthenticatePw(username, password);
		System.out.println("Autheticate password: " + authenticatePw);

		CkScp scp = new CkScp();
		scp.UseSsh(ssh);
		// Tải xuống các tệp có tên khớp với cột file_pattern trong bảng config
		scp.put_SyncMustMatch(file_pattern);
		String remote_dir = remotedir;
		String local_dir = localdir;

		// Các chế độ tải xuống:
		// mode = 0: Tải xuống tất cả các tệp
		// mode = 1: Tải xuống tất cả các tệp không tồn tại trên hệ thống tệp cục bộ.
		// mode = 2: Tải xuống các tệp mới hơn hoặc không tồn tại.
		// mode = 3: Chỉ tải xuống các tệp mới hơn.
		// Nếu một tệp chưa tồn tại trên hệ thống tệp cục bộ, nó sẽ không được tải xuống
		// từ máy chủ.
		// mode = 5: Chỉ tải xuống các tệp bị thiếu hoặc các tệp có kích thước khác
		// nhau.
		// mode = 6: Tương tự như mode 5, nhưng cũng tải xuống các tệp mới hơn.
		int mode = 2;

		// Thực hiện download từ remote_dir về local_dir
		scp.SyncTreeDownload(remote_dir, local_dir, mode, false);

		// disconnect
		ssh.Disconnect();

	}

	public void writeLog(int idconfig, String status) throws ClassNotFoundException {
		try {
			// Open a connect
			Connection connection_user = (Connection) BaseConnection.getMySQLConnection();

			// Lấy tên thư mục lưu file từ config table
			File fileLocal = new File(getConfig().get("local_dir"));
			File[] listFile = fileLocal.listFiles();
			for (int i = 0; i < listFile.length; i++) {
				File file2 = listFile[i];
//				java.lang.String sql = "UPDATE `control_database`.`logtab` SET " + "`idconfig` = '" + idconfig
//						+ "' AND `status_file` = '" + status + "' AND `filePathLocal` = '" + file2.getAbsolutePath() + "';";
				String sql = "INSERT INTO logtab (idlogtab, idconfig, filePathLocal, status_file) VALUES ('" + i
						+ "', '" + idconfig + "', '" + file2.getAbsolutePath() + "', '" + status + "');";
				Statement stmt = (Statement) connection_user.createStatement();
				stmt.addBatch(sql);
				stmt.executeBatch();

			}

			// Disconnect
			connection_user.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

}
