package download;

import java.io.File;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.poi.hssf.record.DBCellRecord;

import com.chilkatsoft.CkGlobal;
import com.chilkatsoft.CkScp;
import com.chilkatsoft.CkSsh;
import com.jscape.ftcl.b.a.M;
import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;
import com.mysql.jdbc.Statement;

import connectionDatabase.BaseConnection;

public class DownloadFileFromServer {

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
			// 1. Mở kết nối đến database control
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
		int idConfig = Integer.parseInt(idconfig);

		// drive.ecepvn.org,guest_access,123456,/volume1/ECEP/song.nguyen/DW_2020/data
		// Cắt remotedir để lấy các phần tử hostname, username, password, remotedir, file_path
		String[] s = remotedir.split(",");
		String hn = s[0];
		String us = s[1];
		String pw = s[2];
		String rd = s[3];
		
		// 2. Lấy các thuộc tính cần thiết trong bảng configtable đưa vào phương thức download
		download(hn, us, pw, rd, portNum, localdir, file_pattern);

		// 8. Gọi phương thức writeLog() để lấy tên tất cả các file trong local ghi vào log table
		writeLog(idConfig, "ready_to_staging");

	}

	public static void download(String hostname, String username, String password, String remotedir, int port,
			String localdir, String file_pattern) {
		// 3. Load thư viện chilkat
		try {
			System.loadLibrary("chilkat");
		} catch (UnsatisfiedLinkError e) {
			System.err.println("Native code library failed to load.\n" + e);
			System.exit(1);
		}
		
		// 4. Mở khóa Bundle
		CkGlobal glob = new CkGlobal();
		glob.UnlockBundle("Waiting...");

		// 5. Dùng tham số hostname, port, username, password để kết nối tới SSH server
		CkSsh ssh = new CkSsh();
		ssh.Connect(hostname, port); 

		// Chờ tối đa 5s khi đọc phản hồi
		ssh.put_IdleTimeoutMs(5000);

		// Xác thực bằng tài khoản/mặt khẩu
		boolean authenticatePw = ssh.AuthenticatePw(username, password);
		System.out.println("Autheticate password: " + authenticatePw);

		// Gọi đối tượng scp để download
		CkScp scp = new CkScp();
		scp.UseSsh(ssh);
		// 6. Dùng tham số file_pattern, remote_dir, local_dir để thực hiện download file về local
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

		// 7. Ngắt kết nối SSH server
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

				String sql = "INSERT INTO logtab ( idconfig, filePathLocal, status_file) VALUES ('" + idconfig + "', '" + file2.getAbsolutePath() + "', '" + status + "');";
				Statement stmt = (Statement) connection_user.createStatement();
				stmt.addBatch(sql);
				// Thực thi bacth
				stmt.executeBatch();

			}

			// Disconnect
			connection_user.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

}
