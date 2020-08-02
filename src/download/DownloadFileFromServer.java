package download;

import java.io.File;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
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
	//load chilkat library
	static {
		try {
			System.loadLibrary("chilkat");
		} catch (UnsatisfiedLinkError e) {
			System.err.println("Native code library failed to load.\n" + e);
			System.exit(1);
		}
	}

	public static void main(String[] args) throws ClassNotFoundException {
		DownloadFileFromServer dff = new DownloadFileFromServer();
		dff.writeLog(1, "ready_to_staging");

	}

	public Map<String, String> getConfig() {
		Map<String, String> map = new HashMap<String, String>();

		int idconfig = 0;
		String user = "";
		String password = "";
		String remote_Dir = "";
		String port = "";
		String local_dir = "";
		int idlogtab = 0;
		String status_file = "";

		try {
			//Open a connection
			System.out.println("Connecting to database...");
			Connection conn = (Connection) BaseConnection.getMySQLConnection();
			System.out.println("Connected database successfully!");
			//Execute a query
			System.out.println("Creating statement...");
			Statement stmt = (Statement) conn.createStatement();
			String sql = "select * from configtable as c join logtab as l on c.idconfig = l.idconfig where status_file = \"need_to_download\";";
			stmt.execute(sql);
			ResultSet rs = stmt.getResultSet();
			//Extract data from result set
			while (rs.next()) {
				//Retrieve by column name
				idconfig = rs.getInt("idconfig");
				user = rs.getString("user");
				password = rs.getString("password");
				remote_Dir = rs.getString("remote_Dir");
				port = rs.getString("port");
				local_dir = rs.getString("local_dir");
				idlogtab = rs.getInt("idlogtab");
				status_file = rs.getString("status_file");

				map.put("idconfig", String.valueOf(idconfig));
				map.put("user", user);
				map.put("password", password);
				map.put("remote_Dir", remote_Dir);
				map.put("port", port);
				map.put("local_dir", local_dir);
				map.put("idlogtab", String.valueOf(idlogtab));
				map.put("status_file", status_file);
			}
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
		String remotedir = mapConfig.get("remote_Dir");
		String localdir = mapConfig.get("localdir");
		String port = mapConfig.get("port");
		int portNunm = Integer.parseInt(port);
		int idLogTab = Integer.parseInt(idlogtab);
		String[] s = remotedir.split(",");
		String hn = s[0];
		String us = s[1];
		String pw = s[2];
		String rd = s[3];
		String fn = s[4];

		//Download file with hostname, username, password, remotedir, port, localdir, filename
		download(hn, us, pw, rd, portNunm, localdir, fn);

		File file = new File("localdir");
		if (file.exists()) {
			writeLog(idLogTab, "ready_to_staging");
		} else {
			writeLog(idLogTab, "failed_to_download");
		}
	}

	public static void download(String hostname, String username, String password, String remotedir, int port,
			String localdir, String filename) {
		CkGlobal glob = new CkGlobal();
		glob.UnlockBundle("Waiting...");

		// hostname,port,us,ps,remotedir,filenam,localdir
		// drive.ecepvn.org,guest_access,123456,/volume1/ECEP/song.nguyen/DW_2020/data,sinhvien_*.*
		// drive.google.com,17130271@st.hcmuaf.edu.vn,20071999,/drive/u/1/my-drive,Báo
		// cáo NMCNPM2020_Nhoms13_Web Bán điện thoại di động
		String s = "";
		String[] split = s.split(",");

		CkSsh ssh = new CkSsh();
		ssh.Connect(hostname, port);

		boolean authenticatePw = ssh.AuthenticatePw(username, password);
		System.out.println(authenticatePw);

		CkScp scp = new CkScp();
		scp.UseSsh(ssh);
		scp.put_SyncMustMatch(filename);
		String remote_dir = remotedir;
		String local_dir = localdir;
		scp.SyncTreeDownload(remote_dir, local_dir, 0, false);

		// disconnect
		ssh.Disconnect();

	}

	public void writeLog(int idconfig, String status) throws ClassNotFoundException {
		try {
			//Open a connect
			Connection connection_user = (Connection) BaseConnection.getMySQLConnection();
			connection_user.setAutoCommit(false);
			//Execute a query
			java.lang.String sql = "UPDATE `control_database`.`logtab` SET `status_file` = '" + status
					+ "' WHERE (`idlogTab` = '" + idconfig + "');";
			java.sql.Statement stmt = connection_user.createStatement();
			int i = stmt.executeUpdate(sql);
			System.out.println(i);
			connection_user.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

}
