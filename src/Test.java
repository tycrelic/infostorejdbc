
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class Test {

  public static void main(String args[]) {
    try {
      Class.forName("com.github.tycrelic.infostorejdbc.InfostoreDriver");

      Connection conn = DriverManager.getConnection("jdbc:infostore:dcwvmisdm;authId=secEnterprise", "administrator", "1hkhsadmin");

      Statement stmt = conn.createStatement();

      String sql = "SELECT * FROM CI_SYSTEMOBJECTS WHERE (SI_KIND = 'User' and si_name = 'Administrator')\n"
        + " OR (SI_KIND = 'Profile' and si_name = 'FPMS Security Profile')";

      StringBuilder buf = new StringBuilder(500);
      ResultSet rs = stmt.executeQuery(sql);
      while (rs.next()) {
        String owner = rs.getString(1);
        String table = rs.getString(2);
        int columnId = rs.getInt(3);
        String columnName = rs.getString(4);
        if (columnId == 1) {
          if (buf.length() > 0) {
            System.out.println(buf.toString());
            buf.setLength(0);
          }
          buf.append(owner).append('\t').append(table);
        }
        buf.append('\t').append(columnName);
      }
      if (buf.length() > 0) {
        System.out.println(buf);
        buf.setLength(0);
      }

      conn.close();
    } catch (Exception e) {
      System.out.println(e);
    }

  }
}
