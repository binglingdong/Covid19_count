import java.sql.*;
import java.util.Date;


public class SalaryStdDev {
    private static Connection con = null;

    public void setDBConnection(String url, String user, String password) {
        try {
            Class.forName("com.ibm.db2.jcc.DB2Driver").newInstance();
            con = DriverManager.getConnection(url, user, password);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String dbName = args[0];
        String tableName = args[1];
        String user = args[2];
        String password = args[3];
        String dbURL = "jdbc:db2://localhost:50000/"+dbName;
        //String user = "db2inst1";
        //String password = "GD1OJfLGG64HV2dtwK"; //Change to your own login/passwd

        SalaryStdDev ss = new SalaryStdDev();
        ss.setDBConnection(dbURL, user, password);
        String query = "SELECT salary FROM " + tableName;
        try {
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            double sum = 0;
            double count = 0;
            double sumOfxSquare = 0;

            while(rs.next()){
                double x = rs.getDouble(1);
                count++;
                sum+=x;
                sumOfxSquare += x*x;
            }
            double avg = sum/count;
            double std = Math.sqrt(sumOfxSquare/count - avg*avg);
            System.out.println(std);
            rs.close();
            stmt.close();
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
