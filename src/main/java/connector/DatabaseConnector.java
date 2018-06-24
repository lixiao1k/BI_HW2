package connector;

import java.sql.Connection;
import java.sql.DriverManager;

public class DatabaseConnector {
    public static Connection getDatabaseConnection(String DBName) {
        Connection con = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            con = DriverManager.getConnection("jdbc:mysql://localhost:3306/" + DBName + "?useUnicode=true&characterEncoding=UTF-8","root","lixiaodong1996");
            System.out.println("Successfully connect to " + DBName);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return con;
    }
    public static void main(String args[]){
        getDatabaseConnection("BI_OLTP");
    }
}
