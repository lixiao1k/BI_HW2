package snow;

import connector.DatabaseConnector;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class CreateTables_SNOW {
    public static String dbName = "BI_SNOW";
    public static void main(String args[]){
        CreateTables_SNOW ct = new CreateTables_SNOW();
        ct.createTables();
    }

    public void createTables(){
        Connection conn = null;
        Statement stmt = null;
        String sql;
        try {
            conn = DatabaseConnector.getDatabaseConnection(dbName);
            stmt = conn.createStatement();

            //area_dim
            sql = "DROP TABLE IF EXISTS area_dim;";
            stmt.executeUpdate(sql);
            sql = "CREATE TABLE area_dim(" +
                    "  area_sk INT NOT NULL AUTO_INCREMENT," +
                    "  province VARCHAR(10)," +
                    "  city VARCHAR(20)," +
                    "  PRIMARY KEY (area_sk)" +
                    ")DEFAULT CHARSET=utf8;";
            stmt.executeUpdate(sql);

            //education_dim
            sql = "DROP TABLE IF EXISTS education_dim;";
            stmt.executeUpdate(sql);
            sql = "CREATE TABLE education_dim(" +
                    "  education_sk INT NOT NULL AUTO_INCREMENT," +
                    "  diploma VARCHAR(10)," +
                    "  graduate_school VARCHAR(20)," +
                    "  PRIMARY KEY (education_sk)" +
                    ")DEFAULT CHARSET=utf8;";
            stmt.executeUpdate(sql);

            //product_type_dim
            sql = "DROP TABLE IF EXISTS product_type_dim;";
            stmt.executeUpdate(sql);
            sql = "CREATE TABLE product_type_dim(" +
                    "  type_sk INT NOT NULL AUTO_INCREMENT," +
                    "  big_type VARCHAR(10)," +
                    "  small_type VARCHAR(20)," +
                    "  PRIMARY KEY (type_sk)" +
                    ")DEFAULT CHARSET=utf8;";
            stmt.executeUpdate(sql);

            //client_dim
            sql = "DROP TABLE IF EXISTS client_dim;";
            stmt.executeUpdate(sql);
            sql = "CREATE TABLE client_dim(" +
                    "  client_sk INT NOT NULL AUTO_INCREMENT," +
                    "  client_id CHAR(10) NOT NULL ," +
                    "  client_name VARCHAR(20)," +
                    "  client_gender CHAR(2)," +
                    "  area_sk INT," +
                    "  education_sk INT," +
                    "  PRIMARY KEY (client_sk)" +
                    ")DEFAULT CHARSET=utf8;";
            stmt.executeUpdate(sql);

            //product_dim
            sql = "DROP TABLE IF EXISTS product_dim;";
            stmt.executeUpdate(sql);
            sql = "CREATE TABLE product_dim(" +
                    "  product_sk INT NOT NULL AUTO_INCREMENT," +
                    "  product_id CHAR(10) NOT NULL ," +
                    "  product_price BIGINT(20)," +
                    "  type_sk INT NOT NULL," +
                    "  PRIMARY KEY (product_sk)" +
                    ")DEFAULT CHARSET=utf8;";
            stmt.executeUpdate(sql);

            //time_dim
            sql = "DROP TABLE IF EXISTS time_dim;";
            stmt.executeUpdate(sql);
            sql = "CREATE TABLE time_dim(" +
                    "  time_sk INT NOT NULL AUTO_INCREMENT," +
                    "  time DATETIME," +
                    "  year INT UNSIGNED," +
                    "  quarter INT," +
                    "  month INT," +
                    "  PRIMARY KEY (time_sk)" +
                    ")DEFAULT CHARSET=utf8;";
            stmt.executeUpdate(sql);

            //employee_dim
            sql = "DROP TABLE IF EXISTS employee_dim;";
            stmt.executeUpdate(sql);
            sql = "CREATE TABLE employee_dim(" +
                    "  employee_sk INT NOT NULL AUTO_INCREMENT," +
                    "  employee_id CHAR(10) NOT NULL ," +
                    "  employee_name VARCHAR(20)," +
                    "  employee_phone CHAR(11)," +
                    "  PRIMARY KEY (employee_sk)" +
                    ")DEFAULT CHARSET=utf8;";
            stmt.executeUpdate(sql);

            //sale_fact
            sql = "DROP TABLE IF EXISTS sale_fact;";
            stmt.executeUpdate(sql);
            sql = "CREATE TABLE sale_fact(" +
                    "  client_sk INT NOT NULL ," +
                    "  product_sk INT NOT NULL ," +
                    "  employee_sk INT NOT NULL ," +
                    "  time_sk INT NOT NULL ," +
                    "  sale_fact_money BIGINT(20)" +
                    ")DEFAULT CHARSET=utf8;";
            stmt.executeUpdate(sql);
            System.out.println("all tables have been created");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            stmt.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}


















