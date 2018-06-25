package star;

import connector.DatabaseConnector;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class CreateTables_STAR {
    public static String dbName = "BI_STAR";
    public static void main(String args[]){
        CreateTables_STAR createTables = new CreateTables_STAR();
        createTables.createClientDim();
        createTables.createEmployeeDim();
        createTables.createProductDim();
        createTables.createTimeDim();
        createTables.createSaleFact();
    }

    public void createClientDim(){
        Connection conn = null;
        Statement stmt = null;
        String sql;
        try {
            conn = DatabaseConnector.getDatabaseConnection(dbName);
            stmt = conn.createStatement();
            sql = "DROP TABLE IF EXISTS client_dim;";
            stmt.executeUpdate(sql);
            sql = "CREATE TABLE client_dim(" +
                    "  client_sk INT NOT NULL AUTO_INCREMENT," +
                    "  client_id CHAR(10) NOT NULL ," +
                    "  client_name VARCHAR(20) NOT NULL," +
                    "  client_gender CHAR(2)," +
                    "  client_age INT UNSIGNED," +
                    "  client_address VARCHAR(20)," +
                    "  client_phone CHAR(11)," +
                    "  client_doe VARCHAR(20)," +
                    "  client_nin CHAR(18) NOT NULL," +
                    "  PRIMARY KEY (client_sk)" +
                    ")DEFAULT CHARSET=utf8;";
            stmt.executeUpdate(sql);
            System.out.println("client_dim has been created");
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

    public void createProductDim(){
        Connection conn = null;
        Statement stmt = null;
        String sql;
        try {
            conn = DatabaseConnector.getDatabaseConnection(dbName);
            stmt = conn.createStatement();
            sql = "DROP TABLE IF EXISTS product_dim;";
            stmt.executeUpdate(sql);
            sql = "CREATE TABLE product_dim(" +
                    "  product_sk INT NOT NULL AUTO_INCREMENT," +
                    "  product_id CHAR(10) NOT NULL," +
                    "  product_name VARCHAR(30) NOT NULL," +
                    "  product_type VARCHAR(10)," +
                    "  product_price BIGINT(20)," +
                    "  PRIMARY KEY (product_sk)" +
                    ")DEFAULT CHARSET=utf8;";
            stmt.executeUpdate(sql);
            System.out.println("product_dim has been created");
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

    public void createEmployeeDim(){
        Connection conn = null;
        Statement stmt = null;
        String sql;
        try {
            conn = DatabaseConnector.getDatabaseConnection(dbName);
            stmt = conn.createStatement();
            sql = "DROP TABLE IF EXISTS employee_dim;";
            stmt.executeUpdate(sql);
            sql = "CREATE TABLE employee_dim(" +
                    "  employee_sk INT NOT NULL AUTO_INCREMENT," +
                    "  employee_id CHAR(10) NOT NULL ," +
                    "  employee_name VARCHAR(20) NOT NULL," +
                    "  employee_phone char(11)," +
                    "  employee_level VARCHAR(10)," +
                    "  employee_post VARCHAR(20)," +
                    "  employee_did CHAR(10)," +
                    "  employee_wage INT UNSIGNED," +
                    "  PRIMARY KEY (employee_sk)" +
                    ")DEFAULT CHARSET=utf8;";
            stmt.executeUpdate(sql);
            System.out.println("employee_dim has been created");
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

    public void createTimeDim(){
        Connection conn = null;
        Statement stmt = null;
        String sql;

        try {
            conn = DatabaseConnector.getDatabaseConnection(dbName);
            stmt = conn.createStatement();
            sql = "DROP TABLE IF EXISTS time_dim;";
            stmt.executeUpdate(sql);
            sql = "CREATE TABLE time_dim(" +
                    "  time_sk INT NOT NULL AUTO_INCREMENT," +
                    "  time DATETIME," +
                    "  year INT," +
                    "  quarter INT," +
                    "  month INT," +
                    "  day_of_week INT," +
                    "  day_of_month INT," +
                    "  date DATE," +
                    "  hour INT," +
                    "  PRIMARY KEY (time_sk)" +
                    ")DEFAULT CHARSET=utf8;";
            stmt.executeUpdate(sql);
            System.out.println("time_dim has been created");
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

    public void createSaleFact(){
        Connection conn = null;
        Statement stmt = null;
        String sql;

        try {
            conn = DatabaseConnector.getDatabaseConnection(dbName);
            stmt = conn.createStatement();
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
            System.out.println("sale_fact has been created");
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
