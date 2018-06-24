package oltp;

import connector.DatabaseConnector;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class CreateTables {
    public static String dbName = "BI_OLTP";
    public static void main(String args[]){
        CreateTables ct = new CreateTables();
        ct.deleteIfExistTables();
        ct.createClientTable();
        ct.createProductTable();
        ct.createPrecordTable();
        ct.createDepartmentTable();
        ct.createEmployeeTable();
        ct.createFirmTable();
        ct.createIrecordTable();
        ct.createAssetsTable();
        ct.createSalebillTable();
        ct.createInvestbillTable();
        ct.createPayrollTable();
        ct.addForeignKey();
    }

    //如果存在表则删除，由于有外键约束，删除顺序得事先定义
    public void deleteIfExistTables(){
        Connection conn = null;
        Statement stmt = null;
        String sql;
        try {
            conn = DatabaseConnector.getDatabaseConnection(dbName);
            stmt = conn.createStatement();

            sql = "DROP TABLE IF EXISTS precord;";
            stmt.executeUpdate(sql);
            sql = "DROP TABLE IF EXISTS irecord;";
            stmt.executeUpdate(sql);
            sql = "DROP TABLE IF EXISTS payroll;";
            stmt.executeUpdate(sql);
            sql = "DROP TABLE IF EXISTS client;";
            stmt.executeUpdate(sql);
            sql = "DROP TABLE IF EXISTS product;";
            stmt.executeUpdate(sql);
            sql = "DROP TABLE IF EXISTS department;";
            stmt.executeUpdate(sql);
            sql = "DROP TABLE IF EXISTS employee;";
            stmt.executeUpdate(sql);
            sql = "DROP TABLE IF EXISTS firm;";
            stmt.executeUpdate(sql);
            sql = "DROP TABLE IF EXISTS assets;";
            stmt.executeUpdate(sql);
            sql = "DROP TABLE IF EXISTS salebill;";
            stmt.executeUpdate(sql);
            sql = "DROP TABLE IF EXISTS investbill;";
            stmt.executeUpdate(sql);
            System.out.println("all tables is deleted");
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

    //创建client表
    public void createClientTable(){
        Connection conn = null;
        Statement stmt = null;
        String sql;
        try {
            conn = DatabaseConnector.getDatabaseConnection(dbName);
            stmt = conn.createStatement();

            sql = "CREATE TABLE client(" +
                    "  client_id CHAR(10) NOT NULL ," +
                    "  client_name VARCHAR(20) NOT NULL ," +
                    "  client_gender CHAR(2)," +
                    "  client_age INT UNSIGNED," +
                    "  client_address VARCHAR(20)," +
                    "  client_phone CHAR(11)," +
                    "  client_doe VARCHAR(20)," +
                    "  client_nin CHAR(18) NOT NULL," +
                    "  PRIMARY KEY (client_id)" +
                    ")DEFAULT CHARSET=utf8;";
            stmt.executeUpdate(sql);
            System.out.println("client table is created");
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

    //创建product表
    public void createProductTable(){
        Connection conn = null;
        Statement stmt = null;
        String sql;
        try {
            conn = DatabaseConnector.getDatabaseConnection(dbName);
            stmt = conn.createStatement();

            sql = "CREATE TABLE product(" +
                    "  product_id CHAR(10) NOT NULL ," +
                    "  product_name VARCHAR(30) NOT NULL ," +
                    "  product_type VARCHAR(10)," +
                    "  product_price BIGINT(20)," +
                    "  PRIMARY KEY (product_id)" +
                    ")DEFAULT CHARSET=utf8;";
            stmt.executeUpdate(sql);
            System.out.println("product table is created");
        }catch (SQLException e){
            e.printStackTrace();
        }
        try {
            stmt.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //创建precord表
    public void createPrecordTable(){
        Connection conn = null;
        Statement stmt = null;
        String sql;
        try {
            conn = DatabaseConnector.getDatabaseConnection(dbName);
            stmt = conn.createStatement();

            sql = "CREATE TABLE precord(" +
                    "  precord_id CHAR(10) NOT NULL ," +
                    "  precord_pid CHAR(10) NOT NULL ," +
                    "  precord_cid CHAR(10) NOT NULL ," +
                    "  precord_bid CHAR(10) NOT NULL ," +
                    "  precord_price BIGINT(20)," +
                    "  precord_date DATETIME," +
                    "  precord_employee CHAR(10)," +
                    "  PRIMARY KEY (precord_id)," +
                    "  KEY fk_ppid (precord_pid)," +
                    "  KEY fk_pcid (precord_cid)," +
                    "  KEY fk_pbid (precord_bid)" +
                    ")DEFAULT CHARSET=utf8;";
            stmt.executeUpdate(sql);
            System.out.println("precord table is created");
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

    //创建department表
    public void createDepartmentTable(){
        Connection conn = null;
        Statement stmt = null;
        String sql;
        try {
            conn = DatabaseConnector.getDatabaseConnection(dbName);
            stmt = conn.createStatement();

            sql = "CREATE TABLE department(" +
                    "  department_id CHAR(10) NOT NULL ," +
                    "  department_name VARCHAR(20)," +
                    "  PRIMARY KEY (department_id)" +
                    ")DEFAULT CHARSET=utf8;";
            stmt.executeUpdate(sql);
            System.out.println("department table is created");
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

    //创建employee表
    public void createEmployeeTable(){
        Connection conn = null;
        Statement stmt = null;
        String sql;
        try {
            conn = DatabaseConnector.getDatabaseConnection(dbName);
            stmt = conn.createStatement();

            sql = "CREATE TABLE employee(" +
                    "  employee_id CHAR(10) NOT NULL ," +
                    "  employee_name VARCHAR(20) NOT NULL ," +
                    "  employee_phone char(11)," +
                    "  employee_level VARCHAR(10)," +
                    "  employee_post VARCHAR(20)," +
                    "  employee_did CHAR(10)," +
                    "  employee_wage INT UNSIGNED," +
                    "  PRIMARY KEY (employee_id)" +
                    ")DEFAULT CHARSET=utf8;";
            stmt.executeUpdate(sql);
            System.out.println("employee table is created");
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

    //创建firm表
    public void createFirmTable(){
        Connection conn = null;
        Statement stmt = null;
        String sql;
        try {
            conn = DatabaseConnector.getDatabaseConnection(dbName);
            stmt = conn.createStatement();

            sql = "CREATE TABLE firm(" +
                    "  firm_id CHAR(10) NOT NULL ," +
                    "  firm_name VARCHAR(30)," +
                    "  firm_credit CHAR(5)," +
                    "  firm_address VARCHAR(20)," +
                    "  PRIMARY KEY (firm_id)" +
                    ")DEFAULT CHARSET=utf8;";
            stmt.executeUpdate(sql);
            System.out.println("employee table is created");
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

    //创建irecord表
    public void createIrecordTable(){
        Connection conn = null;
        Statement stmt = null;
        String sql;

        try {
            conn = DatabaseConnector.getDatabaseConnection(dbName);
            stmt = conn.createStatement();

            sql = "CREATE TABLE irecord(" +
                    "  irecord_id CHAR(10) NOT NULL ," +
                    "  irecord_fid CHAR(10) NOT NULL ," +
                    "  irecord_bid CHAR(10) NOT NULL ," +
                    "  irecord_eid CHAR(10) NOT NULL ," +
                    "  irecord_date DATETIME," +
                    "  irecord_ror DECIMAL(10,2)," +
                    "  irecord_remark VARCHAR(255)," +
                    "  PRIMARY KEY (irecord_id)," +
                    "  KEY fk_ifid (irecord_fid)," +
                    "  KEY fk_ibid (irecord_bid)," +
                    "  KEY fk_ieid (irecord_eid)" +
                    ")DEFAULT CHARSET=utf8;";
            stmt.executeUpdate(sql);
            System.out.println("irecord table is created");
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

    //创建assets表
    public void createAssetsTable(){
        Connection conn = null;
        Statement stmt = null;
        String sql;

        try {
            conn = DatabaseConnector.getDatabaseConnection(dbName);
            stmt = conn.createStatement();

            sql = "CREATE TABLE assets(" +
                    "  assets_id CHAR(10) NOT NULL ," +
                    "  assets_name VARCHAR(20)," +
                    "  assets_balance BIGINT(20)," +
                    "  PRIMARY KEY (assets_id)" +
                    ")DEFAULT CHARSET=utf8;";
            stmt.executeUpdate(sql);
            System.out.println("assets table is created");
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

    //创建salebill表
    public void createSalebillTable(){
        Connection conn = null;
        Statement stmt = null;
        String sql;

        try {
            conn = DatabaseConnector.getDatabaseConnection(dbName);
            stmt = conn.createStatement();

            sql = "CREATE TABLE salebill(" +
                    "  salebill_id CHAR(10) NOT NULL ," +
                    "  salebill_type VARCHAR(10) NOT NULL ," +
                    "  salebill_date DATETIME," +
                    "  salebill_money BIGINT(20)," +
                    "  PRIMARY KEY (salebill_id)" +
                    ")DEFAULT CHARSET=utf8;";
            stmt.executeUpdate(sql);
            System.out.println("salebill table is created");
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

    //创建investbill表
    public void createInvestbillTable(){
        Connection conn = null;
        Statement stmt = null;
        String sql;

        try {
            conn = DatabaseConnector.getDatabaseConnection(dbName);
            stmt = conn.createStatement();

            sql = "CREATE TABLE investbill(" +
                    "  investbill_id CHAR(10) NOT NULL ," +
                    "  investbill_type VARCHAR(10) NOT NULL ," +
                    "  investbill_date DATETIME," +
                    "  investbill_money BIGINT(20)," +
                    "  PRIMARY KEY (investbill_id)" +
                    ")DEFAULT CHARSET=utf8;";
            stmt.executeUpdate(sql);
            System.out.println("investbill table is created");
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

    //创建payroll表
    public void createPayrollTable(){
        Connection conn = null;
        Statement stmt = null;
        String sql;
        try {
            conn = DatabaseConnector.getDatabaseConnection(dbName);
            stmt = conn.createStatement();

            sql = "CREATE TABLE payroll(" +
                    "  payroll_id CHAR(10) NOT NULL ," +
                    "  payroll_eid CHAR(10) NOT NULL ," +
                    "  payroll_date DATETIME," +
                    "  payroll_money BIGINT(20)," +
                    "  PRIMARY KEY (payroll_id)," +
                    "  KEY fk_peid (payroll_eid)" +
                    ")DEFAULT CHARSET=utf8;";
            stmt.executeUpdate(sql);
            System.out.println("payroll table is created");
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

    //添加各表的外键约束
    public void addForeignKey(){
        Connection conn = null;
        Statement stmt = null;
        String sql;
        try {
            conn = DatabaseConnector.getDatabaseConnection(dbName);
            stmt = conn.createStatement();

            sql = "ALTER TABLE precord ADD CONSTRAINT fk_ppid FOREIGN KEY (precord_pid) REFERENCES product(product_id);";
            stmt.executeUpdate(sql);
            sql = "ALTER TABLE precord ADD CONSTRAINT fk_pcid FOREIGN KEY (precord_cid) REFERENCES client(client_id);";
            stmt.executeUpdate(sql);
            sql = "ALTER TABLE precord ADD CONSTRAINT fk_pbid FOREIGN KEY (precord_bid) REFERENCES salebill(salebill_id);";
            stmt.executeUpdate(sql);
            sql = "ALTER TABLE irecord ADD CONSTRAINT fk_ifid FOREIGN KEY (irecord_fid) REFERENCES firm(firm_id);";
            stmt.executeUpdate(sql);
            sql = "ALTER TABLE irecord ADD CONSTRAINT fk_ibid FOREIGN KEY (irecord_bid) REFERENCES payroll(payroll_id);";
            stmt.executeUpdate(sql);
            sql = "ALTER TABLE irecord ADD CONSTRAINT fk_ieid FOREIGN KEY (irecord_eid) REFERENCES employee(employee_id);";
            stmt.executeUpdate(sql);
            sql = "ALTER TABLE payroll ADD CONSTRAINT fk_peid FOREIGN KEY (payroll_eid) REFERENCES employee(employee_id);";
            stmt.executeUpdate(sql);
            System.out.println("Foreign keys are created");
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
