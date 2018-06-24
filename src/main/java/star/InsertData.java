package star;

import connector.DatabaseConnector;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

//必须先运行CreateTables.java建表，然后再运行此类ETL插入数据
public class InsertData {
    public static String dbName = "BI_STAR";
    public static void main(String args[]){
        InsertData insertData = new InsertData();
        insertData.insertData();
    }

    public void insertData(){
        Connection conn = null;
        Statement stmt = null;
        String sql;

        try {
            conn = DatabaseConnector.getDatabaseConnection(dbName);
            stmt = conn.createStatement();
            //插入client_dim维度表数据，ETL过程数据来源于OLTP源数据库BI_OLTP中的client表
            sql = "INSERT INTO client_dim SELECT NULL,client_id,client_name,client_gender,client_age,client_address,client_phone,client_doe,client_nin " +
                    "FROM BI_OLTP.client WHERE client_id NOT IN (SELECT client_id FROM client_dim);";
            stmt.executeUpdate(sql);
            //插入product_dim维度表数据，ETL过程数据来源于OLTP源数据库BI_OLTP中的product表
            sql = "INSERT INTO product_dim SELECT NULL,product_id,product_name,product_type,product_price " +
                    "FROM BI_OLTP.product WHERE product_id NOT IN (SELECT product_id FROM product_dim);";
            stmt.executeUpdate(sql);
            //插入employee_dim维度表数据，ETL过程数据来源于OLTP源数据库BI_OLTP中的employee表
            sql = "INSERT INTO employee_dim SELECT NULL,employee_id,employee_name,employee_phone,employee_level,employee_post,employee_did,employee_wage " +
                    "FROM BI_OLTP.employee WHERE employee_id NOT IN (SELECT employee_id FROM employee_dim);";
            stmt.executeUpdate(sql);
            //插入time_dim维度表数据，time_dim中time数据来源于OLTP源数据库BI_OLTP中precord表的precord_date字段
            //其余字段的数据来源于time使用相应函数得出的结果
            sql = "INSERT INTO time_dim SELECT NULL,precord_date,YEAR(precord_date),QUARTER(precord_date),MONTH(precord_date),DAYOFWEEK(precord_date),DAYOFMONTH(precord_date),DATE(precord_date),HOUR(precord_date) " +
                    "FROM BI_OLTP.precord WHERE precord_date NOT IN (SELECT time FROM time_dim);";
            stmt.executeUpdate(sql);


            //插入sale_fact事实表数据，client_sk数据来源于client_dim维度表，employee_sk来源于employee_dim维度表
            //product_sk数据来源于product_dim维度表，time_sk数据来源于time_dim维度表，precord_price来源于源数据库BI_OLTP中precord表的precord_price字段
            sql = "INSERT INTO sale_fact SELECT client_sk,product_sk,employee_sk,time_sk,precord_price " +
                    "FROM BI_OLTP.precord bop, client_dim cd, employee_dim ed, product_dim pd, time_dim td " +
                    "WHERE bop.precord_pid = pd.product_id AND bop.precord_cid = cd.client_id AND bop.precord_employee = ed.employee_id AND bop.precord_date = td.time;";
            stmt.executeUpdate(sql);
            System.out.println("all data has been inserted");
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
