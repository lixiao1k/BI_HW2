package snow;

import connector.DatabaseConnector;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

//运行此文件前必须先运行CreateTables_SNOW文件来建表，同时必须先运行OLTP两个文件来建设源数据库
public class InsertData_SNOW {
    public static String dbName = "BI_SNOW";
    public static void main(String args[]){
        InsertData_SNOW insertData = new InsertData_SNOW();
        insertData.insertData();
    }

    public void insertData(){
        Connection conn = null;
        Statement stmt = null;
        String sql;

        try {
            conn = DatabaseConnector.getDatabaseConnection(dbName);
            stmt = conn.createStatement();
            //地址维度表数据
            sql = "INSERT INTO area_dim(province) VALUES ('河北'),('山西'),('辽宁'),('黑龙江'),('江苏'),('浙江'),('安徽')," +
                    "  ('福建'),('台湾'),('江西'),('山东'),('河南'),('湖北'),('湖南'),('广东'),('海南'),('四川'),('贵州')," +
                    "  ('云南'),('陕西'),('甘肃'),('青海'),('内蒙古'),('广西'),('西藏'),('宁夏'),('新疆'),('北京'),('天津'),('上海'),('重庆'),('香港'),('澳门');";
            stmt.executeUpdate(sql);

            //文化程度维度表数据
            sql = "INSERT INTO education_dim(diploma) VALUES ('文盲'),('小学'),('初中'),('高中'),('学士'),('硕士'),('博士'),('博士后');";
            stmt.executeUpdate(sql);

            //客户维度表数据
            sql = "INSERT INTO client_dim SELECT NULL,client_id,client_name,client_gender,area_sk,education_sk" +
                    "  FROM BI_OLTP.client boc, area_dim ad, education_dim ed " +
                    "WHERE boc.client_address = ad.province AND boc.client_doe = ed.diploma" +
                    "      AND client_id NOT IN (SELECT client_id FROM client_dim);";
            stmt.executeUpdate(sql);

            //产品类型维度表数据
            sql = "INSERT INTO product_type_dim(big_type, small_type) VALUES ('保险','大病医疗保险'),('保险','出行意外保险'),('保险','人身意外保险')," +
                    "  ('保险','人寿保险'),('保险','汽车保险'),('理财','储蓄'),('理财','黄金'),('理财','股票'),('理财','基金'),('理财','期货');";
            stmt.executeUpdate(sql);

            //产品维度表数据
            sql = "INSERT INTO product_dim  SELECT NULL,product_id,product_price,type_sk" +
                    "  FROM BI_OLTP.product bop, product_type_dim ptd " +
                    "WHERE bop.product_type = ptd.big_type AND bop.product_name = ptd.small_type " +
                    "AND product_id NOT IN (SELECT product_id FROM product_dim);";
            stmt.executeUpdate(sql);

            //员工维度表数据
            sql = "INSERT INTO employee_dim SELECT NULL,employee_id, employee_name, employee_phone" +
                    "  FROM BI_OLTP.employee " +
                    "WHERE employee_id NOT IN (SELECT employee_id FROM employee_dim);";
            stmt.executeUpdate(sql);

            //时间维度表数据
            sql = "INSERT INTO time_dim SELECT NULL,precord_date,YEAR(precord_date),QUARTER(precord_date),MONTH(precord_date) " +
                    "  FROM BI_OLTP.precord " +
                    "WHERE precord_date NOT IN (SELECT time FROM time_dim);";
            stmt.executeUpdate(sql);

            //销售记录事实表数据
            sql = "INSERT INTO sale_fact SELECT  client_sk, product_sk, employee_sk, time_sk, precord_price " +
                    "  FROM BI_OLTP.precord bop, client_dim cd, product_dim pd, time_dim td, employee_dim ed " +
                    "WHERE bop.precord_pid = pd.product_id AND bop.precord_cid = cd.client_id AND bop.precord_employee = ed.employee_id " +
                    "AND bop.precord_date = td.time;";
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
