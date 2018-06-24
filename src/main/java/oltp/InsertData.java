package oltp;

import connector.DatabaseConnector;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;


//必须先运行CreateTables类才可运行此类

public class InsertData {
    public static String dbName = "BI_OLTP";
    public static void main(String args[]){
        InsertData insertData = new InsertData();
        insertData.insert2product();
        insertData.insert2department();
        insertData.insert2employee();
        insertData.insert2client();
        insertData.insert2salebill();
        insertData.insert2precord();
    }



    //插入product表数据
    public void insert2product(){
        Connection conn = null;
        PreparedStatement pstmt = null;
        String sql;

        try {
            conn = DatabaseConnector.getDatabaseConnection(dbName);
            conn.setAutoCommit(false);
            pstmt = conn.prepareStatement("");
            sql = "INSERT INTO product VALUES "+
                    "('p000000001','大病医疗保险','保险',150),"+
                    "('p000000002','出行意外保险','保险',50),"+
                    "('p000000003','人身意外保险','保险',50),"+
                    "('p000000004','人寿保险','保险',100),"+
                    "('p000000005','汽车保险','保险',500),"+
                    "('p000000006','储蓄','理财',30000),"+
                    "('p000000007','黄金','理财',200000),"+
                    "('p000000008','股票','理财',500000),"+
                    "('p000000009','基金','理财',60000),"+
                    "('p000000010','期货','理财',300000)";
            pstmt.addBatch(sql);
            pstmt.executeBatch();
            conn.commit();
            System.out.println("product's data is inserted");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            pstmt.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    //插入department表数据
    public void insert2department(){
        Connection conn = null;
        PreparedStatement pstmt = null;
        String sql;

        try {
            conn = DatabaseConnector.getDatabaseConnection(dbName);
            conn.setAutoCommit(false);
            pstmt = conn.prepareStatement("");
            sql = "INSERT INTO department VALUES "+
                    "('d000000001','销售部门'),"+
                    "('d000000002','人事部门'),"+
                    "('d000000003','财会部门'),"+
                    "('d000000004','投资部门'),"+
                    "('d000000005','客户管理部门'),"+
                    "('d000000006','风险管理部门')";
            pstmt.addBatch(sql);
            pstmt.executeBatch();
            conn.commit();
            System.out.println("department's data is inserted");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            pstmt.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    //插入employee表数据
    public void insert2employee(){
        Connection conn = null;
        PreparedStatement pstmt = null;
        String sql;

        try {
            conn = DatabaseConnector.getDatabaseConnection(dbName);
            conn.setAutoCommit(false);
            pstmt = conn.prepareStatement("");
            sql = "INSERT INTO employee VALUES "+
                    "('e000000001','李春','13713700001','T1','销售员','d000000001',5000),"+
                    "('e000000002','卫怡雪','1371370002','T1','销售员','d000000001',5000),"+
                    "('e000000003','何歆虹','1371370003','T1','销售员','d000000001',5000),"+
                    "('e000000004','鲍晶','1371370004','T1','销售员','d000000001',5000),"+
                    "('e000000005','彭凯','1371370005','T2','销售主管','d000000001',8000),"+
                    "('e000000006','陈丽华','1371370006','T2','销售主管','d000000001','8000')";
            pstmt.addBatch(sql);
            pstmt.executeBatch();
            conn.commit();
            System.out.println("employee's data is inserted");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            pstmt.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    //插入client表数据
    public void insert2client(){
        Connection conn = null;
        PreparedStatement pstmt = null;
        String sql;

        try {
            conn = DatabaseConnector.getDatabaseConnection(dbName);
            conn.setAutoCommit(false);
            pstmt = conn.prepareStatement("");
            sql = "INSERT INTO client VALUES "+
                    "('c000000001','朱香思','女',23,'江苏','13613600001','学士','320684199511192342'),"+
                    "('c000000002','李恩香','女',24,'浙江','13613600002','硕士','320684199411192342'),"+
                    "('c000000003','薛锦云','女',25,'上海','13613600003','博士','320684199311192342'),"+
                    "('c000000004','任娜','女',23,'河北','13613600004','高中','320684199511192343'),"+
                    "('c000000005','周乔','女',33,'河南','13613600005','博士','320684198511192342'),"+
                    "('c000000006','金秋','女',30,'江苏','13613600006','硕士','320684198811192342'),"+
                    "('c000000007','赵韵','女',29,'江苏','13613600007','硕士','320684198911192342'),"+
                    "('c000000008','郑丽','女',28,'江苏','13613600008','学士','320684199011192342'),"+
                    "('c000000009','华洪','男',28,'上海','13613600009','学士','320684199011192343'),"+
                    "('c000000010','唐雪','女',25,'浙江','13613600010','硕士','320684199311192343'),"+
                    "('c000000011','孙静','女',26,'浙江','13613600011','硕士','320684199211192342'),"+
                    "('c000000012','王海','男',35,'陕西','13613600012','博士','320684198311192342')";
            pstmt.addBatch(sql);
            pstmt.executeBatch();
            conn.commit();
            System.out.println("client's data is inserted");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            pstmt.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //插入salebill表数据
    public void insert2salebill(){
        Connection conn = null;
        PreparedStatement pstmt = null;
        String sql;

        try {
            conn = DatabaseConnector.getDatabaseConnection(dbName);
            conn.setAutoCommit(false);
            pstmt = conn.prepareStatement("");
            sql = "INSERT INTO salebill VALUES "+
                    "('s000000001','入账','2018-6-24 13:00:00',150),"+
                    "('s000000002','入账','2018-6-23 13:00:00',150),"+
                    "('s000000003','入账','2018-6-23 13:00:00',150),"+
                    "('s000000004','入账','2018-6-22 13:00:00',150),"+
                    "('s000000005','入账','2018-6-20 13:00:00',30000),"+
                    "('s000000006','入账','2018-6-18 13:00:00',30000),"+
                    "('s000000007','入账','2018-6-16 13:00:00',30000),"+
                    "('s000000008','入账','2018-6-14 14:00:00',30000),"+
                    "('s000000009','入账','2018-6-12 14:00:00',500),"+
                    "('s000000010','入账','2018-6-10 14:00:00',500),"+
                    "('s000000011','入账','2018-6-08 14:00:00',500),"+
                    "('s000000012','入账','2018-6-06 14:00:00',500),"+
                    "('s000000013','入账','2018-6-04 14:00:00',50),"+
                    "('s000000014','入账','2018-6-02 15:00:00',50),"+
                    "('s000000015','入账','2018-5-31 15:00:00',50),"+
                    "('s000000016','入账','2018-5-29 15:00:00',50),"+
                    "('s000000017','入账','2018-5-27 15:00:00',50),"+
                    "('s000000018','入账','2018-5-25 15:00:00',50),"+
                    "('s000000019','入账','2018-5-15 15:00:00',100),"+
                    "('s000000020','入账','2018-5-05 16:00:00',100),"+
                    "('s000000021','入账','2018-5-01 16:00:00',100),"+
                    "('s000000022','入账','2018-4-25 16:00:00',100),"+
                    "('s000000023','入账','2018-4-20 10:00:00',200000),"+
                    "('s000000024','入账','2018-4-10 10:00:00',200000),"+
                    "('s000000025','入账','2018-4-05 10:00:00',200000),"+
                    "('s000000026','入账','2018-3-31 10:00:00',60000)";
            pstmt.addBatch(sql);
            pstmt.executeBatch();
            conn.commit();
            System.out.println("salebill's data is inserted");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            pstmt.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //插入precord表数据
    public void insert2precord(){
        Connection conn = null;
        PreparedStatement pstmt = null;
        String sql;

        try {
            conn = DatabaseConnector.getDatabaseConnection(dbName);
            conn.setAutoCommit(false);
            pstmt = conn.prepareStatement("");
            sql = "INSERT INTO precord VALUES "+
                    "('r000000001','p000000001','c000000001','s000000001',150,'2018-6-24 13:00:00','e000000001'),"+
                    "('r000000002','p000000001','c000000002','s000000002',150,'2018-6-23 13:00:00','e000000001'),"+
                    "('r000000003','p000000001','c000000003','s000000003',150,'2018-6-23 13:00:00','e000000001'),"+
                    "('r000000004','p000000001','c000000004','s000000004',150,'2018-6-22 13:00:00','e000000001'),"+
                    "('r000000005','p000000006','c000000005','s000000005',30000,'2018-6-20 13:00:00','e000000002'),"+
                    "('r000000006','p000000006','c000000006','s000000006',30000,'2018-6-18 13:00:00','e000000002'),"+
                    "('r000000007','p000000006','c000000007','s000000007',30000,'2018-6-16 13:00:00','e000000002'),"+
                    "('r000000008','p000000006','c000000008','s000000008',30000,'2018-6-14 14:00:00','e000000002'),"+
                    "('r000000009','p000000005','c000000009','s000000009',500,'2018-6-12 14:00:00','e000000003'),"+
                    "('r000000010','p000000005','c000000010','s000000010',500,'2018-6-10 14:00:00','e000000003'),"+
                    "('r000000011','p000000005','c000000011','s000000011',500,'2018-6-08 14:00:00','e000000003'),"+
                    "('r000000012','p000000005','c000000012','s000000012',500,'2018-6-06 14:00:00','e000000003'),"+
                    "('r000000013','p000000002','c000000001','s000000013',50,'2018-6-04 14:00:00','e000000004'),"+
                    "('r000000014','p000000002','c000000002','s000000014',50,'2018-6-02 15:00:00','e000000004'),"+
                    "('r000000015','p000000002','c000000003','s000000015',50,'2018-5-31 15:00:00','e000000004'),"+
                    "('r000000016','p000000003','c000000004','s000000016',50,'2018-5-29 15:00:00','e000000004'),"+
                    "('r000000017','p000000003','c000000005','s000000017',50,'2018-5-27 15:00:00','e000000004'),"+
                    "('r000000018','p000000003','c000000006','s000000018',50,'2018-5-25 15:00:00','e000000004'),"+
                    "('r000000019','p000000004','c000000007','s000000019',100,'2018-5-15 15:00:00','e000000005'),"+
                    "('r000000020','p000000004','c000000008','s000000020',100,'2018-5-05 16:00:00','e000000005'),"+
                    "('r000000021','p000000004','c000000009','s000000021',100,'2018-5-01 16:00:00','e000000005'),"+
                    "('r000000022','p000000004','c000000010','s000000022',100,'2018-4-25 16:00:00','e000000005'),"+
                    "('r000000023','p000000007','c000000011','s000000023',200000,'2018-4-20 10:00:00','e000000001'),"+
                    "('r000000024','p000000007','c000000012','s000000024',200000,'2018-4-10 10:00:00','e000000001'),"+
                    "('r000000025','p000000007','c000000001','s000000025',200000,'2018-4-05 10:00:00','e000000001'),"+
                    "('r000000026','p000000009','c000000002','s000000026',60000,'2018-3-31 10:00:00','e000000002')";
            pstmt.addBatch(sql);
            pstmt.executeBatch();
            conn.commit();
            System.out.println("precord's data is inserted");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            pstmt.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
