package demo.calcite;

import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class CalciteInMemoryDBDemo {

    public static void main(String[] args) throws SQLException {
        Properties info = new Properties();
        info.setProperty("lex", Lex.JAVA.toString());

        //connection
        CalciteConnection connection = (CalciteConnection) DriverManager.getConnection("jdbc:calcite:", info);

        //root schema
        SchemaPlus rootSchema = connection.getRootSchema();

        //our schema
        Schema schema = new ReflectiveSchema(new HrSchema());

        //add our schema to root schema
        rootSchema.add("hr", schema);

        //create statement
        Statement statement = connection.createStatement();

        //execute query: here the calcite will use linq4j API to process data(group by and join)
        ResultSet resultSet = statement.executeQuery(
                "select d.deptno, min(e.empid) as emp\n"
                        + "from hr.emp as e\n"
                        + "join hr.dept as d\n"
                        + "  on e.deptno = d.deptno\n"
                        + "group by d.deptno\n"
                        + "having count(*) > 0");

        //print output rows
        System.out.println(resultSet.getMetaData().getColumnName(1) + "," + resultSet.getMetaData().getColumnName(2));
        System.out.println("----------,----------");
        while (resultSet.next()) {
            System.out.println(resultSet.getString("deptno") + "," + resultSet.getString("emp"));
        }

        //close connection
        resultSet.close();
        statement.close();
        connection.close();
    }

    //emp table row
    public static class Employee {
        //columns
        public String empid;
        public String deptno;

        public Employee(String empName, String deptno) {
            this.empid = empName;
            this.deptno = deptno;
        }
    }

    //dept table row
    public static class Department {
        //columns
        public String deptno;

        public Department(String deptName) {
            this.deptno = deptName;
        }
    }

    //database
    public static class HrSchema {
        //emp table
        public Employee[] emp = new Employee[]{new Employee("emp_hikmat", "dept_hikmat"), new Employee("emp_ram", "dept_ram")};

        //dept table
        public Department[] dept = new Department[]{new Department("dept_hikmat"), new Department("dept_ram")};
    }
}
