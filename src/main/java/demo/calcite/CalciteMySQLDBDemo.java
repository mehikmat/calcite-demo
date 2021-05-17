package demo.calcite;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.commons.dbcp.BasicDataSource;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class CalciteMySQLDBDemo {

    public static void main(String[] args) throws SQLException {
        Properties info = new Properties();
        info.setProperty("lex", Lex.JAVA.toString());

        //connection
        CalciteConnection connection = (CalciteConnection) DriverManager.getConnection("jdbc:calcite:", info);

        //root schema
        SchemaPlus rootSchema = connection.getRootSchema();

        //our db schema
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setUrl("jdbc:mysql://localhost/crawler");
        dataSource.setUsername("root");
        dataSource.setPassword("87654321");
        Schema schema = JdbcSchema.create(rootSchema, "hr", dataSource, null, "crawler");

        //add our schema to root schema
        rootSchema.add("hr", schema);

        //create statement
        Statement statement = connection.createStatement();

        //execute query: here the calcite will use linq4j API to process data(group by and join)
        ResultSet resultSet = statement.executeQuery(
                "select * from hr.test");

        //print output rows
        System.out.println(resultSet.getMetaData().getColumnName(1) + "," + resultSet.getMetaData().getColumnName(2));
        System.out.println("----------,----------");
        while (resultSet.next()) {
            System.out.println(resultSet.getString("a") + "," + resultSet.getString("b"));
        }

        //close connection
        resultSet.close();
        statement.close();
        connection.close();
    }
}
