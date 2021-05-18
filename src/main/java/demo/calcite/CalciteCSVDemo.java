package demo.calcite;

import org.apache.calcite.adapter.csv.CsvSchema;
import org.apache.calcite.adapter.csv.CsvTable;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import java.io.File;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class CalciteCSVDemo {

    public static void main(String[] args) throws SQLException {
        //connection
        CalciteConnection connection = (CalciteConnection) DriverManager.getConnection("jdbc:calcite:");

        //root schema
        SchemaPlus rootSchema = connection.getRootSchema();

        /*CSV Schema*/
        File csvDir = new File("data/csv");
        rootSchema.add("CSV", new CsvSchema(csvDir, CsvTable.Flavor.SCANNABLE));

        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select * from CSV.EMP");

        while (resultSet.next()) {
            System.out.println(resultSet.getString(1) + "," + resultSet.getString(2));
        }

        //close connection
        resultSet.close();
        statement.close();
        connection.close();
    }
}
