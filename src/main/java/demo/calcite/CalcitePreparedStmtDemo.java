package demo.calcite;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.RelRunner;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class CalcitePreparedStmtDemo {

    public static void main(String[] args) throws SQLException {
        //connection
        CalciteConnection connection = (CalciteConnection) DriverManager.getConnection("jdbc:calcite:");

        //root schema
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(true);

        //our schema
        Schema schema = new ReflectiveSchema(new HrSchema());

        //add our schema to root schema
        rootSchema.add("hr", schema);

        //add function schema to root schema
        SchemaPlus rootSchemaPlus = rootSchema.plus();
        rootSchemaPlus.add("mymethod", ScalarFunctionImpl.create(HrSchema.class, "mymethod"));

        //create prepared statement
        CalcitePrepareImpl prepareImpl = new CalcitePrepareImpl();
        Object dummySchema = null;
        try {
            dummySchema = HrSchema.class.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException("Couldn't make dummySchema", e);
        }
        AdapterContext ctx = new AdapterContext(rootSchemaPlus, dummySchema);

        CalcitePrepare.CalciteSignature<Object> preppedQuery =
                prepareImpl.prepareSql(ctx, CalcitePrepare.Query.of("select hr.emp.deptno, mymethod() from hr.emp"), Object[].class, 1);
        ;

        //execute query: here the calcite will use linq4j API to process data(group by and join)
        HrSchema hrRaw = new HrSchema();
        Enumerator<Object> e = preppedQuery.enumerable(ctx.getDataContext(hrRaw)).enumerator();
        while (e.moveNext()) {
            Object[] cur = (Object[]) e.current();
            System.out.printf("%s\t%s%n", cur[0], cur[1]);
        }

        //close connection
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

        public static String mymethod() {
            return "Hello Hikmat";
        }
    }

    private static class AdapterContext implements CalcitePrepare.Context {
        private final SchemaPlus rootSchemaPlus;
        private final Object dummySchema;
        private final CalciteConnectionConfigImpl config;

        public AdapterContext(SchemaPlus rootSchemaPlus, Object dummySchema) {
            this.rootSchemaPlus = rootSchemaPlus;
            this.dummySchema = dummySchema;
            Properties properties = new Properties();
            properties.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
            properties.put(CalciteConnectionProperty.FUN.camelName(), "postgresql");
            properties.put(CalciteConnectionProperty.CONFORMANCE.camelName(), SqlConformanceEnum.LENIENT.name());
            properties.put(CalciteConnectionProperty.LEX.camelName(), Lex.ORACLE.name());
            this.config = new CalciteConnectionConfigImpl(properties);

        }

        @Override
        public JavaTypeFactory getTypeFactory() {
            return new JavaTypeFactoryImpl();
        }

        @Override
        public CalciteSchema getRootSchema() {
            return this.rootSchemaPlus.unwrap(CalciteSchema.class);
        }

        @Override
        public CalciteSchema getMutableRootSchema() {
            return null;
        }

        @Override
        public List<String> getDefaultSchemaPath() {
            return Collections.singletonList("hr");
        }

        @Override
        public CalciteConnectionConfig config() {
            return config;
        }

        @Override
        public CalcitePrepare.SparkHandler spark() {
            return CalcitePrepare.Dummy.getSparkHandler(this.config.spark());
        }

        @Override
        public DataContext getDataContext() {
            return new CustomDataContext(this, dummySchema);
        }

        public DataContext getDataContext(Object rawSchema) {
            return new CustomDataContext(this, rawSchema);
        }

        @Override
        public List<String> getObjectPath() {
            return null;
        }

        @Override
        public RelRunner getRelRunner() {
            return null;
        }
    }


    public static class CustomDataContext implements DataContext {
        private final AdapterContext adapterContext;
        private final Object rawSchema;

        public CustomDataContext(AdapterContext context, Object rawSchema) {
            adapterContext = context;
            this.rawSchema = rawSchema;
        }

        @Override
        public SchemaPlus getRootSchema() {
            return adapterContext.rootSchemaPlus;
        }

        @Override
        public JavaTypeFactory getTypeFactory() {
            return adapterContext.getTypeFactory();
        }

        @Override
        public QueryProvider getQueryProvider() {
            return null;
        }

        @Override
        public Object get(String s) {
            if ("raw".equals(s))
                return rawSchema;
            return null;
        }

        public <T> T unwrap(Class<T> clz) {
            return null;
        }
    }
}
