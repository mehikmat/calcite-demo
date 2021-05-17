Apache Calcite
--------------
- Apache Calcite is a dynamic data management framework.
- It is basically a SQL Parser, Validator, and Optimizer.
- It contains many of the pieces that comprise a typical database management system.
- It omits some key functions: storage of data, algorithms to process data, and a repository for storing metadata.
- This makes it an excellent choice for mediating between applications and one or more data storage locations and data processing engines.
- It is also a perfect foundation for building a database: just add data.

- It uses linq4j library for processing(eg. group by, filter, etc) in-memory data stored in java objects.
- It can send data processing to databases.

- To add a custom data source, you need to write an adapter that tells Calcite what collections in the data source it should consider "tables".