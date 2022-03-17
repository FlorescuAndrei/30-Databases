package com.andrei.test_connection;

import java.sql.*;

public class BasicCRUD {



    public static void main(String[] args) {

        //If schema is already created
//        String jdbcUrl = "jdbc:mysql://localhost:3306/test_schema?useSSL=false&serverTimezone=UTC";

        //If we have to create the schema
        String jdbcUrl = "jdbc:mysql://localhost:3306";
        String user = "root";
        String pass="root";




        try {
            //Connect to database
            System.out.println("Connecting to database: " +jdbcUrl);
            Connection conn= DriverManager.getConnection(jdbcUrl, user, pass);
            System.out.println("Connection successful");

            // Create schema
            String schemaName = "test_schema";
            String sqlCreateSchema = "CREATE DATABASE IF NOT EXISTS " + schemaName;
            Statement statement =  conn.createStatement();
            statement.execute(sqlCreateSchema);
            System.out.println("\nSchema created: " + schemaName);

            //Connect to schema
            System.out.println("Connecting to schema "  + schemaName);
            conn= DriverManager.getConnection(jdbcUrl + "/" + schemaName, user, pass);
            System.out.println("Connection successful");

            //Create table
            String sqlCreateTable = "CREATE TABLE IF NOT EXISTS contacts (name VARCHAR(50), phone INTEGER, email VARCHAR(50))";
            statement =  conn.createStatement();
            statement.execute(sqlCreateTable);
            System.out.println("\nTable created");

            //Insert
//            statement.execute("insert into contacts (name, phone, email) values ('Paul', 1234, 'paul@email')");
//            statement.execute("insert into contacts (name, phone, email) values ('Joe', 456, 'joe@email')");
//            statement.execute("insert into contacts (name, phone, email) values ('Jane', 789, 'jane@email')");
//            statement.execute("insert into contacts (name, phone, email) values ('Fido', 000, 'dog@email')");

            //Update        - important to add where, else all data will be replaced
//            statement.execute("update contacts set phone=555 where name='Paul'");

            //Delete
//            statement.execute("delete from contacts where name='Paul'");

            //Select
            // for more query we need more Statement instance.
            // We can reuse same statement if we have finished processing result set because the Statement object can have only one result set
            statement.execute("select * from contacts");
            ResultSet results = statement.getResultSet();
            while (results.next()){
                System.out.println(results.getString("name") + " " + results.getInt("phone") + " " +results.getString("email")) ;
            }
            results.close();



            System.out.println("Done!");
            //In MYSQL we do not have to commit changes before closing the connection, they will autocommit ,
            // but may be situation when we have to commit changes before closing the connection.
            statement.close();
            conn.close();

        } catch (SQLException e){
            System.out.println("Something went wrong: " + e.getMessage());
        }
    }
}
