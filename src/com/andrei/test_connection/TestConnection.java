package com.andrei.test_connection;

import java.sql.*;

public class TestConnection {

    public static void main(String[] args) {


        String jdbcUrl = "jdbc:mysql://localhost:3306";
       // String jdbcUrl = "jdbc:mysql://localhost:3306/test_schema?useSSL=false&serverTimezone=UTC";
        String user = "root";
        String pass="root";

        try {
            System.out.println("Connecting to database: " +jdbcUrl);
            Connection conn= DriverManager.getConnection(jdbcUrl, user, pass);
            System.out.println("Connection successful");

            conn.close();

        } catch (SQLException e){
            System.out.println("Something went wrong: " + e.getMessage());
        }
    }
}
