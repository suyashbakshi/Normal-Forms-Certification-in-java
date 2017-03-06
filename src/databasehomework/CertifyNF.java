/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package databasehomework;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.*;
import java.util.Properties;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;

/**
 *
 * @author suyas
 */
public class CertifyNF {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {

        try{
            Class.forName("com.vertica.jdbc.Driver");
        }catch(ClassNotFoundException ex){
            System.out.println("JDBC Driver Class Not Found.");
        }
        Connection m_connection = DriverManager.getConnection("jdbc:vertica://<ip/databasename>","user","password");
        String schemaFile = Utils.schemaFile;

        String split[] = args[0].split("=");
        BufferedReader reader = new BufferedReader(new FileReader(schemaFile));
        PrintWriter bw = new PrintWriter(new BufferedWriter(new FileWriter(Utils.reportFilePath)));
        PrintWriter decFile = new PrintWriter(new BufferedWriter(new FileWriter(Utils.decFilePath)));

        String schema;

        while ((schema = reader.readLine()) != null) {

            //Loop for each given table schema in the input file.
            TableSchema mTable = new TableSchema();
            System.out.println("\nInput Schema : " + schema);

            //Parse each line to retrieve tablename and columns
            int idx = schema.indexOf('(');
            String tablename = schema.substring(0, idx);
            System.out.println("Table Name : " + tablename);
            mTable.setTableName(tablename);

            String sub = schema.substring(++idx, schema.length() - 1);
            String[] columns = sub.split(",");
            System.out.print("Columns : ");
            for (String s : columns) {
                System.out.print(s + " ");
                mTable.addColumn(s);
            }

            
            System.out.println();
            if (Verification.verify1NF(bw, mTable, m_connection)) {
                System.out.println(tablename + " satisfies 1 Normal Form");

                //if 1 NF is satisfied, then check for 2 NF.
                if (Verification.verify2NF(decFile, bw, mTable, m_connection)) {
                    //verify2NF() returns true if table satisfies 2NF.
                    System.out.println(tablename + " satisfies 2 Normal Form");
                    
                    if(Verification.verify3NF(decFile, bw, mTable, m_connection)){
                        System.out.println(tablename + " satisfies 3 Normal Form");
                        
                        if(Verification.verifyBCNF(bw, mTable, m_connection)){
                            System.out.println(tablename + " saatisfies BCNF");
                        }
                        else{
                            System.out.println("BCNF Failed for " + tablename);
                        }
                    }
                    else{
                        System.out.println("3 NF Failed for " + tablename + ". Decompositions Generated.");
                    }
                }
                else
                    System.out.println("2 NF Failed for " + tablename + ". Decompositions Generated.");

            } else {
                System.out.println("1 NF failed for table " + tablename + ". Cannot proceed with further validations.");
            }
            
        }
        
        reader.close();
        m_connection.close();
        bw.close();
        decFile.close();
        
        PrintWriter sqlWriter = new PrintWriter(new BufferedWriter(new FileWriter(Utils.sqlFilePath)));
        for (int i = 0; i < Utils.queries.size(); i++) {
            sqlWriter.println(Utils.queries.get(i));
        }
        sqlWriter.close();
    }

}

class TableSchema {

    private String tablename;
    private ArrayList<String> columns = new ArrayList();
    
    public TableSchema(){
    }
    
    public TableSchema(String tablename,ArrayList<String> columns){
        this.tablename = tablename;
        this.columns = columns;
    }

    public void setTableName(String name) {
        this.tablename = name;
    }

    public void addColumn(String columnName) {
        this.columns.add(columnName);
    }

    public String getTableName() {
        return this.tablename;
    }

    public ArrayList getColumns() {
        return this.columns;
    }
    
    public void showTable(){
        System.out.print(this.tablename + "( " );
        for (int i = 0; i < this.columns.size(); i++) {
            System.out.print(this.columns.get(i) + " ");
        }
        System.out.print(")");
        System.out.println();
    }
    
    @Override
    public String toString(){
        StringBuffer str = new StringBuffer();
        str.append(this.tablename).append("(");
        for (int i = 0; i < this.columns.size(); i++) {
            str.append(this.columns.get(i)).append(" ");
        }
        str.append(")");
        return str.toString();
    }
}
