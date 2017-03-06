/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package databasehomework;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author suyas
 */
public class Verification {

    public static boolean verify1NF(PrintWriter bw, TableSchema schema, Connection mConnection) {

        String tablename = schema.getTableName();
        ArrayList<String> columns = schema.getColumns();

//        System.out.println(columns.size());
        if (checkUnique(mConnection, tablename, columns) && checkNull(mConnection, tablename, columns)) {
            System.out.println("Table " + tablename + " verifies 1 NF.");
        } else {
            try {
                
                bw.println("\n" +tablename + "\t3NF = NO\tFAILED : 1NF");
                
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            return false;
        }

        return true;
    }

    public static boolean verify2NF(PrintWriter decFile, PrintWriter bw, TableSchema schema, Connection m_connection) {

        System.out.println("\nChecking 2 NF for table :" + schema.getTableName());
        String split[] = Utils.getCommaSeparatedPrimes(schema).toString().split(",");
        
        //if there is only one attribute in candidate key, 2 NF is already satisfied
        if(split.length==1){
            return true;
        }
        ArrayList<Dependency> fdList = Utils.generateDeps(schema, m_connection);
        ArrayList<TableSchema> decomp = null;
        StringBuffer fdString = new StringBuffer();
        StringBuffer decompName = new StringBuffer();

        if (fdList.isEmpty()) {
            return true;
        } else {
            for (int i = 0; i < fdList.size(); i++) {

                System.out.println("VIOLATING FD : " + fdList.get(i).showDep());
                fdString.append(fdList.get(i).showDep()).append(",");
                decomp = Utils.generateDecomp(schema, fdList);

            }
            try {
                bw.println("\n" + schema.getTableName() + "\t3NF = NO\tFAILED : 2NF\tREASON : " + fdString.toString());
                
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            
            decFile.println(schema.getTableName() + " decomposition:");
            for (int j = 0; j < decomp.size(); j++) {
                if(j!=0)
                    decompName.append(",");
                decomp.get(j).showTable();
                decompName.append(decomp.get(j).getTableName());
                decFile.println(decomp.get(j).toString());
            }
            decFile.println("Verification:");
            if(Utils.verifyDecomposition(schema, decomp, m_connection)){
                System.out.println("DECOMPOSITION FOR 2 NF IS CORRECT.");
                decFile.println(schema.getTableName() + "=join(" + decompName.toString() + ") ? YES");
                decFile.println();
            }
            else
                System.out.println("DECOMPOSITION FOR 2 NF IS INCORRECT.");
            return false;
        }
    }

    private static boolean checkUnique(Connection connection, String tablename, ArrayList<String> columns) {

//      call Utils.executeQuery from here for given column. Query is the one that checks duplicates.
//      if query returns 1, return false since duplicates are present.
        String mColumn = columns.get(0).replace("(k)", "");
        StringBuilder joinColumn = new StringBuilder();

        //looping to generate the comma separated string of key column names as : k1,k2,k3
        for (int i = 0; i < columns.size(); i++) {
            String mCol = columns.get(i);

            if (i > 0 && mCol.contains("(k)")) {
                joinColumn.append(",");
            }
            if (mCol.contains("(k)")) {
                joinColumn.append(mCol.replace("(k)", ""));
            }
        }
        System.out.println("JOIN : " + joinColumn);

        String query = "select " + joinColumn + " from " + tablename + " group by " + joinColumn + " having count(*)>1 ;";
        ResultSet result = Utils.executeQuery(connection, query);
        try {
            if (result.next()) {
                System.out.println("COLUMN COMBINATION " + joinColumn + " HAS DUPLICATES.");
                result.close();
                return false;
            }
            result.close();

        } catch (SQLException ex) {
            Logger.getLogger(Verification.class
                    .getName()).log(Level.SEVERE, null, ex);
        }
        return true;
    }

    private static boolean checkNull(Connection connection, String tablename, ArrayList<String> column) {

        for (int i = 0; i < column.size(); i++) {

            String mColumn = column.get(i);

            if (mColumn.contains("(k)")) {
                String query = "select count(*) from " + tablename + " where " + mColumn.replace("(k)", "") + " is NULL;";
                ResultSet result = Utils.executeQuery(connection, query);

                try {
                    while (result.next()) {
                        String r = result.getString("count");
                        if (Integer.parseInt(r) > 0) {
                            System.out.println("COLUMN " + mColumn + " HAS NULLS.");
                            return false;

                        }
                    }
                } catch (SQLException ex) {
                    Logger.getLogger(Verification.class
                            .getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
        return true;
    }

    static boolean verify3NF(PrintWriter decFile, PrintWriter bw, TableSchema mTable, Connection m_connection) {

        System.out.println("\nChecking 3 NF for " + mTable.getTableName());
        ArrayList<Dependency> fdList = Utils.generateNPtoNPdep(mTable, m_connection);
        StringBuffer fdString = new StringBuffer();
        StringBuffer decompName = new StringBuffer();

        if (fdList.isEmpty()) {
            bw.println("\n" + mTable.getTableName() + "\t3NF = YES");
            return true;
        } else {
            for (int i = 0; i < fdList.size(); i++) {
                System.out.println("3 NF VIOLATING FD : " + fdList.get(i).showDep());
                fdString.append(fdList.get(i).showDep()).append(",");
            }
            try {
                bw.println("\n" + mTable.getTableName() + "\t3NF = NO\tFAILED : 3NF\tREASON : " + fdString.toString());
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            ArrayList<TableSchema> decomp = Utils.generate3NFDecomp(mTable, fdList);
            
            decFile.println(mTable.getTableName() + " decomposition:");
            for (int i = 0; i < decomp.size(); i++) {
                
                if(i!=0)
                    decompName.append(",");
                decomp.get(i).showTable();
                decompName.append(decomp.get(i).getTableName());
                decFile.println(decomp.get(i).toString());
            }
            decFile.println("Verification:");
            if(Utils.verifyDecomposition(mTable, decomp, m_connection)){
                System.out.println("DECOMPOSITION FOR 3 NF IS CORRECT.");
                decFile.println(mTable.getTableName() + "=join(" + decompName.toString() + ") ? YES");
                decFile.println();
            }
            else{
                System.out.println("DECOMPOSITION FOR 3 NF IS INCORRECT.");
            }
            return false;
        }

    }
    
    static boolean verifyBCNF(PrintWriter bw, TableSchema mTable, Connection m_connection) {

        System.out.println("\nChecking BCNF for " + mTable.getTableName());
        ArrayList<Dependency> fdList = Utils.generateBCNFdep(mTable, m_connection);
        StringBuffer fdString = new StringBuffer();

        if (fdList.isEmpty()) {
            bw.println("\n" + mTable.getTableName() + "\t\t\tBCNF = YES");
            return true;
        } else {
            for (int i = 0; i < fdList.size(); i++) {
                System.out.println("BCNF VIOLATING FD : " + fdList.get(i).showDep());
                fdString.append(fdList.get(i).showDep()).append(",");
            }
            try {
                bw.println("\n" + mTable.getTableName() + "\t3NF = YES\tFAILED : BCNF\tREASON : " + fdString.toString());
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            return false;
        }

    }
    
}
