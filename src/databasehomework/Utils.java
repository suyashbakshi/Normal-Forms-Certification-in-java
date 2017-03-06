/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package databasehomework;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author suyas
 */
public class Utils {

    static final String reportFilePath = "";
    static final String schemaFile = "";
    static final String decFilePath = "";
    static final String sqlFilePath = "";
    static ArrayList<String> queries = new ArrayList();

    public static ResultSet executeQuery(Connection connection, String query) {

        try {
            System.out.println("QUERY FIRED : " + query);
            queries.add(query);
            PreparedStatement ps = connection.prepareStatement(query);
            return ps.executeQuery();

        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }

        return null;
    }

    public static boolean execute(Connection connection, String query) {

        try {
            System.out.println("QUERY FIRED : " + query);
            queries.add(query);
            PreparedStatement ps = connection.prepareStatement(query);
            return ps.execute();

        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }

        return false;
    }

    public static ArrayList<Dependency> generateDeps(TableSchema schema, Connection connection) {

        ArrayList<String> columns = schema.getColumns();
        String tablename = schema.getTableName();
        StringBuffer primes = new StringBuffer();
        ArrayList<String> nonPrime = new ArrayList();
        ArrayList<Dependency> deps = new ArrayList();
        String s = null;

//        loop to separate prime and non-prime attributes
        for (int i = 0; i < columns.size(); i++) {
            s = columns.get(i);
            if (!s.contains("(k)")) {
                nonPrime.add(s);
            }
        }
        primes = Utils.getCommaSeparatedPrimes(schema);

        System.out.println("PRIMES IS " + primes);

        //get prime attributes combination as: 
        //k1 
        //k2 
        //k3
        //k1,k2
        //k1,k3
        //k2,k3
        //k1,k2,k3
        ArrayList<String> primeCombinations = getKeyCombinations(primes);

        String mPrime = null;
        String mNprime = null;

        for (int i = 0; i < primeCombinations.size() - 1; i++) {

            mPrime = primeCombinations.get(i);
            for (int j = 0; j < nonPrime.size(); j++) {

                mNprime = nonPrime.get(j);
//                String viewOneQuery = "create view one as select " + mPrime + "," + mNprime + " from " + tablename + ";";
//                String viewTwoQuery = "create view two as select " + mPrime + "," + mNprime + " from " + tablename + ";";
//                Utils.execute(connection, viewOneQuery);
//                Utils.execute(connection, viewTwoQuery);

                //get view combinations as
                //for k1        the statement as one.k1=two.k1
                //for k1,k2 get the statement as one.k1=two.k1 and one.k2=two.k2 and so on...
                String viewCombination = getViewCombination(mPrime);

                String query = "select count(*) from " + tablename + " as one," + tablename + " as two where " + viewCombination + " one." + mNprime + "<>two." + mNprime + ";";

                ResultSet rs = Utils.executeQuery(connection, query);

                try {
                    rs.next();
                    if (Integer.parseInt(rs.getString("count")) > 0) {
                        System.out.println("No FD between " + mPrime + " and " + mNprime);
                    } else {
                        deps.add(new Dependency(mPrime, mNprime));
                    }
                } catch (SQLException ex) {
                    Logger.getLogger(Utils.class.getName()).log(Level.SEVERE, null, ex);
                }
//                String dropView = "drop view one;";
//                String dropViewTwo = "drop view two;";
//                Utils.execute(connection, dropView);
//                Utils.execute(connection, dropViewTwo);
            }

        }
        return Utils.removeFullyFd(primes, deps);
    }

    public static ArrayList<String> getKeyCombinations(StringBuffer primes) {

        //This function returns a list containing all possible subsets of key columns
        //suppose candidate key is k1,k2,k3 so list returned will have:
        //{k1, k2, k3, k1k2, k2k3, k1k3, k1k2k3}
        //this helps in finding all the partial dependencies for 2NF.
        String split[] = primes.toString().split(",");
        ArrayList<String> combinations = new ArrayList();

        for (int i = 0; i < (1 << split.length); i++) {
            // Print current subset
            String s = "";
            for (int j = 0; j < split.length; j++) {
                if ((i & (1 << j)) > 0) {
//                    System.out.print(split[j] + "-"+j);
                    s += "," + split[j];
                }
            }
            if (!s.equals("")) {
                combinations.add(s.replaceFirst(",", ""));

            }
        }
        return combinations;
    }

    private static String getViewCombination(String prime) {

        String split[] = prime.split(",");
        String s = "";
        for (int i = 0; i < split.length; i++) {
            s += "one." + split[i] + "=two." + split[i] + " and ";
        }
        return s;

    }

    private static ArrayList<Dependency> removeFullyFd(StringBuffer primes, ArrayList<Dependency> deps) {

        //remove all the fd's that have the whole candidate key on the left. Since they do not violate 2 NF.
        //keep only those fd's that have partial candidate key on the left.
        for (int i = 0; i < deps.size(); i++) {

            if (deps.get(i).getLeft().equalsIgnoreCase(primes.toString())) {
                deps.remove(i);
                i--;
            }
        }
        return deps;
    }

    public static StringBuffer getCommaSeparatedPrimes(TableSchema schema) {

        //returns primes as a string k1,k2,k3
        ArrayList<String> columns = schema.getColumns();
        StringBuffer primes = new StringBuffer();
        String s;
        for (int i = 0; i < columns.size(); i++) {
            s = columns.get(i);
            if (s.contains("(k)")) {
                primes.append(s.replace("(k)", "")).append(",");
            }
        }
        primes.deleteCharAt(primes.lastIndexOf(","));
        return primes;
    }

    static ArrayList<TableSchema> generateDecomp(TableSchema schema, ArrayList<Dependency> fdList) {

        ArrayList<String> primeComb = Utils.getKeyCombinations(Utils.getCommaSeparatedPrimes(schema));
        ArrayList<TableSchema> decompositions = new ArrayList();
        ArrayList<String> primes = Utils.getPrimes(schema);

        //flags for creating decomposition for all nonPrimes that do not show up on right side of any FD
        //and hence are fully functional dependent on candidate key and should stay in original table
        ArrayList<String> nonPrimes = Utils.getNonPrimes(schema);
        boolean flags[] = new boolean[nonPrimes.size()];

        for (int i = 0; i < primeComb.size() - 1; i++) {

            String prime = primeComb.get(i);
            ArrayList<String> dCol = new ArrayList();
            dCol.add(prime);
//            System.out.println(prime);
            for (int j = 0; j < fdList.size(); j++) {
//                System.out.println(fdList.get(j).showDep());
                if (fdList.get(j).getLeft().equals(prime)) {
                    dCol.add(fdList.get(j).getRight());

                    //mark the flag for non Prime as false, which appears on right side in a partial dependency
                    //since it cannot be a part of main table
                    int idx = nonPrimes.indexOf(fdList.get(j).getRight());
                    flags[idx] = true;

                }
            }
            if (!(dCol.size() == 1)) {
                decompositions.add(new TableSchema(schema.getTableName() + (i + 1), dCol));
            }
        }

        TableSchema firstDec = new TableSchema();
        firstDec.setTableName(schema.getTableName() + 0);
        firstDec.addColumn("i");

        for (int i = 0; i < primes.size(); i++) {
            firstDec.addColumn(primes.get(i));
        }

        for (int i = 0; i < nonPrimes.size(); i++) {
            if (!flags[i]) {
                firstDec.addColumn(nonPrimes.get(i));
            }
        }
        decompositions.add(firstDec);

        return decompositions;
    }

    public static ArrayList<String> getNonPrimes(TableSchema schema) {
        ArrayList<String> nonPrimes = new ArrayList();

        for (int i = 0; i < schema.getColumns().size(); i++) {
            if (!(schema.getColumns().get(i).toString().contains("(k)"))) {
                nonPrimes.add(schema.getColumns().get(i).toString());
            }
        }
        return nonPrimes;
    }

    public static ArrayList<String> getPrimes(TableSchema schema) {
        ArrayList<String> primes = new ArrayList();

        for (int i = 0; i < schema.getColumns().size(); i++) {
            if (schema.getColumns().get(i).toString().contains("(k)")) {
                primes.add(schema.getColumns().get(i).toString().replace("(k)", ""));
            }
        }
        return primes;
    }

    static ArrayList<Dependency> generateNPtoNPdep(TableSchema mTable, Connection m_connection) {

        //this function returns a list of all the non prime to non prime dependencies for 3 NF verification.
        ArrayList<String> nonPrimes = Utils.getNonPrimes(mTable);
        ArrayList<Dependency> NPtoNPdep = new ArrayList();

        String nPrime = null;
        String nPrime1 = null;

        for (int i = 0; i < nonPrimes.size(); i++) {

            nPrime = nonPrimes.get(i);

            for (int j = i + 1; j < nonPrimes.size(); j++) {

                if (i != j) {
                    nPrime1 = nonPrimes.get(j);
//                    String one = "create view one as select " + nPrime + "," + nPrime1 + " from " + mTable.getTableName() + ";";
//                    String two = "create view two as select " + nPrime + "," + nPrime1 + " from " + mTable.getTableName() + ";";
//                    Utils.execute(m_connection, one);
//                    Utils.execute(m_connection, two);

                    String query = "select count(*) from " + mTable.getTableName() + "as one," + mTable.getTableName()+ " as two where " + Utils.getViewCombination(nPrime) + " one." + nPrime1 + "<>two." + nPrime1 + ";";
                    ResultSet rs = Utils.executeQuery(m_connection, query);

//                    String dropOne = "drop view one;";
//                    String dropTwo = "drop view two;";
//                    Utils.execute(m_connection, dropOne);
//                    Utils.execute(m_connection, dropTwo);

                    try {
                        rs.next();
                        if (Integer.parseInt(rs.getString("count")) > 0) {
                            System.out.println("No FD between " + nPrime + " and " + nPrime1);
                        } else {
                            NPtoNPdep.add(new Dependency(nPrime, nPrime1));
                        }
                    } catch (SQLException ex) {
                        Logger.getLogger(Utils.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            }

        }
        return NPtoNPdep;
    }

    static ArrayList<TableSchema> generate3NFDecomp(TableSchema schema, ArrayList<Dependency> deps) {

        ArrayList<String> nonPrimes = Utils.getNonPrimes(schema);
        ArrayList<String> primes = Utils.getPrimes(schema);
        ArrayList<TableSchema> decomp = new ArrayList();
        boolean flag[] = new boolean[nonPrimes.size()];

        for (int i = 0; i < nonPrimes.size(); i++) {

            String mNprime = nonPrimes.get(i);
            ArrayList<String> dCol = new ArrayList();
            dCol.add(mNprime);
            for (int j = 0; j < deps.size(); j++) {

                if (deps.get(j).getLeft().equals(mNprime)) {

                    dCol.add(deps.get(j).getRight());
                    int idx = nonPrimes.indexOf(deps.get(j).getRight());
                    flag[idx] = true;
                }

            }
            if (dCol.size() != 1) {
                decomp.add(new TableSchema(schema.getTableName() + (i + 1), dCol));
            }
        }
        TableSchema firstDecomp = new TableSchema();
        firstDecomp.setTableName(schema.getTableName() + 0);
        firstDecomp.addColumn("i");

        for (int i = 0; i < primes.size(); i++) {
            firstDecomp.addColumn(primes.get(i));
        }

        for (int i = 0; i < nonPrimes.size(); i++) {
            if (!flag[i]) {
                //System.out.println(nonPrimes.get(i));
                firstDecomp.addColumn(nonPrimes.get(i));
            }
        }
        decomp.add(firstDecomp);

        return decomp;
    }

    static boolean verifyDecomposition(TableSchema schema, ArrayList<TableSchema> decomp, Connection connection) {

        //To verify decompositions, we first need to create the new normalized tables in database.
        for (int i = 0; i < decomp.size(); i++) {
            Utils.createDecompTable(schema, decomp.get(i), connection);
        }
        String joinQuery = "select count(*) from (select DISTINCT " + Utils.getCommaSeparatedPrimes(schema) + " from (select * from " + Utils.getJoinCombination(decomp) + ") as x GROUP BY " + Utils.getCommaSeparatedPrimes(schema) + ") as y;";
        String query = "select count(*) from (select distinct " + Utils.getCommaSeparatedPrimes(schema) + " from " + schema.getTableName() + ") as z;";
        ResultSet joinRs = Utils.executeQuery(connection, joinQuery);
        ResultSet rs = Utils.executeQuery(connection, query);

        try {
            joinRs.next();
            rs.next();

            if (Integer.parseInt(joinRs.getString("count")) == Integer.parseInt(rs.getString("count"))) {
                return true;
            }

        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return false;
    }

    private static void createDecompTable(TableSchema schema, TableSchema decomp, Connection connection) {

        StringBuffer col = new StringBuffer();
        for (int i = 0; i < decomp.getColumns().size(); i++) {
            col.append(decomp.getColumns().get(i)).append(",");
        }
        col.deleteCharAt(col.lastIndexOf(","));
        String createTable = "create table team04schema." + decomp.getTableName() + " as (select DISTINCT " + col.toString() + " from " + schema.getTableName() + ");";
//        System.out.println("CREATE STRING : " + createTable);
        Utils.execute(connection, createTable);
    }

    private static String getJoinCombination(ArrayList<TableSchema> decomp) {

        StringBuffer joinComb = new StringBuffer();
        for (int i = 0; i < decomp.size(); i++) {
            if (i != 0) {
                joinComb.append(" natural join ");
            }
            joinComb.append("team04schema.").append(decomp.get(i).getTableName());
        }
        return joinComb.toString();
    }
    
    static ArrayList<Dependency> generateBCNFdep(TableSchema mTable, Connection m_connection) {

        //this function returns a list of all the non prime to non prime dependencies for 3 NF verification.
        ArrayList<String> nonPrimes = Utils.getNonPrimes(mTable);
        ArrayList<String> primes = Utils.getPrimes(mTable);
        ArrayList<Dependency> BCNFdep = new ArrayList();

        String prime = null;
        String nPrime = null;

        for (int i = 0; i < nonPrimes.size(); i++) {

            nPrime = nonPrimes.get(i);

            for (int j = i + 1; j < nonPrimes.size(); j++) {

                if (i != j) {
                    prime = primes.get(j);
//                    String one = "create view one as select " + nPrime + "," + nPrime1 + " from " + mTable.getTableName() + ";";
//                    String two = "create view two as select " + nPrime + "," + nPrime1 + " from " + mTable.getTableName() + ";";
//                    Utils.execute(m_connection, one);
//                    Utils.execute(m_connection, two);

                    String query = "select count(*) from " + mTable.getTableName() + "as one," + mTable.getTableName()+ " as two where one." + nPrime + "=two." + nPrime + "and one." + prime + "<>two." + prime + ";";
                    ResultSet rs = Utils.executeQuery(m_connection, query);

//                    String dropOne = "drop view one;";
//                    String dropTwo = "drop view two;";
//                    Utils.execute(m_connection, dropOne);
//                    Utils.execute(m_connection, dropTwo);

                    try {
                        rs.next();
                        if (Integer.parseInt(rs.getString("count")) > 0) {
                            System.out.println("No FD between " + nPrime + " and " + prime);
                        } else {
                            BCNFdep.add(new Dependency(nPrime, prime));
                        }
                    } catch (SQLException ex) {
                        Logger.getLogger(Utils.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            }

        }
        return BCNFdep;
    }

}
