import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.DriverManager;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import java.util.Scanner;
 
public class ExecuteHiveCommand {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
 
    /**
     * @param args
     * @throws SQLException
     */
    public static void main(String[] args) throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }

        Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "tnystrand", "");
        Charset charset = Charset.forName("US-ASCII");
        Statement stmt = con.createStatement();

        String mode = "file";
        Scanner scanner=new Scanner(System.in);
        while (true) {
            if (mode.equals("file"))
                System.out.print("<file-mode>: ");
            else if (mode.equals("query"))
                System.out.print("<query-mode>: ");

            //String query = scanner.nextLine();
            String query = System.console().readLine();
            if(query.equals("quit"))
                break;
            else if (query.equals("file"))
                mode="file";
            else if (query.equals("query"))
                mode="query";
            else { 
                String sql = "";
                if (mode.equals("query"))
                    sql = query;
                else if (mode.equals("file")) {
                    Path path = Paths.get(query);
                    try {
                        List<String> lines = Files.readAllLines(path, charset);
                        System.out.println("Found file at: " + query);
                        for (String line : lines) {
                            sql += line + "\n";
                        }
                        // Remove last newline
                        sql = sql.substring(0, sql.length()-2);
                    } catch (IOException e) {
                        System.out.println(e);
                    } 
                }
                
                System.out.println("Running: " + sql);
                try {
                    ResultSet res = stmt.executeQuery(sql);

                    ResultSetMetaData rsmd = res.getMetaData();
                    int columnsNumber = rsmd.getColumnCount();
                    System.out.println("Found " + columnsNumber + " columns");

                    while (res.next()) {
                        String row = "";
                        int i;
                        for (i = 1; i <= columnsNumber-1; i++) {
                            row += res.getString(i) + ", ";          
                        }
                        row += res.getString(i);
                        System.out.println(row);
                    }
                }
                catch (Exception e) {
                    System.out.println(e);
                }
            }
        }
    }
}
