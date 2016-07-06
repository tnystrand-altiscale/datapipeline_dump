import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.DriverManager;

import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import java.util.Scanner;

import jline.console.ConsoleReader;
import jline.console.completer.StringsCompleter;
 
public class ExecuteHiveCommand {
    //private static final String DRIVERNAME = "org.apache.hive.jdbc.HiveDriver";
    private static final String DRIVERNAME = "com.jethrodata.JethroDriver";
    private static final Charset CHARSET = Charset.forName("US-ASCII");
    private Connection altiConnect;
    private Statement altiStatement;
    private String mode;
    private HashMap<String, String> hivesettings;
    private String colorPrompt;

    public String parseQueryFromFile(String filepath) throws IOException {
        Path path = Paths.get(filepath);
        String sql = "";
        try {
            List<String> lines = Files.readAllLines(path, CHARSET);
            System.out.println("Found file at: " + filepath);
            for (String line : lines) {
                sql += line + "\n";
            }
            // Remove last newline
            sql = sql.substring(0, sql.length()-2);
        }
        catch (IOException e) {
            throw e;
        }
        return sql;
    }

    public ExecuteHiveCommand() throws SQLException {
        System.setProperty("socksProxyHost", "localhost");
        System.setProperty("socksProxyPort", "8080");

        //this.altiConnect = DriverManager.getConnection("jdbc:hive2://localhost:10001/default", "tnystrand", "");
        this.altiConnect = DriverManager.getConnection("jdbc:JethroData://localhost:9112/pipeline");
        //this.altiConnect = DriverManager.getConnection("jdbc:JethroData://10.252.18.164:9112/pipeline", "tnystrand", "jethro");
        this.altiStatement = altiConnect.createStatement();
        this.mode = "query";
        this.colorPrompt = "\u001B[42m";

        this.hivesettings = new HashMap<String, String>();
        hivesettings.put("hive.execution.engine", "tez");
        hivesettings.put("hive.tez.java.opts", "-Xmx7096m");
        hivesettings.put("hive.tez.container.size", "8000");
        hivesettings.put("tez.cpu.vcores", "4");
        hivesettings.put("tez.session.client.timeout.secs", "10000000");
        hivesettings.put("tez.session.am.dag.submit.timeout.secs", "10000000");
        hivesettings.put("tez.queue.name", "research");
    }

    public void run() {
        HashSet<String> keywords = new HashSet<String>();
        keywords.add("cluster_metrics_prod_2");
        keywords.add("container_fact");
        StringsCompleter completer = new StringsCompleter(keywords);
        try {
            ConsoleReader reader = new ConsoleReader();
            reader.addCompleter(completer);
            reader.setPrompt(colorPrompt + mode + "> ");
            String query = "";
            PrintWriter out = new PrintWriter(reader.getOutput());

            // Keep looping till quit singal sent
            while (true) {
                reader.setPrompt(mode + "> ");
                query = reader.readLine();
                // Simple parsing
                if(query.equalsIgnoreCase("quit"))
                    break;
                else if (query.equalsIgnoreCase("file"))
                    mode="file";
                else if (query.equalsIgnoreCase("query"))
                    mode="query";
                else { 
                    String sql = "";
                    try {
                        if (mode.equals("query")) {
                            sql = query;
                            executeAndPrint(sql, out);
                        }
                        else if (mode.equals("file")) {
                            // Consider returning list of queries to run
                            sql = parseQueryFromFile(query);
                            executeAndPrint(sql, out);
                        }
                    }
                    catch (Exception e) {
                        if (!e.getMessage().equals("The query did not generate a result set!"))
                            System.out.println(e);
                    }
                }
                
                out.flush();
            }
        }
        catch (Throwable t) {
            t.printStackTrace();
        }
    }

    public void initialSettings() {
        PrintWriter out = new PrintWriter(System.out);
        for (Map.Entry<String, String> entry : hivesettings.entrySet()) {
            String sqlquery = "set " + entry.getKey() + "=" + entry.getValue();
            try {
                executeAndPrint(sqlquery, out);
            }
            catch (Exception e) {
                if (!e.getMessage().equals("The query did not generate a result set!"))
                    System.out.println(e);
            }
            out.flush();
        }
    }

    public void executeAndPrint(String sql, PrintWriter out) throws Exception {
        try {
            out.println("Running: " + sql);
            out.flush();
            
            // Run query
            ResultSet res = altiStatement.executeQuery(sql);

            // Get and print results of query
            ResultSetMetaData rsmd = res.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            out.println("Found " + columnsNumber + " columns");

            while (res.next()) {
                String row = "";
                int i;
                for (i = 1; i <= columnsNumber-1; i++) {
                    row += res.getString(i) + ", ";          
                }
                row += res.getString(i);
                out.println(row);
            }
            out.flush();
        }
        catch (Exception e) {
            throw e;
        }
    }

    /**
     * @param args
     * @throws SQLException
     */
    public static void main(String[] args) throws SQLException {
        // Make sure all classes are available
        try {
            Class.forName(DRIVERNAME);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        ExecuteHiveCommand hive = new ExecuteHiveCommand();
        hive.initialSettings();
        hive.run();
    }
}
