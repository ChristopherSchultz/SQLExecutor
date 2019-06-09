package net.christopherschultz.sqlexecutor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Formatter;
import java.util.Properties;

/**
 * Executes a SQL script one statement at a time. See the "usage" method for
 * all available options.
 *
 * Copyright (C) 2013 - 2018 Christopher Schultz
 *
 * @author Christopher Schultz
 */
public class SQLExecutor
{
    // ANSI escape sequence to clear the entire screen and go to [1,1]
    static final byte[] CLS = new byte[] {
        (byte)0x001b, '[', '2', 'J', // clear
        (byte)0x001b, '[', 'H'       // home
    };

    static final String NEWLINE = System.getProperty("line.separator");

    public static void main(String[] args)
    {
        String username = null;
        String password = null;
        boolean readSecurePassword = false;
        String jdbcUrl = null;
        String jdbcDriverClassName = null;
        String driverJarFile = null;
        String script = null;
        String encoding = System.getProperty("file.encoding", "UTF-8");
        boolean clearScreenBeforeStatement = false;
        // Number of statements to skip at the beginning of the script.
        int skipLines = 0;

        int i;
        for(i=0; i<args.length; )
        {
            final String arg = args[i++];

            if("--username".equals(arg))
                username = args[i++];
            else if("--password".equals(arg))
                password = args[i++];
            else if("--askpass".equals(arg))
                readSecurePassword = true;
            else if("--url".equals(arg))
                jdbcUrl = args[i++];
            else if("--driver".equals(arg))
                jdbcDriverClassName = args[i++];
            else if("--driverjar".equals(arg))
                driverJarFile = args[i++];
            else if("--script".equals(arg))
                script = args[i++];
            else if("--encoding".equals(arg))
                encoding = args[i++];
            else if("--skip".equals(arg))
                skipLines = Integer.parseInt(args[i++]);
            else if("--clear".equals(arg))
                clearScreenBeforeStatement = true;
            else if("--help".equals(arg) || "-h".equals(arg))
            {
                usage();
                System.exit(0);
            }
            else
                script = arg;
        }

        if(null == jdbcDriverClassName
           || null == script
           || null == jdbcUrl)
        {
            usage();
            System.exit(1);
        }

        if(readSecurePassword)
            password = new String(System.console().readPassword("Enter password: "));

        // Load JDBC driver
        Driver driver = null;
        try
        {
            ClassLoader cl = Thread.currentThread().getContextClassLoader();

            if(null != driverJarFile) {
                URL url = new URL("file:" + driverJarFile);

                cl = new URLClassLoader(new URL[] { url }, cl);
            }

            Class<?> driverClass = Class.forName(jdbcDriverClassName, true, cl);
            if(!Driver.class.isAssignableFrom(driverClass))
                throw new IllegalArgumentException("Driver class " + jdbcDriverClassName + " is not a JDBC driver.");

            @SuppressWarnings("unchecked") // This is indeed checked, above
            Class<Driver> specClass = (Class<Driver>)driverClass;

            driver = specClass.getConstructor().newInstance();
        }
        catch (Exception e)
        {
            System.err.println("Failed to load JDBC driver");
            e.printStackTrace();
            System.exit(1);
        }

        // Connect to database
        Connection conn = null;
        try
        {
            Properties props = null;

            if(null != username || null != password) {
                props = new Properties();
                if(null != username)
                    props.put("user", username);
                if(null != password)
                    props.put("password", password);
            }

            conn = driver.connect(jdbcUrl, props);
        }
        catch (SQLException sqle)
        {
            System.err.println("Could not connect to database");
            sqle.printStackTrace();
            System.exit(1);
        }

        BufferedReader in = null;
        BufferedReader cmd = null;

        // The current line number
        int lineNumber = 1;

        try
        {
            in = new BufferedReader(new InputStreamReader(new FileInputStream(script), encoding));
            cmd = new BufferedReader(new InputStreamReader(System.in, System.getProperty("file.encoding")));

            System.out.print("Executing script '");
            System.out.print(script);
            System.out.println('\'');

            StringBuilder query = null;
            String line = "";

            while(skipLines-- > 0)
            {
                in.readLine();
                ++lineNumber;
            }

            // Set to true when it's time to totally stop: 'q' command or EOF
            boolean stop = false;

            // Set to "true" to advance to the next statement in the file.
            // This will be set to false if a statement fails for some reason,
            // allowing the user to make an external adjustment if necessary
            // and re-run the same statement.
            boolean readNext = true;

            // The script's line number where the statement begins
            long statementStartLine = 0;

            boolean complete = false;
            boolean goUntilError = false;
            boolean firstQuery = true;

            while(!stop)
            {
                if(readNext)
                {
                    query = new StringBuilder();
                    statementStartLine = lineNumber;
                }

                // Might have to read more script than we know about.
                // This allows a user to say "this statement isn't over yet,
                // read to the next ; character.
                boolean more = true;

                while(more)
                {
                    // System.out.println("At top of 'more' loop, readNext=" + readNext + ", more=" + more + ", complete=" + complete);

                    if(!complete && null == line)
                        System.out.println("!! Reached end-of-script");

                    // Usually don't have to read more script
                    more = false;

                    if(readNext)
                    {
                        while(null != (line = in.readLine()))
                        {
                            ++lineNumber;

                            query.append(line).append(NEWLINE);

                            // Stop when you get to the end of a statement
                            if(line.trim().endsWith(";"))
                                break;
                        }
                    }

                    complete |= (null == line && queryIsBlank(query));

                    if(complete)
                    {
                        goUntilError = false; // Stop the madness
                        System.out.println("Script " + script + " is complete.");
                        System.out.println();
                        System.out.print("> Command (D/b/r/c/>/h/q)? ");
                    }
                    else
                    {
                        if(clearScreenBeforeStatement && readNext) {
                            // Don't require ENTER before showing the first query.
                            if(firstQuery)
                                firstQuery = false;
                            else
                            {
                                System.out.println("Press ENTER to continue to the next query...");

                                cmd.readLine();
                            }

                            System.out.write(CLS);
                        }

                        if(statementStartLine == (lineNumber - 1))
                            System.out.println(script + ": " + statementStartLine + ":");
                        else
                            System.out.println(script + ": " + statementStartLine + " - " + (lineNumber - 1) + ":");

                        System.out.print(query); // Should already end with a \n
                        System.out.println();
                        System.out.print("> Execute (D/x/g/s/b/r/c/m/!/>/h/q)? ");
                    }

                    final String command;
                    if(goUntilError) {
                        command = "x";
                        System.out.println(); // Newline replaces the one the user would enter
                    } else {
                        command = cmd.readLine();
                    }

                    if(null == command || "q".equals(command))
                    {
                        if(complete)
                            System.out.println("Finished " + script);
                        else
                            System.out.println("Quitting at " + script + ":" + statementStartLine);

                        stop = true;
                    }
                    else if("s".equals(command) || "n".equals(command))
                    {
                        if(!complete)
                        {
                            System.out.println(">>>> Skipping Statement <<<<");
                            readNext = true;
                        }
                    }
                    else if("m".equals(command))
                    {
                        if(!complete)
                        {
                            more = true;
                            readNext = true;
                        }
                    }
                    else if("b".equals(command))
                    {
                        try
                        {
                            System.out.println("Executing BEGIN...");
                            executeSQL("BEGIN;", conn, System.out);
                        }
                        catch (SQLException sqle)
                        {
                            System.err.println("Failed to BEGIN transaction.");
                            sqle.printStackTrace();
                        }
                        readNext = false;
                    }
                    else if("r".equals(command))
                    {
                        try
                        {
                            System.out.println("Executing ROLLBACK...");
                            executeSQL("ROLLBACK;", conn, System.out);
                        }
                        catch (SQLException sqle)
                        {
                            System.err.println("Failed to ROLLBACK transaction.");
                            sqle.printStackTrace();
                        }
                        readNext = false;
                    }
                    else if("c".equals(command))
                    {
                        try
                        {
                            System.out.println("Executing COMMIT...");
                            executeSQL("COMMIT;", conn, System.out);
                        }
                        catch (SQLException sqle)
                        {
                            System.err.println("Failed to COMMIT transaction.");
                            sqle.printStackTrace();
                        }
                        readNext = false;
                    }
                    else if("x".equals(command) || "y".equals(command))
                    {
                        if(!complete)
                        {
                            System.out.println(">>>> Executing statement <<<<");
                            System.out.flush();
                            try
                            {
                                executeSQL(query.toString(), conn, System.out);
                                readNext = true;
                            }
                            catch (SQLException sqle)
                            {
                                System.err.println("Failed to execute statement");
                                sqle.printStackTrace();
                                goUntilError = false;
                                readNext = false;
                            }
                        }
                    }
                    else if("g".equals(command))
                    {
                        goUntilError = true;
                        readNext = false;
                    }
                    else if(">".equals(command))
                    {
                        System.out.println("Enter the SQL statement you'd like to execute:");
                        System.out.println("(All on a single line: statement will be executed after a newline is entered)");
                        System.out.print("> ");
                        final String statement = cmd.readLine();

                        if(queryIsBlank(statement))
                        {
                            System.out.println(">>>> No query entered. Ignoring <<<<");
                        }
                        else
                        {
                            System.out.println(">>>> Executing ad-hoc statement <<<<");
                            System.out.flush();
                            try
                            {
                                executeSQL(statement, conn, System.out);
                            }
                            catch (SQLException sqle)
                            {
                                System.err.println("Failed to execute ad-hoc statement");
                                sqle.printStackTrace();
                            }
                        }
                        readNext = false;
                    }
                    else if("?".equals(command) || "h".equals(command))
                    {
                        System.out.println("Commands: ");
                        System.out.println();
                        System.out.println("  h or ?       Show this help screen");
                        System.out.println("  d (default)  Display the current statement");
                        System.out.println("  x or y       Execute the current statement and continue");
                        System.out.println("  s or n       Skip the current statement");
                        System.out.println("  g            Execute statements until an error is encountered");
                        System.out.println("  b            BEGIN a new transaction (executes BEGIN statement)");
                        System.out.println("  r            ROLLBACK the current transaction (executes a ROLLBACK statement)");
                        System.out.println("  c            COMMIT the current transaction (executes COMMIT statement)");
                        System.out.println("  m            Read more lines of the script into the current statement");
                        System.out.println("  e            Edit the query using a text editor");
                        System.out.println("  >            Execute an arbitrary ad-hoc statement");
                        System.out.println("  q            Quit");
                        System.out.println();

                        readNext = false;
                    }
                    else if ("e".equals(command))
                    {
                        query = editQuery(query);

                        readNext = false;
                    }
                    else if("".equals(command) || "D".equalsIgnoreCase(command)) // Just to document behavior
                    {
                        // Do nothing: just re-display the query
                        readNext = false;
                    }
                    else
                    {
                        System.err.println("Error: unrecognized command: " + command);

                        // Unrecognized command: just re-display the query
                        readNext = false;
                    }
                }
            }
        }
        catch (IOException ioe)
        {
            System.err.println("Failed to read script file");
            ioe.printStackTrace();
        }
        finally
        {
            if(null != in) try { in.close(); }
            catch (IOException ioe) { System.err.println("Could not close script file"); }
        }
    }

    private static StringBuilder editQuery(final StringBuilder query)
    {
        // Attempt to drop to editor
        String editor = System.getenv("EDITOR");
        if(null == editor || 0 == editor.trim().length()) {
            System.err.println("Error: Cannot determine editor");
            return query;
        }

        Writer queryOut = null;
        Reader queryIn = null;

        try {
            File queryFile = File.createTempFile("SQLExecutor.query.", ".sql");

            queryOut  = new OutputStreamWriter(new FileOutputStream(queryFile), "UTF-8");
            queryOut.write(query.toString()); // Write the current query to the temp file
            queryOut.close();
            queryOut = null;

            ProcessBuilder pb = new ProcessBuilder(editor, queryFile.getAbsolutePath());
            pb.redirectInput(ProcessBuilder.Redirect.INHERIT);
            pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
            pb.redirectError(ProcessBuilder.Redirect.INHERIT);

            Process proc = pb.start();

            int exitValue = -1;

            try {
                exitValue = proc.waitFor();
            } catch (InterruptedException ie) {
                ie.printStackTrace();
            }

            System.out.println("Got process result " + exitValue);

            queryIn = new InputStreamReader(new FileInputStream(queryFile), "UTF-8");

            StringBuilder sb = new StringBuilder();
            char[] buffer = new char[1024];
            int read;
            while(-1 != (read = queryIn.read(buffer)))
                sb.append(buffer, 0, read);

            queryIn.close();
            queryIn = null;

            if(!queryFile.delete())
                System.err.println("Error: Unable to delete query file " + queryFile.getAbsolutePath());

            return sb;
        } catch (IOException ioe) {
            System.out.println("Could not edit query");
            ioe.printStackTrace();

            return query;
        } finally {
            if(null != queryOut) try { queryOut.close(); } catch (IOException ioe)
            { System.out.println("Could not close query file"); ioe.printStackTrace(); }
            if(null != queryIn) try { queryIn.close(); } catch (IOException ioe)
            { System.out.println("Could not close query file"); ioe.printStackTrace(); }
        }
    }

    static void executeSQL(final String statement, final Connection conn, final PrintStream out)
        throws SQLException
    {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try
        {
            ps = conn.prepareStatement(statement);

            long queryTime = System.currentTimeMillis();

            boolean result = ps.execute();

            queryTime = System.currentTimeMillis() - queryTime;

            int maxColumnWidth = 4096;

            if(result)
            {
                rs = ps.getResultSet();
                long rows = 0;

                if(rs.next())
                {
                    final ResultSetMetaData rsmd = rs.getMetaData();
                    final int columnCount = rsmd.getColumnCount();
                    final int[] columnWidths = new int[columnCount];
                    final String[] columnFormats = new String[columnCount];
                    // We'll use the header a few times, so only build it once
                    final StringBuilder header = new StringBuilder();

                    // Build the output footer
                    header.append('+');
                    for(int i=0; i<columnCount; ++i)
                    {
                        header.append('-');

                        int displaySize = rsmd.getColumnDisplaySize(i + 1);
                        final int labelSize = rsmd.getColumnLabel(i + 1).length();
                        if(displaySize < labelSize)
                            displaySize = labelSize;

                        if(displaySize > maxColumnWidth)
                            displaySize = maxColumnWidth;

                        for(int j=0; j<displaySize; ++j)
                            header.append('-');

                        header.append("-+");

                        columnWidths[i] = displaySize;
                        columnFormats[i] = " %" + displaySize + "s |";
                    }

                    header.trimToSize();

                    // Print the header.
                    out.println(header);

                    out.print('|');

                    //
                    // Rather than using StringBuilders for everything,
                    // use streams for all formatted-output. Unfortunately,
                    // java.util.Formatter will close the underlying stream
                    // when you call close() on it, so we wrap a Proxy around
                    // the real object that will only advertise that it is an
                    // Appendable, so it can't be closed.
                    //
                    // I'm not sure this is actually worth it, but it doesn't
                    // add *that* much complexity and should reduce memory usage
                    // a bit.
                    //
                    Appendable appendable = (Appendable)Proxy.newProxyInstance(SQLExecutor.class.getClassLoader(),
                                                                               new Class<?>[] { Appendable.class },
                                                                               new InvocationHandler() {
                                    public Object invoke(Object o, Method m, Object[] args)
                                    {
                                        try
                                        {
                                            return m.invoke(out, args);
                                        }
                                        catch (IllegalAccessException iae)
                                        {
                                            iae.printStackTrace();
                                        }
                                        catch (InvocationTargetException ite)
                                        {
                                            ite.printStackTrace();
                                        }
                                        return null;
                                    }

                                });

                    for(int i=0; i<columnCount; ++i)
                    {
                        Formatter fmt = new Formatter(appendable);
                        fmt.format(" %-" + columnWidths[i] + "s |", rsmd.getColumnLabel(i + 1));
                        fmt.close();
                    }

                    out.println();

                    // This time, it's the separator between header and data
                    out.println(header);

                    do
                    {
                        out.print('|');
                        for(int i=0; i<columnCount; ++i)
                        {
                            String value = rs.getString(i + 1);
                            if(rs.wasNull())
                                value = "NULL";

                            Formatter fmt = new Formatter(appendable);
                            fmt.format(columnFormats[i], value);
                            fmt.close();
                        }
                        ++rows;
                        out.println();
                    } while(rs.next());

                    // This time, it's the footer
                    out.println(header);
                }
                out.print(rows);
                if(1 == rows)
                    out.print(" row in set (");
                else
                    out.print(" rows in set (");
                out.print(queryTime);
                out.println("ms)");
                out.println();
            }
            else
            {
                out.print("Query OK, ");
                out.print(ps.getUpdateCount());
                if(1 == ps.getUpdateCount())
                    out.print(" row affected (");
                else
                    out.print(" rows affected (");
                out.print(queryTime);
                out.println("ms)");
                out.println();
            }
        }
        finally
        {
            if(null != ps) try { ps.close(); }
            catch (SQLException sqle) { sqle.printStackTrace(); }
            if(null != rs) try { rs.close(); }
            catch (SQLException sqle) { sqle.printStackTrace(); }
        }
    }

    // Sadly, CharSequence doesn't give access to individual code points,
    // so we have to implement both StringBuilder and String blank-checkers.
    static boolean queryIsBlank(StringBuilder s)
    {
        if(null == s)
            return true;

        final int length = s.length();
        for (int offset = 0; offset < length; ) {
           final int codepoint = s.codePointAt(offset);

           if(!Character.isWhitespace(codepoint))
               return false;

           offset += Character.charCount(codepoint);
        }
        return true;
    }

    static boolean queryIsBlank(String s)
    {
        if(null == s)
            return true;

        final int length = s.length();
        for (int offset = 0; offset < length; ) {
           final int codepoint = s.codePointAt(offset);

           if(!Character.isWhitespace(codepoint))
               return false;

           offset += Character.charCount(codepoint);
        }
        return true;
    }

    static void usage()
    {
        System.out.print("Usage: java ");
        System.out.print(SQLExecutor.class.getName());
        System.out.println(" [options] [script]");
        System.out.println();
        System.out.println("Options (required):");
        System.out.println("  --driver className  The name of the JDBC driver class.");
        System.out.println("  --url URL           The JDBC URL for the database connection.");
        System.out.println("  <file> -or-");
        System.out.println("  --script   file     The name of the script file to execute.");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  --askpass           Securely-requests the password from the console.");
        System.out.println("  --clear             Clears the screen before displaying each statement.");
        System.out.println("  --driverjar jarfile Specifies the JAR file containing the JDBC driver.");
        System.out.println("  --encoding charset  The character encoding of the script file (default: " + System.getProperty("file.encoding", "UTF-8") + ").");
        System.out.println("  --password password The database password.");
        System.out.println("  --skip n            Skips n lines at the beginning of the script.");
        System.out.println("  --username username The name of the database user.");
        System.out.println("  --help, -h          Shows this help text.");
    }
}
