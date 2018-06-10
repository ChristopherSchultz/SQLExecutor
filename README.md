# SQLExecutor
A command-line tool to execute SQL scripts.

## Building

    mvn package

## Running

    java -jar sqlexecutor.jar [options]

When running with `java -jar`, you can't also specify a JDBC driver JAR file. Instead, use the `--driverjar` option.

## Command-line options
```
Options (required):
  --driver className  The name of the JDBC driver class.
  --url URL           The JDBC URL for the database connection.
  <file> -or-
  --script   file     The name of the script file to execute.

Options:
  --askpass           Securely-requests the password from the console.
  --clear             Clears the screen before displaying each statement.
  --driverjar jarfile Specifies the JAR file containing the JDBC driver.
  --encoding charset  The character encoding of the script file.
  --username username The name of the database user.
  --password password The database password.
  --skip n            Skips n lines at the beginning of the script.
  --help, -h          Shows this help text.
```

## Script-execution commands

```
Commands:

  h or ?       Show this help screen
  d (default)  Display the current statement
  x or y       Execute the current statement and continue
  s or n       Skip the current statement
  g            Execute statements until an error is encountered
  b            BEGIN a new transaction (executes BEGIN statement)
  r            ROLLBACK the current transaction (executes a ROLLBACK statement)
  c            COMMIT the current transaction (executes COMMIT statement)
  m            Read more lines of the script into the current statement
  >            Execute an arbitrary ad-hoc statement
  q            Quit
```

