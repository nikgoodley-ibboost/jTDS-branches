// jTDS JDBC Driver for Microsoft SQL Server and Sybase
// Copyright (C) 2004 The jTDS Project
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public
// License as published by the Free Software Foundation; either
// version 2.1 of the License, or (at your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
//
package net.sourceforge.jtds.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.BatchUpdateException;
import java.sql.SQLWarning;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedList;

/**
 * jTDS implementation of the java.sql.Statement interface.<p>
 * NB. As allowed by the JDBC standard and like most other drivers,
 * this implementation only allows one open result set at a time.
 * <p>
 * Implementation notes:
 * <p>
 * I experimented with allowing multiple open result sets as supported
 * by the origianal jTDS but rejected this approach for the following
 * reasons:
 * <ol>
 * <li>It is more difficult to ensure that there are no memory leaks and that
 *     cursors are closed if multiple open sets are allowed.
 * <li>The use of one result set allows cursor and non cursor result sets to
 *     be derived from exeuteQuery() or execute() and getResultSet() in the
 *     same way that other drivers do.
 * </ol>
 * In the event of an IO failure the setClosed() method forces this statement
 * and associated result set to close preventing the propogation of errors.
 * This class includes a finalize method which increases the chances of the
 * statement being closed tidly in a pooled environment where the user has
 * forgotten to explicitly close the statement before it goes out of scope.
 *
 * @see java.sql.Statement
 * @see java.sql.Connection#createStatement
 * @see java.sql.ResultSet
 *
 * @author Mike Hutchinson
 * @version $Id: JtdsStatement.java,v 1.35.2.1 2005-09-17 10:58:59 alin_sinpalean Exp $
 */
public class JtdsStatement implements java.sql.Statement {
    /*
     * Constants used for backwards compatibility with JDK 1.3
     */
    static final int RETURN_GENERATED_KEYS = 1;
    static final int NO_GENERATED_KEYS = 2;
    static final int CLOSE_CURRENT_RESULT = 1;
    static final int KEEP_CURRENT_RESULT = 2;
    static final int CLOSE_ALL_RESULTS = 3;
    static final int BOOLEAN = 16;
    static final int DATALINK = 70;
    static final Integer SUCCESS_NO_INFO = new Integer(-2);
    static final Integer EXECUTE_FAILED = new Integer(-3);

    /** The connection owning this statement object. */
    protected ConnectionJDBC2 connection;
    /** The TDS object used for server access. */
    protected TdsCore tds = null;
    /** The read query timeout in seconds */
    protected int queryTimeout = 0;
    /** The current <code>ResultSet</code>. */
    protected JtdsResultSet currentResult = null;
    /** The current update count. */
    private int updateCount = -1;
    /** The fetch direction for result sets. */
    protected int fetchDirection = ResultSet.FETCH_FORWARD;
    /** The fetch size (default 100, only used by cursor <code>ResultSet</code>s). */
    protected int fetchSize = 100;
    /** True if this statement is closed. */
    protected boolean closed = false;
    /** The maximum field size (not used at present). */
    protected int maxFieldSize = 0;
    /** The maximum number of rows to return (not used at present). */
    protected int maxRows = 0;
    /** True if SQL statements should be preprocessed. */
    protected boolean escapeProcessing = true;
    /** SQL Diagnostic exceptions and warnings. */
    protected SQLDiagnostic messages;
    /** Batched SQL Statement array. */
    protected ArrayList batchValues = null;
    /** Dummy result set for getGeneratedKeys. */
    protected JtdsResultSet genKeyResultSet;
    /**
     * List of queued results (update counts, possibly followed by a
     * <code>ResultSet</code>).
     */
    protected LinkedList resultQueue = new LinkedList();
    /** List of open result sets. */
    protected ArrayList openResultSets;

    /**
     * Construct a new Statement object.
     *
     * @param connection The parent connection.
     */
    JtdsStatement(ConnectionJDBC2 connection) {
        this.connection = connection;
        this.messages = new SQLDiagnostic();
        this.tds = new TdsCore(this.connection, messages);
    }

    /**
     * Called when this object goes out of scope
     * to close any resultSet object and this statement.
     */
    protected void finalize() {
       try {
           close();
       } catch (SQLException e) {
           // Ignore errors
       }
   }

    /**
     * Get the Statement's TDS object.
     *
     * @return The TDS support as a <code>TdsCore</core> Object.
     */
    TdsCore getTds() {
        return tds;
    }

    /**
     * Check that this statement is still open.
     *
     * @throws SQLException if statement closed.
     */
    protected void checkOpen() throws SQLException {
        if (closed || connection == null || connection.isClosed()) {
            throw new SQLException(
                    Messages.get("error.generic.closed", "Statement"), "HY010");
        }
    }

    /**
     * Report that user tried to call a method which has not been implemented.
     *
     * @param method The method name to report in the error message.
     * @throws SQLException
     */
    void notImplemented(String method) throws SQLException {
        throw new SQLException(
                Messages.get("error.generic.notimp", method), "HYC00");
    }

    /**
     * Close current result set (if any).
     */
    void closeCurrentResultSet() throws SQLException {
        try {
            if (currentResult != null) {
                currentResult.close();
            }
//        } catch (SQLException e) {
            // Ignore
        } finally {
            currentResult = null;
        }
    }

    /**
     * Close all result sets.
     */
    void closeAllResultSets() throws SQLException {
        try {
            if (openResultSets != null) {
                for (int i = 0; i < openResultSets.size(); i++) {
                    JtdsResultSet rs = (JtdsResultSet) openResultSets.get(i);
                    if (rs != null) {
                        rs.close();
                    }
                }
            }
            closeCurrentResultSet();
        } finally {
            openResultSets = null;
        }
    }

    /**
     * Add an SQLWarning object to the statment warnings list.
     *
     * @param w The SQLWarning to add.
     */
    void addWarning(SQLWarning w) {
        messages.addWarning(w);
    }

    /**
     * This method should be over-ridden by any sub-classes that place values
     * other than <code>String</code>s into the <code>batchValues</code> list
     * to handle execution properly.
     *
     * @param value SQL <code>String</code> or list of parameters to execute
     * @param last  <code>true</code> if this is the last query/parameter list
     *              in the batch
     */
    protected void executeBatchOther(Object value, boolean last)
            throws SQLException {
        throw new SQLException(
                Messages.get("error.statement.badbatch",
                        value.toString()), "HYC00");
    }

    /**
     * Execute SQL to obtain a result set.
     *
     * @param sql The SQL statement to execute.
     * @param spName Optional stored procedure name.
     * @param params Optional parameters.
     * @return The result set as a <code>ResultSet</code>.
     * @throws SQLException
     */
    protected ResultSet executeSQLQuery(String sql,
                                        String spName,
                                        ParamInfo[] params)
            throws SQLException {
        String warningMessage = null;

        //
        // Could not open a Cursor so try a direct select
        //
        tds.executeSQL(sql, spName, params, false, queryTimeout, maxRows,
                maxFieldSize, true);

        // FIXME Should we ignore update counts here?
        while (!tds.getMoreResults() && !tds.isEndOfResponse());

        if (tds.isResultSet()) {
            currentResult = new JtdsResultSet(this, tds.getColumns());
        } else {
                throw new SQLException(
                            Messages.get("error.statement.noresult"), "24000");
        }

        if (warningMessage != null) {
            //
            // Update warning chain if cursor was downgraded
            //
            addWarning(new SQLWarning(
                    Messages.get("warning.cursordowngraded", warningMessage), "01000"));
        }

        return currentResult;
    }

    /**
     * Execute any type of SQL.
     *
     * @param sql The SQL statement to execute.
     * @param spName Optional stored procedure name.
     * @param params Optional parameters.
     * @return <code>boolean</code> true if a result set is returned by the statement.
     * @throws SQLException if an error condition occurs
     */
    protected boolean executeSQL(String sql,
                                 String spName,
                                 ParamInfo[] params,
                                 boolean returnKeys,
                                 boolean update)
            throws SQLException {
        String warningMessage = null;

        //
        // We are talking to a Sybase server or we could not open a cursor
        // or we did not have a SELECT so just execute the SQL normally.
        //
        tds.executeSQL(sql, spName, params, false, queryTimeout, maxRows,
                maxFieldSize, true);

        if (warningMessage != null) {
            // Update warning chain if cursor was downgraded
            addWarning(new SQLWarning(Messages.get(
                    "warning.cursordowngraded", warningMessage), "01000"));
        }

        if (processResults(returnKeys, update)) {
            Object nextResult = resultQueue.removeFirst();

            // Next result is an update count
            if (nextResult instanceof Integer) {
                updateCount = ((Integer) nextResult).intValue();
                return false;
            }

            // Next result is a ResultSet. Set currentResult and remove it.
            currentResult = (JtdsResultSet) nextResult;
            return true;
        } else {
            return false;
        }
    }

    /**
     * Queue up update counts into {@link #resultQueue} until the end of the
     * response is reached or a <code>ResultSet</code> is encountered. Calling
     * <code>processResults</code> while a <code>ResultSet</code> is open will
     * not close it, but will consume all remaining rows.
     *
     * @param returnKeys <code>true</code> if a generated keys
     *                   <code>ResultSet</code> is expected
     * @param update     <code>true</code> if the method is called from within
     *                   <code>executeUpdate</code>
     * @return           <code>true</code> if there are any results,
     *                   <code>false</code> otherwise
     * @throws SQLException if an error condition occurs
     */
    private boolean processResults(boolean returnKeys, boolean update) throws SQLException {
        if (!resultQueue.isEmpty()) {
            throw new IllegalStateException(
                    "There should be no queued results.");
        }

        while (!tds.isEndOfResponse()) {
            if (!tds.getMoreResults()) {
                if (tds.isUpdateCount()) {
                    if (update && connection.isLastUpdateCount()) {
                        resultQueue.clear();
                    }
                    resultQueue.addLast(new Integer(tds.getUpdateCount()));
                }
            } else {
                if (returnKeys) {
                    // This had better be the generated key
                    // FIXME We could use SELECT @@IDENTITY AS jTDS_SOMETHING and check the column name to make sure
                    if (tds.getNextRow()) {
                        genKeyResultSet = new CachedResultSet(this,
                                tds.getColumns(),
                                tds.getRowData());
                    }
                } else {
                    // TODO Should we allow execution of multiple statements via executeUpdate?
                    if (update && resultQueue.isEmpty()) {
                        throw new SQLException(
                                Messages.get("error.statement.nocount"), "07000");
                    }

                    resultQueue.add(new JtdsResultSet(this, tds.getColumns()));
                    break;
                }
            }
        }

        return !resultQueue.isEmpty();
    }

    /**
     * Cache as many results as possible (up to the first
     * <code>ResultSet</code>). Called by <code>ResultSet</code>s when the
     * end is reached.
     */
    protected void cacheResults() throws SQLException {
        // Cache results
        processResults(false, false);
    }

    /**
     * Initialize the <code>Statement</code>, by cleaning up all queued and
     * unprocessed results. Called by all execute methods.
     *
     * @throws SQLException if an error occurs
     */
    protected void initialize() throws SQLException {
        updateCount = -1;
        resultQueue.clear();
        genKeyResultSet = null;
        tds.clearResponseQueue();
        closeAllResultSets();
    }

// ------------------ java.sql.Statement methods ----------------------

    public int getFetchDirection() throws SQLException {
        checkOpen();

        return this.fetchDirection;
    }

    public int getFetchSize() throws SQLException {
        checkOpen();

        return this.fetchSize;
    }

    public int getMaxFieldSize() throws SQLException {
        checkOpen();

        return this.maxFieldSize;
    }

    public int getMaxRows() throws SQLException {
        checkOpen();

        return this.maxRows;
    }

    public int getQueryTimeout() throws SQLException {
        checkOpen();

        return this.queryTimeout;
    }

    public int getResultSetConcurrency() throws SQLException {
        checkOpen();

        return ResultSet.CONCUR_READ_ONLY;
    }

    public int getResultSetHoldability() throws SQLException {
        checkOpen();

        return JtdsResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    public int getResultSetType() throws SQLException {
        checkOpen();

        return ResultSet.TYPE_FORWARD_ONLY;
    }

    public int getUpdateCount() throws SQLException {
        checkOpen();

        return updateCount;
    }

    public void cancel() throws SQLException {
        checkOpen();

        if (tds != null) {
            tds.cancel();
        }
    }

    public synchronized void clearBatch() throws SQLException {
        checkOpen();

        if (batchValues != null) {
            batchValues.clear();
        }
    }

    public void clearWarnings() throws SQLException {
        checkOpen();

        messages.clearWarnings();
    }

    public synchronized void close() throws SQLException {
        if (!closed) {
            try {
                closeAllResultSets();

                if (!connection.isClosed()) {
                    tds.clearResponseQueue();
                    tds.close();
                }
            } finally {
                closed = true;
                tds = null;
                connection.removeStatement(this);
                connection = null;
            }
        }
    }

    public boolean getMoreResults() throws SQLException {
        checkOpen();

        return getMoreResults(CLOSE_ALL_RESULTS);
    }

    /**
     * Execute batch of SQL Statements.
     * <p/>
     * The JDBC3 standard says that for a server which does not continue
     * processing a batch once the first error has occurred (e.g. SQL Server),
     * the returned array will never contain <code>EXECUTE_FAILED</code> in any
     * of the elements. If a failure has occurred the array length will be less
     * than the count of statements in the batch. For those servers that do
     * continue to execute a batch containing an error (e.g. Sybase), elements
     * may contain <code>EXECUTE_FAILED</code>. Note: even here the array
     * length may also be shorter than the statement count if a serious error
     * has prevented the rest of the batch from being executed. In addition,
     * when executing batched stored procedures, Sybase will also stop after
     * the first error.
     *
     * @return update counts as an <code>int[]</code>
     */
    public synchronized int[] executeBatch()
            throws SQLException, BatchUpdateException {
        checkOpen();
        initialize();

        if (batchValues == null || batchValues.size() == 0) {
            return new int[0];
        }

        ArrayList counts = new ArrayList();
        int size = batchValues.size();
        int executeSize = connection.getBatchSize();
        executeSize = (executeSize == 0) ? Integer.MAX_VALUE : executeSize;
        SQLException sqlEx = null;

        try {
            for (int i = 0; i < size;) {
                Object value = batchValues.get(i);
                ++i;
                // Execute batch now if max size reached or end of batch
                boolean executeNow = (i % executeSize == 0) || i == size;

                if (value instanceof String) {
                    tds.executeSQL((String)value, null, null, true, 0, -1, -1,
                            executeNow);
                } else {
                    executeBatchOther(value, executeNow);
                }

                // If the batch has been sent, process the results
                if (executeNow) {
                    sqlEx = tds.getBatchCounts(counts, sqlEx);

                    // If a serious error or a MS server error then we stop
                    // execution now as count is too small. Sybase continues,
                    // flagging failed updates in the count array.
                    if (sqlEx != null && counts.size() != i) {
                        break;
                    }
                }
            }
            //
            // Copy the update counts into the int array
            //
            int updateCounts[] = new int[counts.size()];
            for (int i = 0; i < updateCounts.length; i++) {
                updateCounts[i] = ((Integer) counts.get(i)).intValue();
            }
            //
            // See if we should return an exception
            //
            if (sqlEx != null) {
                BatchUpdateException batchEx =
                        new BatchUpdateException(sqlEx.getMessage(),
                                                 sqlEx.getSQLState(),
                                                 sqlEx.getErrorCode(),
                                                 updateCounts);
                // Chain any other exceptions
                batchEx.setNextException(sqlEx.getNextException());
                throw batchEx;
            }
            return updateCounts;
        } catch (BatchUpdateException ex) {
            // If it's a BatchUpdateException let it go
            throw ex;
        } catch (SQLException ex) {
            // An SQLException can only occur while sending the batch
            // (getBatchCounts() doesn't throw SQLExceptions), so we have to
            // end the batch and return the partial results
            // FIXME What should we send here to flush out the batch?
            // Come to think of it, is there any circumstance under which this
            // could actually happen without the connection getting closed?
            // No counts will have been returned either as last packet will not
            // have been sent.
            throw new BatchUpdateException(ex.getMessage(), ex.getSQLState(),
                    ex.getErrorCode(), new int[0]);
        } finally {
            clearBatch();
        }
    }
/*
    public synchronized int[] executeBatch() throws SQLException {
        checkOpen();

        if (batchValues == null || batchValues.size() == 0) {
            return new int[0];
        }

        int size = batchValues.size();
        int[] updateCounts = new int[size];
        int i = 0;

        try {
            for (; i < size; i++) {
                Object value = batchValues.get(i);

                if (value instanceof String) {
                    updateCounts[i] = executeUpdate((String) value);
                } else {
                    updateCounts[i] = executeBatchOther(value);
                }
            }
        } catch (SQLException e) {
            int[] tmpUpdateCounts = new int[i + 1];

            System.arraycopy(updateCounts, 0, tmpUpdateCounts, 0, i + 1);

            tmpUpdateCounts[i] = EXECUTE_FAILED;
            throw new BatchUpdateException(e.getMessage(), tmpUpdateCounts);
        } finally {
            clearBatch();
        }

        return updateCounts;
    }
*/
    public void setFetchDirection(int direction) throws SQLException {
        checkOpen();
        switch (direction) {
        case ResultSet.FETCH_UNKNOWN:
        case ResultSet.FETCH_REVERSE:
        case ResultSet.FETCH_FORWARD:
            this.fetchDirection = direction;
            break;

        default:
            throw new SQLException(Messages.get("error.generic.badoption",
                                   Integer.toString(direction),
                                   "setFetchDirection"), "24000");
        }
    }

    public void setFetchSize(int rows) throws SQLException {
        checkOpen();

        if (rows < 0 || (maxRows > 0 && rows > maxRows)) {
            throw new SQLException(
                Messages.get("error.generic.optltzero", "setFetchSize"),
                    "HY092");
        }

        this.fetchSize = rows;
    }

    public void setMaxFieldSize(int max) throws SQLException {
        checkOpen();

        if (max < 0) {
            throw new SQLException(
                Messages.get("error.generic.optltzero", "setMaxFieldSize"),
                    "HY092");
        }

        maxFieldSize = max;
    }

    public void setMaxRows(int max) throws SQLException {
        checkOpen();

        if (max < 0) {
            throw new SQLException(
                Messages.get("error.generic.optltzero", "setMaxRows"),
                    "HY092");
        }

        this.maxRows = max;
    }

    public void setQueryTimeout(int seconds) throws SQLException {
        checkOpen();

        if (seconds < 0) {
            throw new SQLException(
                Messages.get("error.generic.optltzero", "setQueryTimeout"),
                    "HY092");
        }

        this.queryTimeout = seconds;
    }

    public boolean getMoreResults(int current) throws SQLException {
        checkOpen();

        switch (current) {
            case CLOSE_ALL_RESULTS:
                updateCount = -1;
                closeAllResultSets();
                break;
            case CLOSE_CURRENT_RESULT:
                updateCount = -1;
                closeCurrentResultSet();
                break;
            case KEEP_CURRENT_RESULT:
                updateCount = -1;
                // If there is an open result set it is transferred to
                // the list of open result sets. For JtdsResultSet
                // result sets we cache the remaining data. For CachedResultSet
                // result sets the data is already cached.
                if (openResultSets == null) {
                    openResultSets = new ArrayList();
                }
                if (currentResult instanceof CachedResultSet) {
                    // NB. Due to restrictions on the way API cursors are
                    // created, MSCursorResultSet can never be followed by
                    // any other result sets, update counts or return variables.
                    openResultSets.add(currentResult);
                } else if (currentResult instanceof JtdsResultSet) {
                    currentResult.cacheResultSetRows();
                    openResultSets.add(currentResult);
                }
                currentResult = null;
                break;
            default:
                throw new SQLException(Messages.get("error.generic.badoption",
                                                          Integer.toString(current),
                                                          "getMoreResults"),
                                       "HY092");
        }

        // Dequeue any results
        if (!resultQueue.isEmpty() || processResults(false, false)) {
            Object nextResult = resultQueue.removeFirst();

            // Next result is an update count
            if (nextResult instanceof Integer) {
                updateCount = ((Integer) nextResult).intValue();
                return false;
            }

            // Next result is a ResultSet. Set currentResult and remove it.
            currentResult = (JtdsResultSet) nextResult;
            return true;
        } else {
            return false;
        }
    }

    public void setEscapeProcessing(boolean enable) throws SQLException {
        checkOpen();

        this.escapeProcessing = enable;
    }

    public int executeUpdate(String sql) throws SQLException {
        return executeUpdate(sql, NO_GENERATED_KEYS);
    }

    public synchronized void addBatch(String sql) throws SQLException {
        checkOpen();

        if (sql == null) {
            throw new NullPointerException();
        }

        if (batchValues == null) {
            batchValues = new ArrayList();
        }

        batchValues.add(sql);
    }

    public void setCursorName(String name) throws SQLException {
        checkOpen();
        Support.notImplemented("setCursorName");
    }

    public boolean execute(String sql) throws SQLException {
        return execute(sql, NO_GENERATED_KEYS);
    }

    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        checkOpen();
        initialize();

        if (sql == null || sql.length() == 0) {
            throw new SQLException(Messages.get("error.generic.nosql"), "HY000");
        }

        boolean returnKeys;
        String sqlWord = "";
        if (escapeProcessing) {
            ArrayList params = new ArrayList();
            String tmp[] = new SQLParser(sql, params, connection).parse();

            if (tmp[1].length() != 0 || params.size() > 0) {
                throw new SQLException(
                    Messages.get("error.statement.badsql"), "07000");
            }

            sql = tmp[0];
            sqlWord = tmp[2];
        } else {
            // Escape processing turned off so
            // see if we can extract "insert" from start of statement
            sql = sql.trim();
            if (sql.length() > 5) {
                sqlWord = sql.substring(0,6).toLowerCase();
            }
        }

        if (autoGeneratedKeys == RETURN_GENERATED_KEYS) {
            returnKeys = sqlWord.equals("insert");

            if (returnKeys) {
                if (connection.getDatabaseMajorVersion() >= 8) {
                    sql += " SELECT SCOPE_IDENTITY() AS ID";
                } else {
                    sql += " SELECT @@IDENTITY AS ID";
                }
            }
        } else if (autoGeneratedKeys == NO_GENERATED_KEYS) {
            returnKeys = false;
        } else {
            throw new SQLException(
                    Messages.get("error.generic.badoption",
                            Integer.toString(autoGeneratedKeys),
                            "executeUpdate"),
                    "HY092");
        }

        executeSQL(sql, null, null, returnKeys, true);

        int res = getUpdateCount();
        return res == -1 ? 0 : res;
    }

    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        checkOpen();
        initialize();

        if (sql == null || sql.length() == 0) {
            throw new SQLException(Messages.get("error.generic.nosql"), "HY000");
        }

        boolean returnKeys;
        String sqlWord = "";
        if (escapeProcessing) {
            ArrayList params = new ArrayList();
            String tmp[] = new SQLParser(sql, params, connection).parse();

            if (tmp[1].length() != 0 || params.size() > 0) {
                throw new SQLException(
                    Messages.get("error.statement.badsql"), "07000");
            }

            sql = tmp[0];
            sqlWord = tmp[2];
        } else {
            // Escape processing turned off so
            // see if we can extract "insert" from start of statement
            sql = sql.trim();
            if (sql.length() > 5) {
                sqlWord = sql.substring(0,6).toLowerCase();
            }
        }

        if (autoGeneratedKeys == RETURN_GENERATED_KEYS) {
            returnKeys = sqlWord.equals("insert");
        } else if (autoGeneratedKeys == NO_GENERATED_KEYS) {
            returnKeys = false;
        } else {
            throw new SQLException(Messages.get("error.generic.badoption",
                                                      Integer.toString(autoGeneratedKeys),
                                                      "execute"),
                                   "HY092");
        }

        if (returnKeys) {
            if (connection.getDatabaseMajorVersion() >= 8) {
                sql += " SELECT SCOPE_IDENTITY() AS ID";
            } else {
                sql += " SELECT @@IDENTITY AS ID";
            }
        }

        return executeSQL(sql, null, null, returnKeys, false);
    }

    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        checkOpen();

        if (columnIndexes == null) {
            throw new SQLException(
                Messages.get("error.generic.nullparam", "executeUpdate"),"HY092");
        } else if (columnIndexes.length != 1) {
            throw new SQLException(
                Messages.get("error.generic.needcolindex", "executeUpdate"),"HY092");
        }

        return executeUpdate(sql, RETURN_GENERATED_KEYS);
    }

    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        checkOpen();

        if (columnIndexes == null) {
            throw new SQLException(
                Messages.get("error.generic.nullparam", "execute"),"HY092");
        } else if (columnIndexes.length != 1) {
            throw new SQLException(
                Messages.get("error.generic.needcolindex", "execute"),"HY092");
        }

        return execute(sql, RETURN_GENERATED_KEYS);
    }

    public Connection getConnection() throws SQLException {
        checkOpen();

        return this.connection;
    }

    public ResultSet getGeneratedKeys() throws SQLException {
        checkOpen();

        if (genKeyResultSet == null) {
            String colNames[] = {"ID"};
            int    colTypes[] = {Types.INTEGER};
            //
            // Return an empty result set
            //
            CachedResultSet rs = new CachedResultSet(this, colNames, colTypes);
            rs.initDone();
            genKeyResultSet = rs;
        }

        return genKeyResultSet;
    }

    public ResultSet getResultSet() throws SQLException {
        checkOpen();

        return currentResult;
    }

    public SQLWarning getWarnings() throws SQLException {
        checkOpen();

        return messages.getWarnings();
    }

    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        checkOpen();

        if (columnNames == null) {
            throw new SQLException(
                Messages.get("error.generic.nullparam", "executeUpdate"),"HY092");
        } else if (columnNames.length != 1) {
            throw new SQLException(
                Messages.get("error.generic.needcolname", "executeUpdate"),"HY092");
        }

        return executeUpdate(sql, RETURN_GENERATED_KEYS);
    }

    public boolean execute(String sql, String[] columnNames) throws SQLException {
        if (columnNames == null) {
            throw new SQLException(
                Messages.get("error.generic.nullparam", "execute"),"HY092");
        } else if (columnNames.length != 1) {
            throw new SQLException(
                Messages.get("error.generic.needcolname", "execute"),"HY092");
        }

        return execute(sql, RETURN_GENERATED_KEYS);
    }

    public ResultSet executeQuery(String sql) throws SQLException {
        checkOpen();
        initialize();

        if (sql == null || sql.length() == 0) {
            throw new SQLException(Messages.get("error.generic.nosql"), "HY000");
        }
        if (escapeProcessing) {
            ArrayList params = new ArrayList();
            String tmp[] = new SQLParser(sql, params, connection).parse();

            if (tmp[1].length() != 0 || params.size() > 0) {
                throw new SQLException(
                    Messages.get("error.statement.badsql"), "07000");
            }

            sql = tmp[0];
        }

        return this.executeSQLQuery(sql, null, null);
    }
}
