// jTDS JDBC Driver for Microsoft SQL Server and Sybase
// Copyright (C) 2005 The jTDS Project
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
package java.sql;

import java.io.InputStream;
import java.io.Reader;
import java.net.URL;
import java.util.Calendar;

/**
 * An object that represents a precompiled SQL statement.
 * <p/>
 * A SQL statement is precompiled and stored in a <code>PreparedStatement</code>
 * object. This object can then be used to efficiently execute this statement
 * multiple times.
 * <p/>
 * <b>Note:</b> The setter methods (<code>setShort, setString</code>, and so on)
 * for setting IN parameter values must specify types that are compatible with
 * the defined SQL type of the input parameter. For instance, if the IN
 * parameter has SQL type INTEGER, then the method <code>setInt</code> should be
 * used.
 * <p/>
 * If arbitrary parameter type conversions are required, the method
 * <code>setObject</code> should be used with a target SQL type.
 * <p/>
 * In the following example of setting a parameter, con represents an active
 * connection:
 * <pre>
 * PreparedStatement pstmt = con.prepareStatement("UPDATE EMPLOYEES
 *                                  SET SALARY = ? WHERE ID = ?");
 * pstmt.setFloat(1, 153833.89)
 * pstmt.setInt(2, 110592)
 * </pre>
 *
 * @see Connection#prepareStatement(java.lang.String)
 * @see ResultSet
 */
public interface PreparedStatement
        extends Statement {

    /**
     * Executes the SQL query in this <code>PreparedStatement</code> object and
     *  returns the <code>ResultSet</code> object generated by the query.
     *
     * @return a <code>ResultSet</code> object that contains the data produced
     *         by the query; never null
     * @throws SQLException if a database access error occurs or the SQL
     *         statement does not return a <code>ResultSet</code> object
     */
    ResultSet executeQuery()
            throws SQLException;

    /**
     * Executes the SQL statement in this <code>PreparedStatement</code> object,
     * which must be an SQL INSERT, UPDATE or DELETE statement; or an SQL
     * statement that returns nothing, such as a DDL statement.
     *
     * @return either (1) the row count for INSERT, UPDATE, or DELETE statements
     *         or (2) 0 for SQL statements that return nothing
     * @throws SQLException if a database access error occurs or the SQL
     *         statement returns a <code>ResultSet</code> object
     */
    int executeUpdate()
            throws SQLException;

    /**
     * Sets the designated parameter to SQL NULL.
     * <p/>
     * <b>Note:</b> You must specify the parameter's SQL type.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param sqlType the SQL type code defined in java.sql.Types
     * @throws SQLException if a database access error occurs
     */
    void setNull(int parameterIndex, int sqlType)
            throws SQLException;

    /**
     * Sets the designated parameter to the given Java boolean value. The driver
     * converts this to an SQL BIT value when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the parameter value
     * @throws SQLException if a database access error occurs
     */
    void setBoolean(int parameterIndex, boolean x)
            throws SQLException;

    /**
     * Sets the designated parameter to the given Java byte value. The driver
     * converts this to an SQL TINYINT value when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the parameter value
     * @throws SQLException if a database access error occurs
     */
    void setByte(int parameterIndex, byte x)
            throws SQLException;

    /**
     * Sets the designated parameter to the given Java short value. The driver
     * converts this to an SQL SMALLINT value when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the parameter value
     * @throws SQLException if a database access error occurs
     */
    void setShort(int parameterIndex, short x)
            throws SQLException;

    /**
     * Sets the designated parameter to the given Java int value. The driver
     * converts this to an SQL INTEGER value when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the parameter value
     * @throws SQLException if a database access error occurs
     */
    void setInt(int parameterIndex, int x)
            throws SQLException;

    /**
     * Sets the designated parameter to the given Java long value. The driver
     * converts this to an SQL BIGINT value when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the parameter value
     * @throws SQLException if a database access error occurs
     */
    void setLong(int parameterIndex, long x)
            throws SQLException;

    /**
     * Sets the designated parameter to the given Java float value. The driver
     * converts this to an SQL FLOAT value when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the parameter value
     * @throws SQLException if a database access error occurs
     */
    void setFloat(int parameterIndex, float x)
            throws SQLException;

    /**
     * Sets the designated parameter to the given Java double value. The driver
     * converts this to an SQL DOUBLE value when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the parameter value
     * @throws SQLException if a database access error occurs
     */
    void setDouble(int parameterIndex, double x)
            throws SQLException;

    /**
     * Sets the designated parameter to the given Java String value. The driver
     * converts this to an SQL VARCHAR or LONGVARCHAR value (depending on the
     * argument's size relative to the driver's limits on VARCHAR values) when
     * it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the parameter value
     * @throws SQLException if a database access error occurs
     */
    void setString(int parameterIndex, String x)
            throws SQLException;

    /**
     * Sets the designated parameter to the given Java array of bytes. The
     * driver converts this to an SQL VARBINARY or LONGVARBINARY (depending on
     * the argument's size relative to the driver's limits on VARBINARY values)
     * when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the parameter value
     * @throws SQLException if a database access error occurs
     */
    void setBytes(int parameterIndex, byte[] x)
            throws SQLException;

    /**
     * Sets the designated parameter to the given <code>java.sql.Date</code>
     * value. The driver converts this to an SQL DATE value when it sends it to
     * the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the parameter value
     * @throws SQLException if a database access error occurs
     */
    void setDate(int parameterIndex, Date x)
            throws SQLException;

    /**
     * Sets the designated parameter to the given <code>java.sql.Time</code>
     * value. The driver converts this to an SQL TIME value when it sends it to
     *  the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the parameter value
     * @throws SQLException if a database access error occurs
     */
    void setTime(int parameterIndex, Time x)
            throws SQLException;

    /**
     * Sets the designated parameter to the given
     * <code>java.sql.Timestamp</code> value. The driver converts this to an
     * SQL TIMESTAMP value when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the parameter value
     * @throws SQLException if a database access error occurs
     */
    void setTimestamp(int parameterIndex, Timestamp x)
            throws SQLException;

    /**
     * Sets the designated parameter to the given input stream, which will have
     * the specified number of bytes. When a very large ASCII value is input to
     * a LONGVARCHAR parameter, it may be more practical to send it via a
     * <code>java.io.InputStream</code>. Data will be read from the stream as
     * needed until end-of-file is reached. The JDBC driver will do any
     * necessary conversion from ASCII to the database char format.
     * <p/>
     * <b>Note:</b> This stream object can either be a standard Java stream
     * object or your own subclass that implements the standard interface.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the Java input stream that contains the ASCII parameter value
     * @param length the number of bytes in the stream
     * @throws SQLException if a database access error occurs
     */
    void setAsciiStream(int parameterIndex, InputStream x, int length)
            throws SQLException;

    /**
     * Sets the designated parameter to the given input stream, which will have
     * the specified number of bytes. When a very large binary value is input
     * to a LONGVARBINARY parameter, it may be more practical to send it via a
     * <code>java.io.InputStream</code> object. The data will be read from the
     * stream as needed until end-of-file is reached.
     * <p/>
     * <b>Note:</b> This stream object can either be a standard Java stream
     * object or your own subclass that implements the standard interface.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the Java input stream that contains the binary parameter value
     * @param length the number of bytes in the stream
     * @throws SQLException if a database access error occurs
     */
    void setBinaryStream(int parameterIndex, InputStream x, int length)
            throws SQLException;

    /**
     * Clears the current parameter values immediately.
     * <p/>
     * In general, parameter values remain in force for repeated use of a
     * statement. Setting a parameter value automatically clears its previous
     * value. However, in some cases it is useful to immediately release the
     * resources used by the current parameter values; this can be done by
     * calling the method <code>clearParameters</code>.
     *
     * @throws SQLException if a database access error occurs
     */
    void clearParameters()
            throws SQLException;

    /**
     * Sets the value of the designated parameter with the given object. The
     * second argument must be an object type; for integral values, the
     * <code>java.lang</code> equivalent objects should be used.
     * <p/>
     * The given Java object will be converted to the given targetSqlType before
     *  being sent to the database. If the object has a custom mapping (is of a
     * class implementing the interface SQLData), the JDBC driver should call
     * the method SQLData.writeSQL to write it to the SQL data stream. If, on
     * the other hand, the object is of a class implementing
     * <code>Blob or Clob</code>, the driver should pass it to the database as
     * a value of the corresponding SQL type.
     * <p/>
     * Note that this method may be used to pass database-specific abstract
     * data types.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the object containing the input parameter value
     * @param targetSqlType the SQL type (as defined in java.sql.Types) to be
     *                      sent to the database. The scale argument may further
     *                      qualify this type.
     * @param  scale for java.sql.Types.DECIMAL or java.sql.Types.NUMERIC types,
     *               this is the number of digits after the decimal point. For
     *               all other types, this value will be ignored.
     * @throws SQLException if a database access error occurs
     * @see Types
     */
    void setObject(int parameterIndex, Object x, int targetSqlType, int scale)
            throws SQLException;

    /**
     * Sets the value of the designated parameter with the given object. This
     * method is like the method setObject above, except that it assumes a scale
     * of zero.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the object containing the input parameter value
     * @param targetSqlType the SQL type (as defined in java.sql.Types) to be
     *                      sent to the database
     * @throws SQLException if a database access error occurs
     */
    void setObject(int parameterIndex, Object x, int targetSqlType)
            throws SQLException;

    /**
     * Sets the value of the designated parameter using the given object. The
     * second parameter must be of type <code>Object</code>; therefore, the
     * <code>java.lang</code> equivalent objects should be used for built-in
     * types.
     * <p/>
     * The JDBC specification specifies a standard mapping from
     * <code.Java Object</code> types to SQL types. The given argument will be
     * converted to the corresponding SQL type before being sent to the database.
     * <p/>
     * Note that this method may be used to pass datatabase- specific abstract
     * data types, by using a driver-specific Java type. If the object is of a
     * class implementing the interface SQLData, the JDBC driver should call the
     * method <code>SQLData.writeSQL</code> to write it to the SQL data stream.
     * If, on the other hand, the object is of a class implementing
     * <code>Blob or Clob</code>, the driver should pass it to the database as
     * a value of the corresponding SQL type.
     * <p/>
     * This method throws an exception if there is an ambiguity, for example,
     * if the object is of a class implementing more than one of the interfaces
     * named above.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the object containing the input parameter value
     * @throws SQLException if a database access error occurs or the type of the
     * given object is ambiguous
     */
    void setObject(int parameterIndex, Object x)
            throws SQLException;

    /**
     * Executes the SQL statement in this <code>PreparedStatement</code> object,
     * which may be any kind of SQL statement. Some prepared statements return
     * multiple results; the execute method handles these complex statements as
     * well as the simpler form of statements handled by the methods
     * <code>executeQuery</code> and <code>executeUpdate</code>.
     * <p/>
     * The <code>execute</code> method returns a boolean to indicate the form of
     * the first result. You must call either the method
     * <code>getResultSet</code> or <code>getUpdateCount</code> to retrieve the
     * result; you must call <code>getMoreResults</code> to move to any
     * subsequent result(s).
     *
     * @return true if the first result is a <code>ResultSet</code> object;
     *         false if the first result is an update count or there is no
     *         result
     * @throws SQLException if a database access error occurs or an argument is
     *         supplied to this method
     * @see Statement#execute(java.lang.String)
     * @see Statement#getResultSet()
     * @see Statement#getUpdateCount()
     * @see Statement#getMoreResults()
     */
    boolean execute() throws SQLException;

    /**
     * Adds a set of parameters to this <code>PreparedStatement</code> object's
     * batch of commands.
     *
     * @throws SQLException if a database access error occurs
     * @see Statement#addBatch(java.lang.String)
     */
    void addBatch() throws SQLException;

    /**
     * Sets the designated parameter to the given <code>Reader</code> object,
     * which is the given number of characters long. When a very large UNICODE
     * value is input to a LONGVARCHAR parameter, it may be more practical to
     * send it via a <code>java.io.Reader object</code>. The data will be read
     * from the stream as needed until end-of-file is reached. The JDBC driver
     * will do any necessary conversion from UNICODE to the database char format.
     * <p/>
     * <b>Note:</b> This stream object can either be a standard Java stream
     * object or your own subclass that implements the standard interface.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param reader the <code>java.io.Reader</code> object that contains the
     *               Unicode data
     * @param length the number of characters in the stream
     * @throws SQLException if a database access error occurs
     */
    void setCharacterStream(int parameterIndex, Reader reader, int length)
            throws SQLException;

    /**
     * Sets the designated parameter to the given <code>Blob</code> object. The
     * driver converts this to an SQL BLOB value when it sends it to the database.
     *
     * @param i the first parameter is 1, the second is 2, ...
     * @param x a <code>Blob</code> object that maps an SQL BLOB value
     * @throws SQLException if a database access error occurs
     */
    void setBlob(int i, Blob x) throws SQLException;

    /**
     * Sets the designated parameter to the given <code>Clob</code> object. The
     * driver converts this to an SQL CLOB value when it sends it to the database.
     *
     * @param i the first parameter is 1, the second is 2, ...
     * @param x a <code>Clob</code> object that maps an SQL CLOB value
     * @throws SQLException if a database access error occurs
     */
    void setClob(int i, Clob x) throws SQLException;

    /**
     * Retrieves a <code>ResultSetMetaData</code> object that contains
     * information about the columns of the <code>ResultSet</code> object that
     * will be returned when this <code>PreparedStatement</code> object is
     * executed.
     * <p/>
     * Because a <code>PreparedStatement</code> object is precompiled, it is
     * possible to know about the <code>ResultSet</code> object that it will
     * return without having to execute it. Consequently, it is possible to
     * invoke the method getMetaData on a <code>PreparedStatement</code> object
     * rather than waiting to execute it and then invoking the
     * <code>ResultSet.getMetaData</code> method on the <code>ResultSet</code>
     * object that is returned.
     * <p/>
     * <b>NOTE:</b> Using this method may be expensive for some drivers due to
     * the lack of underlying DBMS support.
     *
     * @return the description of a <code>ResultSet</code> object's columns or
     *         null if the driver cannot return a <code>ResultSetMetaData</code>
     *         object
     * @throws SQLException if a database access error occurs
     */
    ResultSetMetaData getMetaData() throws SQLException;

    /**
     * Sets the designated parameter to the given <code>java.sql.Date</code>
     * value, using the given <code>Calendar</code> object. The driver uses the
     * <code>Calendar</code> object to construct an SQL DATE value, which the
     * driver then sends to the database. With a a <code>Calendar</code> object,
     * the driver can calculate the date taking into account a custom timezone.
     * If no <code>Calendar</code> object is specified, the driver uses the
     * default timezone, which is that of the virtual machine running the
     * application.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the parameter value
     * @param cal the <code>Calendar</code> object the driver will use to
     *            construct the date
     * @throws SQLException if a database access error occurs
     */
    void setDate(int parameterIndex, Date x, Calendar cal)
            throws SQLException;

    /**
     * Sets the designated parameter to the given <code>java.sql.Time</code>
     * value, using the given <code>Calendar</code> object. The driver uses the
     * <code>Calendar</code> object to construct an SQL TIME value, which the
     * driver then sends to the database. With a a <code>Calendar</code> object,
     * the driver can calculate the time taking into account a custom timezone.
     * If no <code>Calendar</code> object is specified, the driver uses the
     * default timezone, which is that of the virtual machine running the
     * application.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the parameter value
     * @param cal the <code>Calendar</code> object the driver will use to
     *            construct the time
     * @throws SQLException if a database access error occurs
     */
    void setTime(int parameterIndex, Time x, Calendar cal)
            throws SQLException;

    /**
     * Sets the designated parameter to the given <code>java.sql.Timestamp</code>
     * value, using the given <code>Calendar</code> object. The driver uses the
     * <code>Calendar</code> object to construct an SQL TIMESTAMP value, which
     * the driver then sends to the database. With a <code>Calendar</code>
     * object, the driver can calculate the timestamp taking into account a
     * custom timezone. If no <code>Calendar</code> object is specified, the
     * driver uses the default timezone, which is that of the virtual machine
     * running the application.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the parameter value
     * @param cal the <code>Calendar</code> object the driver will use to
     *            construct the timestamp
     * @throws SQLException if a database access error occurs
     */
    void setTimestamp(int parameterIndex, Timestamp x, Calendar cal)
            throws SQLException;

    /**
     * Sets the designated parameter to SQL NULL. This version of the method
     * <code>setNull</code> should be used for user-defined types. Examples of
     * user-defined types include: DISTINCT, JAVA_OBJECT, and named array types.
     * <p/>
     * <b>Note:</b> To be portable, applications must give the SQL type code and
     * the fully-qualified SQL type name when specifying a NULL user-defined. In
     * the case of a user-defined type the name is the type name of the parameter
     * itself. For a <code>REF</code> parameter, the name is the type name of
     * the referenced type. If a JDBC driver does not need the type code or type
     * name information, it may ignore it. Although it is intended for
     * user-defined, this method may be used to set a null parameter of any
     * JDBC type. If the parameter does not have a user-defined, the given
     * <code>typeName</code> is ignored
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param sqlType a value from <code>java.sql.Types</code>
     * @param typeName the fully-qualified name of an SQL user-defined type;
     *                 ignored if the parameter is not a user-defined type
     * @throws SQLException if a database access error occurs
     */
    void setNull(int parameterIndex, int sqlType, String typeName)
            throws SQLException;

    /**
     * Sets the designated parameter to the given <code>java.net.URL</code>
     * value. The driver converts this to an SQL DATALINK value when it sends it
     * to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the <code>java.net.URL</code> object to be set
     * @throws SQLException if a database access error occurs
     */
    void setURL(int parameterIndex, URL x) throws SQLException;
}
