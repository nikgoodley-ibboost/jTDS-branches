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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Map;

/**
 * jTDS implementation of the java.sql.CallableStatement interface.
 *<p>
 * Implementation note:
 * <ol>
 * <li>This class is a simple subclass of PreparedStatement and mainly adds support for
 * setting parameters.
 * <li>The class supports named parameters in a similar way to the
 * patch supplied by Tommy Sandstrom to the original jTDS code.
 * </ol>
 *
 * @author Mike Hutchinson
 * @version $Id: JtdsCallableStatement.java,v 1.23.2.4 2009-12-30 11:37:21 ickzon Exp $
 */
public class JtdsCallableStatement extends JtdsPreparedStatement implements CallableStatement {
    /** Last parameter retrieved was null. */
    protected boolean paramWasNull;

    /**
     * Construct a CallableStatement object.
     *
     * @param connection The connection owning this statement.
     * @param sql The SQL statement specifying the procedure to call.
     * @param resultSetType The result set type eg FORWARD_ONLY.
     * @param concurrency   The result set concurrency eg READ_ONLY.
     * @throws SQLException
     */
    JtdsCallableStatement(JtdsConnection connection, String sql, int resultSetType, int concurrency)
        throws SQLException {
        super(connection, sql, resultSetType, concurrency, false);
    }

    /**
     * Find a parameter by name.
     *
     * @param name The name of the parameter to locate.
     * @param set True if function is called from a set / register method.
     * @return The parameter index as an <code>int</code>.
     * @throws SQLException
     */
    final int findParameter(String name, boolean set)
        throws SQLException {
        checkOpen();

        // no need to force the user to care about the param syntax
        if(! name.startsWith( "@" ))
           name = "@" + name;

        for (int i = 0; i < parameters.length; i++){
            if (parameters[i].name != null && parameters[i].name.equalsIgnoreCase(name))
                return i + 1;
        }

        if (set && !name.equalsIgnoreCase("@return_status")) {
            for (int i = 0; i < parameters.length; i++){
                if (parameters[i].name == null) {
                    parameters[i].name = name;

                    return i + 1;
                }
            }
        }

        throw new SQLException(Messages.get("error.callable.noparam", name), "07000");
    }

    /**
     * Retrieve the value of an output parameter.
     *
     * @param parameterIndex the ordinal position of the parameter
     * @return the parameter value as an <code>Object</code>
     * @throws SQLException if the parameter has not been set
     */
    protected Object getOutputValue(int parameterIndex)
            throws SQLException {
        checkOpen();
        ParamInfo parameter = getParameter(parameterIndex);
        if (!parameter.isOutput) {
            throw new SQLException(
                    Messages.get("error.callable.notoutput",
                            new Integer(parameterIndex)),
                    "07000");
        }
        Object value = parameter.getOutValue();
        paramWasNull = (value == null);
        return value;
    }

    /**
     * Check that this statement is still open.
     *
     * @throws SQLException if statement closed.
     */
    @Override
   protected void checkOpen() throws SQLException {
        if (isClosed()) {
            throw new SQLException(
                    Messages.get("error.generic.closed", "CallableStatement"), "HY010");
        }
    }

    /**
     * Execute the SQL batch on a MS server.
     * @param size the total size of the batch.
     * @param executeSize the maximum number of statements to send in one request.
     * @param counts the returned update counts.
     * @return Chained exceptions linked to a <code>SQLException</code>.
     * @throws SQLException
     */
    @Override
   protected SQLException executeMSBatch(int size, int executeSize, ArrayList counts)
    throws SQLException {
        if (parameters.length == 0) {
            // No parameters so we can execute as a simple batch
            return super.executeMSBatch(size, executeSize, counts);
        }
        SQLException sqlEx = null;
        for (int i = 0; i < size;) {
            Object value = batchValues.get(i);
            ++i;
            // Execute batch now if max size reached or end of batch
            boolean executeNow = (i % executeSize == 0) || i == size;

            tds.startBatch();
            tds.executeSQL(sql, procName, (ParamInfo[]) value, false, 0, -1, -1, executeNow);

            // If the batch has been sent, process the results
            if (executeNow) {
                sqlEx = tds.getBatchCounts(counts, sqlEx);

                // If a serious error then we stop execution now as count
                // is too small.
                if (sqlEx != null && counts.size() != i) {
                    break;
                }
            }
        }
        return sqlEx;
    }

    /**
     * Execute the SQL batch on a Sybase server.
     * <p/>
     * For the rare case of CallableStatement batches each statement is executed individually. This ensures that
     * problems with the server reading into the middle of a statement are avoided. See bug report [1374518] for more
     * details.
     *
     * @param size        the total size of the batch
     * @param executeSize the maximum number of statements to send in one request (ignored for this version of the
     *                    method as only one statement will be sent at a time)
     * @param counts the returned update counts
     * @return chained exceptions linked to a <code>SQLException</code>
     * @throws SQLException if a serious error occurs during execution
     */
    @Override
   protected SQLException executeSybaseBatch(int size, int executeSize, ArrayList counts)
    throws SQLException
    {
        if (parameters.length == 0) {
            // No parameters so we can execute as a simple batch
            return super.executeSybaseBatch(size, executeSize, counts);
        }

        SQLException sqlEx = null;

        for (int i = 0; i < size;) {
            Object value = batchValues.get(i);
            ++i;
            tds.executeSQL(sql, procName, (ParamInfo[]) value, false, 0, -1, -1, true);

            // If the batch has been sent, process the results
            sqlEx = tds.getBatchCounts(counts, sqlEx);

            // If a serious error then we stop execution now as count
            // is too small.
            if (sqlEx != null && counts.size() != i) {
                break;
            }
        }
        return sqlEx;
    }


// ---------- java.sql.CallableStatement methods follow ----------

    @Override
   public boolean wasNull() throws SQLException {
        checkOpen();

        return paramWasNull;
    }

    @Override
   public byte getByte(int parameterIndex) throws SQLException {
        return ((Integer) Support.convert(this, getOutputValue(parameterIndex), java.sql.Types.TINYINT, null)).byteValue();
    }

    @Override
   public double getDouble(int parameterIndex) throws SQLException {
        return ((Double) Support.convert(this, getOutputValue(parameterIndex), java.sql.Types.DOUBLE, null)).doubleValue();
    }

    @Override
   public float getFloat(int parameterIndex) throws SQLException {
        return ((Float) Support.convert(this, getOutputValue(parameterIndex), java.sql.Types.REAL, null)).floatValue();
    }

    @Override
   public int getInt(int parameterIndex) throws SQLException {
        return ((Integer) Support.convert(this, getOutputValue(parameterIndex), java.sql.Types.INTEGER, null)).intValue();
    }

    @Override
   public long getLong(int parameterIndex) throws SQLException {
        return ((Long) Support.convert(this, getOutputValue(parameterIndex), java.sql.Types.BIGINT, null)).longValue();
    }

    @Override
   public short getShort(int parameterIndex) throws SQLException {
        return ((Integer) Support.convert(this, getOutputValue(parameterIndex), java.sql.Types.SMALLINT, null)).shortValue();
    }

    @Override
   public boolean getBoolean(int parameterIndex) throws SQLException {
        return ((Boolean) Support.convert(this, getOutputValue(parameterIndex), BOOLEAN, null)).booleanValue();
    }

    @Override
   public byte[] getBytes(int parameterIndex) throws SQLException {
        checkOpen();
        return ((byte[]) Support.convert(this, getOutputValue(parameterIndex), java.sql.Types.VARBINARY, connection.getCharset()));
    }

    @Override
   public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
        if (sqlType == java.sql.Types.DECIMAL
            || sqlType == java.sql.Types.NUMERIC) {
            registerOutParameter(parameterIndex, sqlType, TdsData.DEFAULT_SCALE);
        } else {
            registerOutParameter(parameterIndex, sqlType, 0);
        }
    }

    @Override
   public void registerOutParameter(int parameterIndex, int sqlType, int scale)
        throws SQLException {
        checkOpen();

        if (scale < 0 || scale > connection.getMaxPrecision()) {
            throw new SQLException(Messages.get("error.generic.badscale"), "HY092");
        }

        ParamInfo pi = getParameter(parameterIndex);

        pi.isOutput = true;

        if ("ERROR".equals(Support.getJdbcTypeName(sqlType))) {
            throw new SQLException(Messages.get("error.generic.badtype",
                    Integer.toString(sqlType)), "HY092");
        }

        if (sqlType == java.sql.Types.CLOB) {
            pi.jdbcType = java.sql.Types.LONGVARCHAR;
        } else if (sqlType == java.sql.Types.BLOB) {
            pi.jdbcType = java.sql.Types.LONGVARBINARY;
        } else {
            pi.jdbcType = sqlType;
        }

        pi.scale = scale;
    }

    @Override
   public Object getObject(int parameterIndex) throws SQLException {
        Object value = getOutputValue(parameterIndex);

        // Don't return UniqueIdentifier objects as the user won't know how to
        // handle them
        if (value instanceof UniqueIdentifier) {
            return value.toString();
        }

        // If the user requested String/byte[] instead of LOBs, do the conversion
        if (!connection.getUseLOBs()) {
            value = Support.convertLOB(value);
        }

        return value;
    }

    @Override
   public String getString(int parameterIndex) throws SQLException {
        checkOpen();
        return (String) Support.convert(this, getOutputValue(parameterIndex),
                java.sql.Types.VARCHAR, connection.getCharset());
    }

    @Override
   public void registerOutParameter(int parameterIndex, int sqlType, String typeName)
        throws SQLException {
        notImplemented("CallableStatement.registerOutParameter(int, int, String");
    }

    @Override
   public byte getByte(String parameterName) throws SQLException {
        return getByte(findParameter(parameterName, false));
    }

    @Override
   public double getDouble(String parameterName) throws SQLException {
        return getDouble(findParameter(parameterName, false));
    }

    @Override
   public float getFloat(String parameterName) throws SQLException {
        return getFloat(findParameter(parameterName, false));
    }

    @Override
   public int getInt(String parameterName) throws SQLException {
        return getInt(findParameter(parameterName, false));
    }

    @Override
   public long getLong(String parameterName) throws SQLException {
        return getLong(findParameter(parameterName, false));
    }

    @Override
   public short getShort(String parameterName) throws SQLException {
        return getShort(findParameter(parameterName, false));
    }

    @Override
   public boolean getBoolean(String parameterName) throws SQLException {
        return getBoolean(findParameter(parameterName, false));
    }

    @Override
   public byte[] getBytes(String parameterName) throws SQLException {
        return getBytes(findParameter(parameterName, false));
    }

    @Override
   public void setByte(String parameterName, byte x) throws SQLException {
        setByte(findParameter(parameterName, true), x);
    }

    @Override
   public void setDouble(String parameterName, double x) throws SQLException {
        setDouble(findParameter(parameterName, true), x);
    }

    @Override
   public void setFloat(String parameterName, float x) throws SQLException {
        setFloat(findParameter(parameterName, true), x);
    }

    @Override
   public void registerOutParameter(String parameterName, int sqlType)
        throws SQLException {
        registerOutParameter(findParameter(parameterName, true), sqlType);
    }

    @Override
   public void setInt(String parameterName, int x) throws SQLException {
        setInt(findParameter(parameterName, true), x);
    }

    @Override
   public void setNull(String parameterName, int sqlType) throws SQLException {
        setNull(findParameter(parameterName, true), sqlType);
    }

    @Override
   public void registerOutParameter(String parameterName, int sqlType, int scale)
        throws SQLException {
        registerOutParameter(findParameter(parameterName, true), sqlType, scale);
    }

    @Override
   public void setLong(String parameterName, long x) throws SQLException {
        setLong(findParameter(parameterName, true), x);
    }

    @Override
   public void setShort(String parameterName, short x) throws SQLException {
        setShort(findParameter(parameterName, true), x);
    }

    @Override
   public void setBoolean(String parameterName, boolean x) throws SQLException {
        setBoolean(findParameter(parameterName, true), x);
    }

    @Override
   public void setBytes(String parameterName, byte[] x) throws SQLException {
        setBytes(findParameter(parameterName, true), x);
    }

    @Override
   public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
        return (BigDecimal) Support.convert(this,
                getOutputValue(parameterIndex), java.sql.Types.DECIMAL, null);
    }

    @Override
   public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
        BigDecimal bd = (BigDecimal) Support.convert(this,
                getOutputValue(parameterIndex), java.sql.Types.DECIMAL, null);

        return bd.setScale(scale);
    }

    @Override
   public URL getURL(int parameterIndex) throws SQLException {
        checkOpen();
        String url = (String) Support.convert(this,
                getOutputValue(parameterIndex), java.sql.Types.VARCHAR,
                connection.getCharset());

        try {
            return new java.net.URL(url);
        } catch (MalformedURLException e) {
            throw new SQLException(Messages.get("error.resultset.badurl", url), "22000");
        }
    }

    @Override
   public Array getArray(int parameterIndex) throws SQLException {
        notImplemented("CallableStatement.getArray");
        return null;
    }

    @Override
   public Blob getBlob(int parameterIndex) throws SQLException {
        byte[] value = getBytes(parameterIndex);

        if (value == null) {
            return null;
        }

        return new BlobImpl(connection, value);
    }

    @Override
   public Clob getClob(int parameterIndex) throws SQLException {
        String value = getString(parameterIndex);

        if (value == null) {
            return null;
        }

        return new ClobImpl(connection, value);
    }

    @Override
   public Date getDate(int parameterIndex) throws SQLException {
        return (java.sql.Date) Support.convert(this,
                getOutputValue(parameterIndex), java.sql.Types.DATE, null);
    }

    @Override
   public Ref getRef(int parameterIndex) throws SQLException {
        notImplemented("CallableStatement.getRef");
        return null;
    }

    @Override
   public Time getTime(int parameterIndex) throws SQLException {
        return (Time) Support.convert(this, getOutputValue(parameterIndex), java.sql.Types.TIME, null);
    }

    @Override
   public Timestamp getTimestamp(int parameterIndex) throws SQLException {
        return (Timestamp) Support.convert(this, getOutputValue(parameterIndex), java.sql.Types.TIMESTAMP, null);
    }

    @Override
   public void setAsciiStream(String parameterName, InputStream x, int length)
        throws SQLException {
        setAsciiStream(findParameter(parameterName, true), x, length);
    }

    @Override
   public void setBinaryStream(String parameterName, InputStream x, int length)
        throws SQLException {
        setBinaryStream(findParameter(parameterName, true), x, length);
    }

    @Override
   public void setCharacterStream(String parameterName, Reader reader, int length)
        throws SQLException {
        setCharacterStream(findParameter(parameterName, true), reader, length);
    }

    @Override
   public Object getObject(String parameterName) throws SQLException {
        return getObject(findParameter(parameterName, false));
    }

    @Override
   public void setObject(String parameterName, Object x) throws SQLException {
        setObject(findParameter(parameterName, true), x);
    }

    @Override
   public void setObject(String parameterName, Object x, int targetSqlType)
        throws SQLException {
        setObject(findParameter(parameterName, true), x, targetSqlType);
    }

    @Override
   public void setObject(String parameterName, Object x, int targetSqlType, int scale)
        throws SQLException {
        setObject(findParameter(parameterName, true), x, targetSqlType, scale);
    }

    @Override
   public Object getObject(int parameterIndex, Map map) throws SQLException {
        notImplemented("CallableStatement.getObject(int, Map)");
        return null;
    }

    @Override
   public String getString(String parameterName) throws SQLException {
        return getString(findParameter(parameterName, false));
    }

    @Override
   public void registerOutParameter(String parameterName, int sqlType, String typeName)
        throws SQLException {
        notImplemented("CallableStatement.registerOutParameter(String, int, String");
    }

    @Override
   public void setNull(String parameterName, int sqlType, String typeName)
        throws SQLException {
        notImplemented("CallableStatement.setNull(String, int, String");
    }

    @Override
   public void setString(String parameterName, String x) throws SQLException {
        setString(findParameter(parameterName, true), x);
    }

    @Override
   public BigDecimal getBigDecimal(String parameterName) throws SQLException {
        return getBigDecimal(findParameter(parameterName, false));
    }

    @Override
   public void setBigDecimal(String parameterName, BigDecimal x)
        throws SQLException {
        setBigDecimal(findParameter(parameterName, true), x);
    }

    @Override
   public URL getURL(String parameterName) throws SQLException {
        return getURL(findParameter(parameterName, false));
    }

    @Override
   public void setURL(String parameterName, URL x) throws SQLException {
        setObject(findParameter(parameterName, true), x);
    }

    @Override
   public Array getArray(String parameterName) throws SQLException {
        return getArray(findParameter(parameterName, false));
    }

    @Override
   public Blob getBlob(String parameterName) throws SQLException {
        return getBlob(findParameter(parameterName, false));
    }

    @Override
   public Clob getClob(String parameterName) throws SQLException {
        return getClob(findParameter(parameterName, false));
    }

    @Override
   public Date getDate(String parameterName) throws SQLException {
        return getDate(findParameter(parameterName, false));
    }

    @Override
   public void setDate(String parameterName, Date x) throws SQLException {
        setDate(findParameter(parameterName, true), x);
    }

    @Override
   public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
        java.sql.Date date = getDate(parameterIndex);

        if (date != null && cal != null) {
            date = new java.sql.Date(Support.timeToZone(date, cal));
        }

        return date;
    }

    @Override
   public Ref getRef(String parameterName) throws SQLException {
        return getRef(findParameter(parameterName, false));
    }

    @Override
   public Time getTime(String parameterName) throws SQLException {
        return getTime(findParameter(parameterName, false));
    }

    @Override
   public void setTime(String parameterName, Time x) throws SQLException {
        setTime(findParameter(parameterName, true), x);
    }

    @Override
   public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
        java.sql.Time time = getTime(parameterIndex);

        if (time != null && cal != null) {
            time = new java.sql.Time(Support.timeToZone(time, cal));
        }

        return time;
    }

    @Override
   public Timestamp getTimestamp(String parameterName) throws SQLException {
        return getTimestamp(findParameter(parameterName, false));
    }

    @Override
   public void setTimestamp(String parameterName, Timestamp x) throws SQLException {
        setTimestamp(findParameter(parameterName, true), x);
    }

    @Override
   public Timestamp getTimestamp(int parameterIndex, Calendar cal)
        throws SQLException {
        Timestamp timestamp = getTimestamp(parameterIndex);

        if (timestamp != null && cal != null) {
            timestamp = new Timestamp(Support.timeToZone(timestamp, cal));
        }

        return timestamp;
    }

    @Override
   public Object getObject(String parameterName, Map map) throws SQLException {
         return getObject(findParameter(parameterName, false), map);
    }

    @Override
   public Date getDate(String parameterName, Calendar cal) throws SQLException {
        return getDate(findParameter(parameterName, false), cal);
    }

    @Override
   public Time getTime(String parameterName, Calendar cal) throws SQLException {
        return getTime(findParameter(parameterName, false), cal);
    }

    @Override
   public Timestamp getTimestamp(String parameterName, Calendar cal)
        throws SQLException {
        return getTimestamp(findParameter(parameterName, false), cal);
    }

    @Override
   public void setDate(String parameterName, Date x, Calendar cal)
        throws SQLException {
        setDate(findParameter(parameterName, true), x, cal);
    }

    @Override
   public void setTime(String parameterName, Time x, Calendar cal)
        throws SQLException {
        setTime(findParameter(parameterName, true), x, cal);
    }

    @Override
   public void setTimestamp(String parameterName, Timestamp x, Calendar cal)
        throws SQLException {
        setTimestamp(findParameter(parameterName, true), x, cal);
    }

    /////// JDBC4 demarcation, do NOT put any JDBC3 code below this line ///////

    /* (non-Javadoc)
     * @see java.sql.CallableStatement#getCharacterStream(int)
     */
    @Override
   public Reader getCharacterStream(int parameterIndex) throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    /* (non-Javadoc)
     * @see java.sql.CallableStatement#getCharacterStream(java.lang.String)
     */
    @Override
   public Reader getCharacterStream(String parameterName) throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    /* (non-Javadoc)
     * @see java.sql.CallableStatement#getNCharacterStream(int)
     */
    @Override
   public Reader getNCharacterStream(int parameterIndex) throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    /* (non-Javadoc)
     * @see java.sql.CallableStatement#getNCharacterStream(java.lang.String)
     */
    @Override
   public Reader getNCharacterStream(String parameterName) throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    /* (non-Javadoc)
     * @see java.sql.CallableStatement#getNClob(int)
     */
    @Override
   public NClob getNClob(int parameterIndex) throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    /* (non-Javadoc)
     * @see java.sql.CallableStatement#getNClob(java.lang.String)
     */
    @Override
   public NClob getNClob(String parameterName) throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    /* (non-Javadoc)
     * @see java.sql.CallableStatement#getNString(int)
     */
    @Override
   public String getNString(int parameterIndex) throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    /* (non-Javadoc)
     * @see java.sql.CallableStatement#getNString(java.lang.String)
     */
    @Override
   public String getNString(String parameterName) throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    /* (non-Javadoc)
     * @see java.sql.CallableStatement#getRowId(int)
     */
    @Override
   public RowId getRowId(int parameterIndex) throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    /* (non-Javadoc)
     * @see java.sql.CallableStatement#getRowId(java.lang.String)
     */
    @Override
   public RowId getRowId(String parameterName) throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    /* (non-Javadoc)
     * @see java.sql.CallableStatement#getSQLXML(int)
     */
    @Override
   public SQLXML getSQLXML(int parameterIndex) throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    /* (non-Javadoc)
     * @see java.sql.CallableStatement#getSQLXML(java.lang.String)
     */
    @Override
   public SQLXML getSQLXML(String parameterName) throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    /* (non-Javadoc)
     * @see java.sql.CallableStatement#setAsciiStream(java.lang.String, java.io.InputStream)
     */
    @Override
   public void setAsciiStream(String parameterName, InputStream x)
            throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    /* (non-Javadoc)
     * @see java.sql.CallableStatement#setAsciiStream(java.lang.String, java.io.InputStream, long)
     */
    @Override
   public void setAsciiStream(String parameterName, InputStream x, long length)
            throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    /* (non-Javadoc)
     * @see java.sql.CallableStatement#setBinaryStream(java.lang.String, java.io.InputStream)
     */
    @Override
   public void setBinaryStream(String parameterName, InputStream x)
            throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    /* (non-Javadoc)
     * @see java.sql.CallableStatement#setBinaryStream(java.lang.String, java.io.InputStream, long)
     */
    @Override
   public void setBinaryStream(String parameterName, InputStream x, long length)
            throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    /* (non-Javadoc)
     * @see java.sql.CallableStatement#setBlob(java.lang.String, java.sql.Blob)
     */
    @Override
   public void setBlob(String parameterName, Blob x) throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    /* (non-Javadoc)
     * @see java.sql.CallableStatement#setBlob(java.lang.String, java.io.InputStream)
     */
    @Override
   public void setBlob(String parameterName, InputStream inputStream)
            throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    /* (non-Javadoc)
     * @see java.sql.CallableStatement#setBlob(java.lang.String, java.io.InputStream, long)
     */
    @Override
   public void setBlob(String parameterName, InputStream inputStream,
            long length) throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    /* (non-Javadoc)
     * @see java.sql.CallableStatement#setCharacterStream(java.lang.String, java.io.Reader)
     */
    @Override
   public void setCharacterStream(String parameterName, Reader reader)
            throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    /* (non-Javadoc)
     * @see java.sql.CallableStatement#setCharacterStream(java.lang.String, java.io.Reader, long)
     */
    @Override
   public void setCharacterStream(String parameterName, Reader reader,
            long length) throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    /* (non-Javadoc)
     * @see java.sql.CallableStatement#setClob(java.lang.String, java.sql.Clob)
     */
    @Override
   public void setClob(String parameterName, Clob x) throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    /* (non-Javadoc)
     * @see java.sql.CallableStatement#setClob(java.lang.String, java.io.Reader)
     */
    @Override
   public void setClob(String parameterName, Reader reader)
            throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    /* (non-Javadoc)
     * @see java.sql.CallableStatement#setClob(java.lang.String, java.io.Reader, long)
     */
    @Override
   public void setClob(String parameterName, Reader reader, long length)
            throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    /* (non-Javadoc)
     * @see java.sql.CallableStatement#setNCharacterStream(java.lang.String, java.io.Reader)
     */
    @Override
   public void setNCharacterStream(String parameterName, Reader value)
            throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    /* (non-Javadoc)
     * @see java.sql.CallableStatement#setNCharacterStream(java.lang.String, java.io.Reader, long)
     */
    @Override
   public void setNCharacterStream(String parameterName, Reader value,
            long length) throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    /* (non-Javadoc)
     * @see java.sql.CallableStatement#setNClob(java.lang.String, java.sql.NClob)
     */
    @Override
   public void setNClob(String parameterName, NClob value) throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    /* (non-Javadoc)
     * @see java.sql.CallableStatement#setNClob(java.lang.String, java.io.Reader)
     */
    @Override
   public void setNClob(String parameterName, Reader reader)
            throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    /* (non-Javadoc)
     * @see java.sql.CallableStatement#setNClob(java.lang.String, java.io.Reader, long)
     */
    @Override
   public void setNClob(String parameterName, Reader reader, long length)
            throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    /* (non-Javadoc)
     * @see java.sql.CallableStatement#setNString(java.lang.String, java.lang.String)
     */
    @Override
   public void setNString(String parameterName, String value)
            throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    /* (non-Javadoc)
     * @see java.sql.CallableStatement#setRowId(java.lang.String, java.sql.RowId)
     */
    @Override
   public void setRowId(String parameterName, RowId x) throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    /* (non-Javadoc)
     * @see java.sql.CallableStatement#setSQLXML(java.lang.String, java.sql.SQLXML)
     */
    @Override
   public void setSQLXML(String parameterName, SQLXML xmlObject)
            throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    //// JDBC4.1 demarcation, do NOT put any JDBC3/4.0 code below this line ////

    @Override
    public void closeOnCompletion() throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    @Override
    public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    @Override
    public <T> T getObject(String parameterName, Class<T> type) throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }
}