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

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.util.ArrayList;
import java.nio.ByteBuffer;

import net.sourceforge.jtds.util.Logger;

/**
 * This class implements the Sybase TDS 5.0 protocol.
 *
 * @author Mike Hutchinson
 * @author Matt Brinkley
 * @author Alin Sinpalean
 * @author FreeTDS project
 * @version $Id: TdsCore50.java,v 1.4 2009-07-31 12:54:10 ickzon Exp $
 */
class TdsCore50 extends TdsCore {

    //
    // Sybase capability flags
    //
    /** Sybase char and binary > 255.*/
    protected static final int SYB_LONGDATA    = 1;
    /** Sybase date and time data types.*/
    protected static final int SYB_DATETIME    = 2;
    /** Sybase nullable bit type.*/
    protected static final int SYB_BITNULL     = 4;
    /** Sybase extended column meta data.*/
    protected static final int SYB_EXTCOLINFO  = 8;
    /** Sybase univarchar etc. */
    protected static final int SYB_UNICODE     = 16;
    /** Sybase 15+ unitext. */
    protected static final int SYB_UNITEXT     = 32;
    /** Sybase 15+ bigint. */
    protected static final int SYB_BIGINT      = 64;
    /** Sybase capability mask.*/
    protected int sybaseInfo;
    
    //
    // Sybase / SQL Server 6.5 Login database security error message codes
    //
    /** Error 916: Server user id ? is not a valid user in database '?'.*/
    private static final int ERR_INVALID_USER   = 916;
    /** Error 10351: Server user id ? is not a valid user in database '?'.*/
    static final int ERR_INVALID_USER_2 = 10351;
    /** Error 4001: Cannot open default database '?'.*/
    static final int ERR_NO_DEFAULT_DB  = 4001;

    /*
     * Constants for variable length data types
     */
    private static final int VAR_MAX               = 255;
    private static final int SYB_LONGVAR_MAX       = 16384;
    private static final int SYB_CHUNK_SIZE        = 8192;
    

    /**
     * Static block to initialise TDS data type descriptors.
     */
    static {//                             SQL Type       Size Prec scale DS signed TDS8 Col java Type
        types[SYBCHAR]      = new TypeInfo("char",          -1, -1, 0, 0, java.sql.Types.CHAR);
        types[SYBVARCHAR]   = new TypeInfo("varchar",       -1, -1, 0, 0, java.sql.Types.VARCHAR);
        types[SYBINTN]      = new TypeInfo("int",           -1, 10, 0,11, java.sql.Types.INTEGER);
        types[SYBINT1]      = new TypeInfo("tinyint",        1,  3, 0, 4, java.sql.Types.TINYINT);
        types[SYBINT2]      = new TypeInfo("smallint",       2,  5, 0, 6, java.sql.Types.SMALLINT);
        types[SYBINT4]      = new TypeInfo("int",            4, 10, 0,11, java.sql.Types.INTEGER);
        types[SYBINT8]      = new TypeInfo("bigint",         8, 19, 0,20, java.sql.Types.BIGINT);
        types[SYBFLT8]      = new TypeInfo("float",          8, 15, 0,24, java.sql.Types.DOUBLE);
        types[SYBDATETIME]  = new TypeInfo("datetime",       8, 23, 3,23, java.sql.Types.TIMESTAMP);
        types[SYBBIT]       = new TypeInfo("bit",            1,  1, 0, 1, java.sql.Types.BIT);
        types[SYBTEXT]      = new TypeInfo("text",          -4,  0, 0, 0, java.sql.Types.CLOB);
        types[SYBNTEXT]     = new TypeInfo("ntext",         -4,  0, 0, 0, java.sql.Types.CLOB);
        types[SYBUNITEXT]   = new TypeInfo("unitext",       -4,  0, 0, 0, java.sql.Types.CLOB);
        types[SYBIMAGE]     = new TypeInfo("image",         -4,  0, 0, 0, java.sql.Types.BLOB);
        types[SYBMONEY4]    = new TypeInfo("smallmoney",     4, 10, 4,12, java.sql.Types.DECIMAL);
        types[SYBMONEY]     = new TypeInfo("money",          8, 19, 4,21, java.sql.Types.DECIMAL);
        types[SYBDATETIME4] = new TypeInfo("smalldatetime",  4, 16, 0,19, java.sql.Types.TIMESTAMP);
        types[SYBREAL]      = new TypeInfo("real",           4,  7, 0,14, java.sql.Types.REAL);
        types[SYBBINARY]    = new TypeInfo("binary",        -1, -3, 0, 0, java.sql.Types.BINARY);
        types[SYBVOID]      = new TypeInfo("void",          -1,  1, 0, 1, 0);
        types[SYBVARBINARY] = new TypeInfo("varbinary",     -1, -3, 0, 0, java.sql.Types.VARBINARY);
        types[SYBNVARCHAR]  = new TypeInfo("nvarchar",      -1, -1, 0, 0, java.sql.Types.VARCHAR);
        types[SYBBITN]      = new TypeInfo("bit",           -1,  1, 0, 1, java.sql.Types.BIT);
        types[SYBNUMERIC]   = new TypeInfo("numeric",       -1,  0, 0, 0, java.sql.Types.NUMERIC);
        types[SYBDECIMAL]   = new TypeInfo("decimal",       -1,  0, 0, 0, java.sql.Types.DECIMAL);
        types[SYBFLTN]      = new TypeInfo("float",         -1, 15, 0,24, java.sql.Types.DOUBLE);
        types[SYBMONEYN]    = new TypeInfo("money",         -1, 19, 4,21, java.sql.Types.DECIMAL);
        types[SYBDATETIMN]  = new TypeInfo("datetime",      -1, 23, 3,23, java.sql.Types.TIMESTAMP);
        types[SYBDATE]      = new TypeInfo("date",           4, 10, 0,10, java.sql.Types.DATE);
        types[SYBTIME]      = new TypeInfo("time",           4,  8, 0, 8, java.sql.Types.TIME);
        types[SYBDATEN]     = new TypeInfo("date",          -1, 10, 0,10, java.sql.Types.DATE);
        types[SYBTIMEN]     = new TypeInfo("time",          -1,  8, 0, 8, java.sql.Types.TIME);
        types[XSYBCHAR]     = new TypeInfo("char",          -2,  0, 0, 0, java.sql.Types.CHAR);
        types[XSYBVARCHAR]  = new TypeInfo("varchar",       -2,  0, 0, 0, java.sql.Types.VARCHAR);
        types[XSYBNVARCHAR] = new TypeInfo("nvarchar",      -2,  0, 0, 0, java.sql.Types.VARCHAR);
        types[XSYBNCHAR]    = new TypeInfo("nchar",         -2,  0, 0, 0, java.sql.Types.CHAR);
        types[XSYBVARBINARY]= new TypeInfo("varbinary",     -2,  0, 0, 0, java.sql.Types.VARBINARY);
        types[XSYBBINARY]   = new TypeInfo("binary",        -2,  0, 0, 0, java.sql.Types.BINARY);
        types[SYBLONGBINARY]= new TypeInfo("varbinary",     -5, -1, 0, 0, java.sql.Types.BINARY);
        types[SYBSINT1]     = new TypeInfo("tinyint",        1,  2, 0, 3, java.sql.Types.TINYINT);
        types[SYBUINT2]     = new TypeInfo("unsigned smallint", 2,  5, 0,6, java.sql.Types.INTEGER);
        types[SYBUINT4]     = new TypeInfo("unsigned int",   4, 10, 0,11, java.sql.Types.BIGINT);
        types[SYBUINT8]     = new TypeInfo("unsigned bigint",8, 20, 0,20, java.sql.Types.DECIMAL);
        types[SYBUINTN]     = new TypeInfo("unsigned int",  -1, 10, 0,11, java.sql.Types.BIGINT);
        types[SYBUNIQUE]    = new TypeInfo("uniqueidentifier",-1,36, 0,36,java.sql.Types.CHAR);
        types[SYBVARIANT]   = new TypeInfo("sql_variant",   -5,  0, 0,8000, java.sql.Types.VARCHAR);
        types[SYBSINT8]     = new TypeInfo("bigint",         8, 19, 0,20, java.sql.Types.BIGINT);
    }

    /**
     * Construct a TdsCore object.
     *
     * @param connection The connection which owns this object.
     * @param socket The TDS socket instance.
     * @param serverType The appropriate server type constant.
     */
    TdsCore50(final ConnectionImpl connection,
              final TdsSocket socket,
              final int serverType) {
        super(connection, socket, serverType, TDS50);
    }

    /**
     * Login to the SQL Server.
     *
     * @param cx         StatementImpl instance
     * @param serverName server host name
     * @param database   required database
     * @param user       user name
     * @param password   user password
     * @param domain     Windows NT domain (or null)
     * @param charset    required server character set
     * @param appName    application name
     * @param progName   library name
     * @param wsid       workstation ID
     * @param language   language to use for server messages
     * @param macAddress client network MAC address
     * @param packetSize required network packet size
     * @throws SQLException if an error occurs
     */
    void login(final StatementImpl cx,
               final String serverName,
               final String database,
               final String user,
               final String password,
               final String domain,
               final String charset,
               final String appName,
               final String progName,
               final String wsid,
               final String language,
               final String macAddress,
               final int packetSize)
        throws SQLException {
        Logger.printMethod(this, "login", null);
        try {
            send50LoginPkt(serverName, user, password,
                                charset, appName, progName, 
                                (wsid.length() == 0)? getHostName(): wsid,
                                language, packetSize);
            endOfResponse = false;
            nextToken(cx);

            while (!endOfResponse) {
                nextToken(cx);
            }

            cx.getMessages().checkErrors();
        } catch (IOException ioe) {
            SQLException sqle = new SQLException(
                    Messages.get(
                             "error.generic.ioerror", ioe.getMessage()),
                                 "08S01");
            sqle.initCause(ioe);
            throw sqle;
        }
    }
    
    /**
     * Inform the server that this connection is closing.
     * <p>
     * Used by Sybase a no-op for Microsoft.
     * @param cx StatementImpl instance
     */
    void close(final StatementImpl cx) throws SQLException {
        if (!isClosed) {
            try {
                if (!connection.isClosed()) {
                    socket.setTimeout(1000);
                    out.setPacketType(SYBQUERY_PKT);
                    out.write((byte)TDS_CLOSE_TOKEN);
                    out.write((byte)0);
                    out.flush();
                    endOfResponse = false;
                    cx.initContext();
                    clearResponseQueue(cx);
                }
            } catch (Exception e) {
                // Ignore any exceptions as this connection
                // is closing anyway.
            } finally {
                isClosed = true;
                in.close();
            }
        }
    }

    /**
     * Creates a light weight stored procedure on a Sybase server.
     *
     * @param cx                   the StatementImpl owning this prepare.
     * @param sql                  the SQL statement to prepare.
     * @param params               the actual parameter list
     * @param needCursor           true if a cursorprepare is required
     * @param dummy2               place holder parameter.
     * @param dummy3               place holder parameter.
     * @param returnKeys           set to true if statement will return
     *                             generated keys.                            
     * @return a <code>ProcEntry</code> instance.
     * @throws SQLException if an error occurs
     */
    ProcEntry prepare(final StatementImpl cx, 
                      final String sql, 
                      final ParamInfo[] params, 
                      final boolean needCursor, 
                      final int dummy2, 
                      final int dummy3,
                      final boolean returnKeys)
            throws SQLException 
    {
        checkOpen();
        
        if (returnKeys) {
            return null; // Sybase cannot use @@IDENTITY in proc
        }

        if (needCursor) {
            //
            // We are going to use the CachedResultSet so there is
            // no point in preparing the SQL as it will be discarded
            // in favour of a version with "FOR BROWSE" appended.
            //
            return null;
        }

        //
        // Check parameters set and obtain native types
        //
        for (int i = 0; i < params.length; i++) {
            if (params[i].sqlType.equals("text")
                || params[i].sqlType.equals("image")
                || params[i].sqlType.equals("unitext")) {
                return null; // Sybase does not support text/image params
            }
        }

        cx.initContext();

        if (sql == null || sql.length() == 0) {
            throw new IllegalArgumentException(
                    "sql parameter must be at least 1 character long.");
        }

        String procName = getProcName();

        if (procName == null || procName.length() != 11) {
            throw new IllegalArgumentException(
                    "procName parameter must be 11 characters long.");
        }

        try {
            lockNetwork();
            
            out.setPacketType(SYBQUERY_PKT);
            out.write((byte)TDS5_DYNAMIC_TOKEN);

            ByteBuffer bb = connection.getCharset().encode(sql);
            out.write((short) (bb.remaining() + 41));
            out.write((byte) 1);
            out.write((byte) 0);
            out.write((byte) 10);
            out.write(procName.substring(1), connection.getCharset());
            out.write((short) (bb.remaining() + 26));
            out.write("create proc ", connection.getCharset());
            out.write(procName.substring(1), connection.getCharset());
            out.write(" as ", connection.getCharset());
            out.write(bb);
            out.flush();
            endOfResponse = false;
            clearResponseQueue(cx);
            cx.getMessages().checkErrors();
            ProcEntry pe = new ProcEntry();
            pe.setName(procName);
            pe.setType(ProcEntry.PROCEDURE);
            pe.setColMetaData(cx.getColumns());
            pe.setParamMetaData(cx.getDynamicParameters());
            return pe;
        } catch (IOException ioe) {
            connection.setClosed();
            SQLException sqle = new SQLException(
                    Messages.get(
                            "error.generic.ioerror", ioe.getMessage()),
                    "08S01");
            sqle.initCause(ioe);
            throw sqle;
        } catch (SQLException e) {
            if ("08S01".equals(e.getSQLState())) {
                // Serious error rethrow
                throw e;
            }

            // This exception probably caused by failure to prepare
            // Return null;
            return null;
        } finally {
            freeNetwork();
        }
    }

    /**
     * Drops a Sybase temporary stored procedure.
     *
     * @param cx the StatementImpl instance.
     * @param procName the temporary procedure name
     * @throws SQLException if an error occurs
     */
    void unPrepare(final StatementImpl cx, final String procName)
            throws SQLException 
    {
        checkOpen();
        cx.initContext();

        if (procName == null || procName.length() != 11) {
            throw new IllegalArgumentException(
                    "procName parameter must be 11 characters long.");
        }

        try {
            lockNetwork();

            out.setPacketType(SYBQUERY_PKT);
            out.write((byte)TDS5_DYNAMIC_TOKEN);
            out.write((short) (15));
            out.write((byte) 4);
            out.write((byte) 0);
            out.write((byte) 10);
            out.write(procName.substring(1), connection.getCharset());
            out.write((short)0);
            out.flush();
            endOfResponse = false;
            clearResponseQueue(cx);
            cx.getMessages().checkErrors();
        } catch (IOException ioe) {
            connection.setClosed();
            SQLException sqle = new SQLException(
                    Messages.get(
                            "error.generic.ioerror", ioe.getMessage()),
                    "08S01");
            sqle.initCause(ioe);
            throw sqle;
        } catch (SQLException e) {
            if ("08S01".equals(e.getSQLState())) {
                // Serious error rethrow
                throw e;
            }
            // This exception probably caused by failure to unprepare
        } finally {
            freeNetwork();
        }
    }
    /**
     * Generates a unique statement key for a given SQL statement.
     *
     * @param sql        the sql statment to generate the key for
     * @param params     the statement parameters
     * @param catalog    the catalog is required for uniqueness on Microsoft
     *                   SQL Server
     * @param autoCommit true if in auto commit mode
     * @param cursor     true if this is a prepared cursor
     * @return the unique statement key
     */
     String getStatementKey(final String sql, 
                            final ParamInfo[] params,
                            final String catalog,
                            final boolean autoCommit, 
                            final boolean cursor) throws SQLException 
     {
        StringBuilder key;
        //
        // obtain native types
        //
        for (int i = 0; i < params.length; i++) {
           setNativeType(params[i]);
        }

        key = new StringBuilder(sql.length() + 2);
        // A simple key works for Sybase just need to know if
        // proc created in chained mode or not.
        key.append((autoCommit) ? 'T': 'F');
        // Now the actual SQL statement
        key.append(sql);

        return key.toString();
    }


// ---------------------- Private Methods from here ---------------------

    /**
     * TDS 5.0 Login Packet.
     * <P>
     * @param serverName server host name
     * @param user       user name
     * @param password   user password
     * @param charset    required server character set
     * @param appName    application name
     * @param progName   library name
     * @param wsid       workstation ID
     * @param language   server language for messages
     * @param packetSize required network packet size
     * @throws IOException if an I/O error occurs
     */
    private void send50LoginPkt(final String serverName,
                                final String user,
                                final String password,
                                final String charset,
                                final String appName,
                                final String progName,
                                final String wsid,
                                final String language,
                                final int packetSize)
        throws IOException 
    {
        final byte[] empty = new byte[0];

        out.setPacketType(LOGIN_PKT);
        putLoginString(wsid, 30);           // Host name
        putLoginString(user, 30);           // user name
        putLoginString(password, 30);       // password
        putLoginString("00000123", 30);     // hostproc (offset 93 0x5d)

        out.write((byte) 3); // type of int2
        out.write((byte) 1); // type of int4
        out.write((byte) 6); // type of char
        out.write((byte) 10);// type of flt
        out.write((byte) 9); // type of date
        out.write((byte) 1); // notify of use db
        out.write((byte) 1); // disallow dump/load and bulk insert
        out.write((byte) 0); // sql interface type
        out.write((byte) 0); // type of network connection

        out.write(empty, 0, 7);

        putLoginString(appName, 30);  // appname
        putLoginString(serverName, 30); // server name
        out.write((byte)0); // remote passwords
        ByteBuffer bb = connection.getCharset().encode(password);
        byte buf[] = new byte[bb.remaining()];
        bb.get(buf);
        out.write((byte)buf.length);
        out.write(buf, 0, 253);
        out.write((byte) (buf.length + 2));

        out.write((byte) 5);  // tds version
        out.write((byte) 0);

        out.write((byte) 0);
        out.write((byte) 0);
        putLoginString(progName, 10); // prog name

        out.write((byte) 5);  // prog version
        out.write((byte) 0);
        out.write((byte) 0);
        out.write((byte) 0);

        out.write((byte) 0);  // auto convert short
        out.write((byte) 0x0D); // type of flt4
        out.write((byte) 0x11); // type of date4

        putLoginString(language, 30);  // language

        out.write((byte) 1);  // notify on lang change
        out.write((short) 0);  // security label hierachy
        out.write((byte) 0);  // security encrypted
        out.write(empty, 0, 8);  // security components
        out.write((short) 0);  // security spare

        putLoginString(charset, 30); // Character set

        out.write((byte) 1);  // notify on charset change
        if (packetSize > 0) {
            putLoginString(String.valueOf(packetSize), 6); // specified length of tds packets
        } else {
            putLoginString(String.valueOf(MIN_PKT_SIZE), 6); // Default length of tds packets
        }
        out.write(empty, 0, 4);
        //
        // Request capabilities
        //
        // jTDS sends   01 0B 4F FF 85 EE EF 65 7F FF FF FF D6
        // Sybase 11.92 01 0A    00 00 00 23 61 41 CF FF FF C6
        // Sybase 12.52 01 0A    03 84 0A E3 61 41 FF FF FF C6
        // Sybase 15.00 01 0B 4F F7 85 EA EB 61 7F FF FF FF C6
        //
        // Response capabilities
        //
        // jTDS sends   02 0A 00 00 04 06 80 06 48 00 00 00
        // Sybase 11.92 02 0A 00 00 00 00 00 06 00 00 00 00
        // Sybase 12.52 02 0A 00 00 00 00 00 06 00 00 00 00
        // Sybase 15.00 02 0A 00 00 04 00 00 06 00 00 00 00
        //
        byte capString[] = {
            // Request capabilities
            (byte)0x01,(byte)0x0B,(byte)0x4F,(byte)0xFF,(byte)0x85,(byte)0xEE,(byte)0xEF,
            (byte)0x65,(byte)0x7F,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xD6,
            // Response capabilities
            (byte)0x02,(byte)0x0A,(byte)0x00,(byte)0x02,(byte)0x04,(byte)0x06,
            (byte)0x80,(byte)0x06,(byte)0x48,(byte)0x00,(byte)0x00,(byte)0x0C
        };

        if (packetSize == 0) {
            // Tell the server we will use its packet size
            capString[17] = 0;
        }
        out.write(TDS_CAP_TOKEN);
        out.write((short)capString.length);
        out.write(capString);

        out.flush(); // Send the packet
    }

    /**
     * Read the next TDS token from the response stream.
     *
     * @param cx the StatementImpl instance.
     * @throws SQLException if an I/O or protocol error occurs
     */
    protected void nextToken(final StatementImpl cx)
        throws SQLException
    {
        try {
            this.token = (byte)in.read();
            switch (this.token) {
                case TDS5_PARAMFMT2_TOKEN:
                    tds5ParamFmt2Token(cx);
                    break;
                case TDS_LANG_TOKEN:
                    tdsInvalidToken();
                    break;
                case TDS5_WIDE_RESULT:
                    tds5WideResultToken(cx);
                    break;
                case TDS_CLOSE_TOKEN:
                    tdsInvalidToken();
                    break;
                case TDS_RETURNSTATUS_TOKEN:
                    tdsReturnStatusToken(cx);
                    break;
                case TDS_PROCID:
                    tdsProcIdToken();
                    break;
                case TDS_OFFSETS_TOKEN:
                    tdsOffsetsToken();
                    break;
                case TDS_TABNAME_TOKEN:
                    tdsTableNameToken();
                    break;
                case TDS_COLINFO_TOKEN:
                    tdsColumnInfoToken(cx);
                    break;
                case TDS_COMP_NAMES_TOKEN:
                    tdsInvalidToken();
                    break;
                case TDS_COMP_RESULT_TOKEN:
                    tdsInvalidToken();
                    break;
                case TDS_ORDER_TOKEN:
                    tdsOrderByToken();
                    break;
                case TDS_ERROR_TOKEN:
                case TDS_INFO_TOKEN:
                    tdsErrorToken(cx);
                    break;
                case TDS_PARAM_TOKEN:
                    tdsOutputParamToken(cx);
                    break;
                case TDS_LOGINACK_TOKEN:
                    tdsLoginAckToken(cx);
                    break;
                case TDS_CONTROL_TOKEN:
                    tdsControlToken();
                    break;
                case TDS_ROW_TOKEN:
                    tdsRowToken(cx);
                    break;
                case TDS_ALTROW:
                    tdsInvalidToken();
                    break;
                case TDS5_PARAMS_TOKEN:
                    tds5ParamsToken(cx);
                    break;
                case TDS_CAP_TOKEN:
                    tdsCapabilityToken();
                    break;
                case TDS_ENVCHANGE_TOKEN:
                    tdsEnvChangeToken();
                    break;
                case TDS_MSG50_TOKEN:
                    tds5ErrorToken(cx);
                    break;
                case TDS5_DYNAMIC_TOKEN:
                    tds5DynamicToken();
                    break;
                case TDS5_PARAMFMT_TOKEN:
                    tds5ParamFmtToken(cx);
                    break;
                case TDS_RESULT_TOKEN:
                    tds5ResultToken(cx);
                    break;
                case TDS_DONE_TOKEN:
                case TDS_DONEPROC_TOKEN:
                case TDS_DONEINPROC_TOKEN:
                    tdsDoneToken(cx);
                    break;
                default:
                    tdsInvalidToken();
            }
        } catch (IOException ioe) {
            connection.setClosed();
            SQLException sqle = new SQLException(
                       Messages.get(
                                "error.generic.ioerror", ioe.getMessage()),
                                    "08S01");
            sqle.initCause(ioe);
            throw sqle;
        } catch (OutOfMemoryError err) {
            // Consume the rest of the response
            in.skipToEnd();
            endOfResponse = true;
            cancelPending = false;
            throw err;
        }
    }

    /**
     * Process TDS 5 Sybase 12+ Dynamic results parameter descriptor.
     * <p>When returning output parameters this token will be followed
     * by a TDS5_PARAMS_TOKEN with the actual data.
     * @param  cx the StatementImpl instance that owns this request.
     * @throws IOException
     */
    private void tds5ParamFmt2Token(final StatementImpl cx) throws IOException 
    {
        in.readInt(); // Packet length
        int paramCnt = in.readShort();
        ColInfo[] params = new ColInfo[paramCnt];
        for (int i = 0; i < paramCnt; i++) {
            //
            // Get the parameter details using the
            // ColInfo class as the server format is the same.
            //
            ColInfo col = new ColInfo();
            int colNameLen = in.read();
            col.realName = in.readString(colNameLen, connection.getCharset());
            int column_flags = in.readInt();   /*  Flags */
            col.isCaseSensitive = false;
            col.nullable    = ((column_flags & 0x20) != 0)?
                                        ResultSetMetaData.columnNullable:
                                        ResultSetMetaData.columnNoNulls;
            col.isWriteable = (column_flags & 0x10) != 0;
            col.isIdentity  = (column_flags & 0x40) != 0;
            col.isKey       = (column_flags & 0x02) != 0;
            col.isHidden    = (column_flags & 0x01) != 0;

            col.userType    = in.readInt();
            readType(col);
            // Skip locale information
            in.skip(1);
            params[i] = col;
        }
        cx.setDynamParamInfo(params);
        cx.setDynamParamData(new Object[paramCnt]);
    }

    /**
     * Process Sybase 12+ wide result token which provides enhanced
     * column meta data.
     * 
     * @param  cx the StatementImpl instance that owns this request.
     * @throws IOException
     */
     private void tds5WideResultToken(final StatementImpl cx)
         throws IOException
     {
         in.readInt(); // Packet length
         int colCnt   = in.readShort();
         ColInfo[] columns = new ColInfo[colCnt];
         this.tables  = null;

         for (int colNum = 0; colNum < colCnt; ++colNum) {
             ColInfo col = new ColInfo();
             //
             // Get the alias name
             //
             int nameLen = in.read();
             col.name  = in.readString(nameLen, connection.getCharset());
             //
             // Get the catalog name
             //
             nameLen = in.read();
             col.catalog = in.readString(nameLen, connection.getCharset());
             //
             // Get the schema name
             //
             nameLen = in.read();
             col.schema = in.readString(nameLen, connection.getCharset());
             //
             // Get the table name
             //
             nameLen = in.read();
             col.tableName = in.readString(nameLen, connection.getCharset());
             //
             // Get the column name
             //
             nameLen = in.read();
             col.realName  = in.readString(nameLen, connection.getCharset());
             if (col.name == null || col.name.length() == 0) {
                 col.name = col.realName;
             }
             int column_flags = in.readInt();   /*  Flags */
             col.isCaseSensitive = false;
             col.nullable    = ((column_flags & 0x20) != 0)?
                                    ResultSetMetaData.columnNullable:
                                         ResultSetMetaData.columnNoNulls;
             col.isWriteable = (column_flags & 0x10) != 0;
             col.isIdentity  = (column_flags & 0x40) != 0;
             col.isKey       = (column_flags & 0x02) != 0;
             col.isHidden    = (column_flags & 0x01) != 0;

             col.userType    = in.readInt();
             readType(col);
             // Skip locale information
             in.skip(1);
             columns[colNum] = col;
         }
         cx.setColumns(columns);
     }

    /**
     * Process stored procedure return status token.
     *
     * @param  cx the StatementImpl instance that owns this request.
     * @throws IOException
     */
    private void tdsReturnStatusToken(final StatementImpl cx) 
        throws IOException, SQLException 
    {
        cx.setReturnParam(in.readInt());
    }

    /**
     * Process procedure ID token.
     * <p>
     * Used by DBLIB to obtain the object id of a stored procedure.
     */
    private void tdsProcIdToken() throws IOException {
        in.skip(8);
    }

    /**
     * Process offsets token.
     * <p>
     * Used by DBLIB to return the offset of various keywords in a statement.
     * This saves the client from having to parse a SQL statement. Enabled with
     * <code>&quot;set offsets from on&quot;</code>.
     */
    private void tdsOffsetsToken() throws IOException {
        /*int keyword =*/ in.read();
        /*int unknown =*/ in.read();
        /*int offset  =*/ in.readShort();
    }


    /**
     * Process a table name token.
     * <p> Sent by select for browse or cursor functions.
     *
     * @throws IOException
     */
    private void tdsTableNameToken() throws IOException 
    {
        final int pktLen = in.readShort();
        int bytesRead = 0;
        ArrayList<TableMetaData> tableList = new ArrayList<TableMetaData>();

        while (bytesRead < pktLen) {
            int    nameLen;
            String tabName;
            TableMetaData table;
            // TDS 4.2 or TDS 5.0
            nameLen = in.read();
            bytesRead++;
            if (nameLen == 0) {
                continue; // Sybase/SQL 6.5 use a zero length name to terminate list
            }
            tabName = in.readString(nameLen, connection.getCharset());
            bytesRead += nameLen;
            table = new TableMetaData();
            // tabName can be a fully qualified name
            int dotPos = tabName.lastIndexOf('.');
            if (dotPos > 0) {
                table.name = tabName.substring(dotPos + 1);

                int nextPos = tabName.lastIndexOf('.', dotPos-1);
                if (nextPos + 1 < dotPos) {
                    table.schema = tabName.substring(nextPos + 1, dotPos);
                }
                dotPos = nextPos;
                nextPos = tabName.lastIndexOf('.', dotPos-1);
                if (nextPos + 1 < dotPos) {
                    table.catalog = tabName.substring(nextPos + 1, dotPos);
                }
            } else {
                table.name = tabName;
            }
            tableList.add(table);
        }
        if (tableList.size() > 0) {
            this.tables = tableList.toArray(new TableMetaData[tableList.size()]);
        }
    }

    /**
     * Process a column infomation token.
     * <p>Sent by select for browse or cursor functions.
     * @param  cx the StatementImpl instance that owns this request.
     * @throws IOException
     */
    private void tdsColumnInfoToken(final StatementImpl cx)
        throws IOException
    {
        final int pktLen = in.readShort();
        int bytesRead = 0;
        int columnIndex = 0;
        ColInfo[] columns = cx.getColumns();
        
        while (bytesRead < pktLen) {
            // Seems like all columns are always returned in the COL_INFO
            // packet and there might be more than 255 columns, so we'll
            // just increment a counter instead.
            // Ignore the column index.
            in.read();
            if (columnIndex >= columns.length) {
                throw new IOException("Column index " + (columnIndex + 1) +
                        " invalid in TDS_COLINFO packet");
            }
            ColInfo col = columns[columnIndex++];
            int tableIndex = in.read();
            // In some cases (e.g. if the user calls 'CREATE CURSOR'), the
            // TDS_TABNAME packet seems to be missing although the table index
            // in this packet is > 0. Weird.
            // If tables are available check for valid table index.
            if (tables != null && tableIndex > tables.length) {
                throw new IOException("Table index " + tableIndex +
                        " invalid in TDS_COLINFO packet");
            }
            byte flags = (byte)in.read(); // flags
            bytesRead += 3;

            if (tableIndex != 0 && tables != null) {
                TableMetaData table = tables[tableIndex-1];
                col.catalog   = table.catalog;
                col.schema    = table.schema;
                col.tableName = table.name;
            }

            col.isKey           = (flags & 0x08) != 0;
            col.isHidden        = (flags & 0x10) != 0;

            // If bit 5 is set, we have a column name
            if ((flags & 0x20) != 0) {
                final int nameLen = in.read();
                bytesRead += 1;
                final String colName = in.readString(nameLen, connection.getCharset());
                bytesRead += nameLen;
                col.realName = colName;
            }
        }
    }

    /**
     * Process an order by token.
     * <p>Sent to describe columns in an order by clause.
     * @throws IOException
     */
    private void tdsOrderByToken()
        throws IOException
    {
        // Skip this packet type
        int pktLen = in.readShort();
        in.skip(pktLen);
    }

    /**
     * Process a TD4/TDS7 error or informational message.
     *
     * @param  cx the StatementImpl instance that owns this request.
     * @throws IOException
     */
    private void tdsErrorToken(final StatementImpl cx)
    throws IOException
    {
        int pktLen = in.readShort(); // Packet length
        int sizeSoFar = 6;
        int number = in.readInt();
        in.read(); // state
        int severity = in.read();
        int msgLen = in.readShort();
        String message = in.readString(msgLen, connection.getCharset());
        sizeSoFar += 2 + msgLen;
        final int srvNameLen = in.read();
        in.readString(srvNameLen, connection.getCharset()); // server name
        sizeSoFar += 1 + srvNameLen;

        final int procNameLen = in.read();
        in.readString(procNameLen, connection.getCharset()); // proc name
        sizeSoFar += 1 + procNameLen;

        in.readShort(); // line
        sizeSoFar += 2;
        // Skip any EED information to read rest of packet
        if (pktLen - sizeSoFar > 0)
            in.skip(pktLen - sizeSoFar);

        if (this.token == TDS_ERROR_TOKEN)
        {
            if (severity < 10) {
                severity = 11; // Ensure treated as error
            }
            if (severity >= 20) {
                // A fatal error has occured, the connection will be closed by
                // the server immediately after the last TDS_DONE packet
                fatalError = true;
            }
        } else {
            if (severity > 9) {
                severity = 9; // Ensure treated as warning
            }
        }

        cx.getMessages().addDiagnostic(number, severity, message);
    }

    /**
     * Process output parameters.
     * </p>
     * Normally the output parameters are preceded by a TDS type 79
     * (procedure return value) record; however there are at least two
     * situations with TDS version 8 where this is not the case:
     * <ol>
     * <li>For the return value of a SQL 2000+ user defined function.</li>
     * <li>For a remote procedure call (server.database.user.procname) where
     * the 79 record is only sent if a result set is also returned by the remote
     * procedure. In this case the 79 record just acts as marker for the start of
     * the output parameters. The actual return value is in an output param token.</li>
     * </ol>
     * Output parameters are distinguished from procedure return values by the value of
     * a byte that immediately follows the parameter name. A value of 1 seems to indicate
     * a normal output parameter while a value of 2 indicates a procedure return value.
     *
     * @param  cx the StatementImpl instance that owns this request.
     * @throws IOException
     */
    private void tdsOutputParamToken(final StatementImpl cx)
        throws IOException, SQLException 
    {
        in.readShort(); // Packet length
        String name = in.readString(in.read(), connection.getCharset()); // Column Name
        // Next byte indicates if output parameter or return value
        // 1 = normal output param, 2 = function or stored proc return
        in.read();
        // Next byte is the parameter type that we supplied which
        // may not be the same as the parameter definition
        in.read();
        // Not sure what these bytes are (they always seem to be zero).
        in.skip(3);
        ColInfo col = new ColInfo();
        readType(col);
        Object value = readData(col);

        //
        // Real output parameters will either be unnamed or will have a valid
        // parameter name beginning with '@'. Ignore any other spurious parameters
        // such as those returned from calls to writetext in the proc.
        //
        if (name.length() == 0 || name.startsWith("@")) {
            // Set next output parameter in list
            cx.setNextOutputParam(value, col);
        }
    }

    /**
     * Process a login acknowledgement packet.
     *
     * @param  cx the StatementImpl instance that owns this request.
     * @throws IOException
     */
    protected void tdsLoginAckToken(final StatementImpl cx) throws IOException {
        String product;
        int major, minor, build = 0;
        in.readShort(); // Packet length

        int ack = in.read(); // Ack TDS 5 = 5 for OK 6 for fail, 1/0 for the others

        //
        // Discard TDS Version information (must be 5.0)
        //
        in.read();
        in.read();
        in.read();
        in.read();

        product = in.readString(in.read(), connection.getCharset());

        if (product.toLowerCase().contains("anywhere")) {
            // ASA  9 and below : 'Adaptive Server Anywhere',
            // ASA 10 and higher: 'SQL Anywhere'
            this.serverType = ANYWHERE;
            major = in.read();
            minor = in.read();
            in.skip(1);
        } else {
            major = in.read();
            if (major < 10) {
                major += 10; // Correct for Sybase 11.03
            }
            minor = in.read() * 10;
            minor += in.read();
        }
        in.skip(1);

        if (product.length() > 1 && -1 != product.indexOf('\0')) {
            product = product.substring(0, product.indexOf('\0'));
        }

        connection.setDBServerInfo(product, major, minor, build, this.serverType);

        if (ack != 5) {
            // Login rejected by server create SQLException
            cx.getMessages().addDiagnostic(4002, 14, "Login failed");
            this.token = TDS_ERROR_TOKEN;
        } else {
            // MJH 2005-11-02
            // If we get this far we are logged in OK so convert
            // any database security exceptions into warnings. 
            // Any exceptions are likely to be caused by problems in 
            // accessing the default database for this login id for 
            // SQL 6.5 and Sybase ASE. 
            // I am not convinced that this is a good idea but it
            // appears that other drivers e.g. jConnect do this and
            // return the exceptions on the connection warning chain.
            // See bug/RFE [1346086] Problem with DATABASE name change on Sybase
            //
            // Avoid returning useless warnings about language
            // character set etc.
            cx.getMessages().clearWarnings();
            //
            // Get list of exceptions
            SQLException ex = cx.getMessages().getExceptions();
            // Clear old exception list
            cx.getMessages().clearExceptions();
            //
            // Convert default database security exceptions to warnings
            //
            while (ex != null) {
                if (ex.getErrorCode() == ERR_INVALID_USER 
                    || ex.getErrorCode() == ERR_INVALID_USER_2
                    || ex.getErrorCode() == ERR_NO_DEFAULT_DB) {
                    cx.getMessages().addWarning(new SQLWarning(ex.getMessage(), 
                                                 ex.getSQLState(), 
                                                 ex.getErrorCode()));
                } else {
                    cx.getMessages().addException(new SQLException(ex.getMessage(), 
                                                     ex.getSQLState(), 
                                                     ex.getErrorCode()));
                }
                ex = ex.getNextException();
            }
        }
    }

    /**
     * Process a control token (function unknown).
     *
     * @throws IOException
     */
    private void tdsControlToken() throws IOException {
        int pktLen = in.readShort();

        in.skip(pktLen);
    }

    /**
     * Process a row data token.
     *
     * @param  cx the StatementImpl instance that owns this request.
     * @throws IOException
     */
    private void tdsRowToken(final StatementImpl cx) throws IOException {
        ColInfo[] columns = cx.getColumns();
        Object[] row  = cx.getRowBuffer();
        for (int i = 0; i < columns.length; i++) {
            row[i] = readData(columns[i]);
        }
    }

    /**
     * Process TDS 5.0 Params Token.
     * Stored procedure output parameters or data returned in parameter format
     * after a TDS Dynamic packet or as extended error information.
     * <p>The type of the preceding token is inspected to determine if this packet
     * contains output parameter result data. A TDS5_PARAMFMT2_TOKEN is sent before
     * this one in Sybase 12 to introduce output parameter results.
     * A TDS5_PARAMFMT_TOKEN is sent before this one to introduce extended error
     * information.
     *
     * @param  cx the StatementImpl instance that owns this request.
     * @throws IOException
     */
    private void tds5ParamsToken(final StatementImpl cx) throws IOException, SQLException 
    {
        ColInfo[] dynamParamInfo = cx.getDynamParamInfo(); 
        if (dynamParamInfo == null) {
            throw new IOException(
              "TDS 5 Param results token (0xD7) not preceded by param format (0xEC or 0X20).");
        }
        Object[] dynamParamData = cx.getDynamParamData();
        for (int i = 0; i < dynamParamData.length; i++) {
            dynamParamData[i] =
                readData(dynamParamInfo[i]);
            String name = dynamParamInfo[i].realName;
            //
            // Real output parameters will either be unnamed or will have a valid
            // parameter name beginning with '@'. Ignore any other Spurious parameters
            // such as those returned from calls to writetext in the proc.
            //
            if (name.length() == 0 || name.startsWith("@")) {
                // Sybase 12+ this token used to set output parameter results
                cx.setNextOutputParam(dynamParamData[i], null);
            }
        }
    }

    /**
     * Processes a TDS 5.0 capability token.
     * <p>
     * Sent after login to describe the server's capabilities.
     *
     * @throws IOException if an I/O error occurs
     */
    private void tdsCapabilityToken() throws IOException {
        in.readShort(); // Packet length
        if (in.read() != 1) {
            throw new IOException("TDS_CAPABILITY: expected request string");
        }
        int capLen = in.read();
        if (capLen != 11 && capLen != 0) {
            throw new IOException("TDS_CAPABILITY: byte count not 11");
        }
        byte capRequest[] = new byte[11];
        if (capLen == 0) {
            Logger.println("TDS_CAPABILITY: Invalid request length");
        } else {
            in.read(capRequest);
        }
        if (in.read() != 2) {
            throw new IOException("TDS_CAPABILITY: expected response string");
        }
        capLen = in.read();
        if (capLen != 10 && capLen != 0) {
            throw new IOException("TDS_CAPABILITY: byte count not 10");
        }
        byte capResponse[] = new byte[10];
        if (capLen == 0) {
            Logger.println("TDS_CAPABILITY: Invalid response length");
        } else {
            in.read(capResponse);
        }
        //
        // Request capabilities
        //
        // jTDS sends   01 0B 4F FF 85 EE EF 65 7F FF FF FF D6
        // Sybase 11.92 01 0A    00 00 00 23 61 41 CF FF FF C6
        // Sybase 12.52 01 0A    03 84 0A E3 61 41 FF FF FF C6
        // Sybase 15.00 01 0B 4F F7 85 EA EB 61 7F FF FF FF C6
        //
        // Response capabilities
        //
        // jTDS sends   02 0A 00 00 04 06 80 06 48 00 00 00
        // Sybase 11.92 02 0A 00 00 00 00 00 06 00 00 00 00
        // Sybase 12.52 02 0A 00 00 00 00 00 06 00 00 00 00
        // Sybase 15.00 02 0A 00 00 04 00 00 06 00 00 00 00
        //
        // Now set the correct attributes for this connection.
        // See the CT_LIB documentation for details on the bit
        // positions.
        //
        int capMask = 0;
        if ((capRequest[0] & 0x02) == 0x02) {
            capMask |= SYB_UNITEXT;
        }
        if ((capRequest[1] & 0x03) == 0x03) {
            capMask |= SYB_DATETIME;
        }
        if ((capRequest[2] & 0x80) == 0x80) {
            capMask |= SYB_UNICODE;
        }
        if ((capRequest[3] & 0x02) == 0x02) {
            capMask |= SYB_EXTCOLINFO;
        }
        if ((capRequest[2] & 0x01) == 0x01) {
            capMask |= SYB_BIGINT;
        }
        if ((capRequest[4] & 0x04) == 0x04) {
            capMask |= SYB_BITNULL;
        }
        if ((capRequest[7] & 0x30) == 0x30) {
            capMask |= SYB_LONGDATA;
        }
        this.sybaseInfo = capMask;
    }

    /**
     * Process a TDS 5 error or informational message.
     *
     * @param  cx the StatementImpl instance that owns this request.
     * @throws IOException
     */
    private void tds5ErrorToken(final StatementImpl cx) throws IOException 
    {
        int pktLen = in.readShort(); // Packet length
        int sizeSoFar = 6;
        int number = in.readInt();
        in.read(); // state
        int severity = in.read();
        // Discard text state
        int stateLen = in.read();
        in.readString(stateLen, connection.getCharset());
        in.read(); // == 1 if extended error data follows
        // Discard status and transaction state
        in.readShort();
        sizeSoFar += 4 + stateLen;

        int msgLen = in.readShort();
        String message = in.readString(msgLen, connection.getCharset());
        sizeSoFar += 2 + msgLen;
        final int srvNameLen = in.read();
        in.readString(srvNameLen, connection.getCharset()); // server name
        sizeSoFar += 1 + srvNameLen;

        final int procNameLen = in.read();
        in.readString(procNameLen, connection.getCharset()); // proc name
        sizeSoFar += 1 + procNameLen;

        in.readShort(); // line
        sizeSoFar += 2;
        // Skip any EED information to read rest of packet
        if (pktLen - sizeSoFar > 0)
            in.skip(pktLen - sizeSoFar);

        if (severity > 10)
        {
            cx.getMessages().addDiagnostic(number, severity, message);
        } else {
            cx.getMessages().addDiagnostic(number, severity, message);
        }
    }

    /**
     * Process an environment change packet.
     *
     * @throws IOException
     * @throws SQLException
     */
    private void tdsEnvChangeToken()
        throws IOException, SQLException
    {
        int len = in.readShort();
        int type = in.read();

        switch (type) {
            case TDS_ENV_DATABASE:
                {
                    int clen = in.read();
                    final String newDb = in.readString(clen, connection.getCharset());
                    clen = in.read();
                    final String oldDb = in.readString(clen, connection.getCharset());
                    connection.setDatabase(newDb, oldDb);
                    break;
                }

            case TDS_ENV_LANG:
                {
                    int clen = in.read();
                    String language = in.readString(clen, connection.getCharset());
                    clen = in.read();
                    String oldLang = in.readString(clen, connection.getCharset());
                    if (Logger.isActive()) {
                        Logger.println("Language changed from " + oldLang + " to " + language);
                    }
                    break;
                }

            case TDS_ENV_CHARSET:
                {
                    final int clen = in.read();
                    final String charset = in.readString(clen, connection.getCharset());
                    in.skip(len - 2 - clen);
                    connection.setServerCharset(charset);
                    break;
                }

            case TDS_ENV_PACKSIZE:
                    {
                        final int blocksize;
                        final int clen = in.read();
                        blocksize = Integer.parseInt(in.readString(clen, connection.getCharset()));
                        in.skip(len - 2 - clen);
                        out.setBufferSize(blocksize);
                        if (Logger.isActive()) {
                            Logger.println("Changed blocksize to " + blocksize);
                        }
                    }
                    break;

            default:
                {
                    if (Logger.isActive()) {
                        Logger.println("Unknown environment change type 0x" +
                                            Integer.toHexString(type));
                    }
                    in.skip(len - 1);
                    break;
                }
        }
    }

    /**
     * Process TDS5 dynamic SQL aknowledgements.
     *
     * @throws IOException
     */
    private void tds5DynamicToken()
            throws IOException
    {
        int pktLen = in.readShort();
        byte type = (byte)in.read();
        /*byte status = (byte)*/in.read();
        pktLen -= 2;
        if (type == (byte)0x20) {
            // Only handle aknowledgements for now
            int len = in.read();
            in.skip(len);
            pktLen -= len+1;
        }
        in.skip(pktLen);
    }

    /**
     * Process TDS 5 Dynamic results parameter descriptors.
     * <p>
     * With Sybase 12+ this has been superseded by the TDS5_PARAMFMT2_TOKEN
     * except when used to return extended error information.
     *
     * @param  cx the StatementImpl instance that owns this request.
     * @throws IOException
     */
    private void tds5ParamFmtToken(final StatementImpl cx) throws IOException 
    {
        in.readShort(); // Packet length
        int paramCnt = in.readShort();
        ColInfo[] params = new ColInfo[paramCnt];
        for (int i = 0; i < paramCnt; i++) {
            //
            // Get the parameter details using the
            // ColInfo class as the server format is the same.
            //
            ColInfo col = new ColInfo();
            int colNameLen = in.read();
            col.realName = in.readString(colNameLen, connection.getCharset());
            int column_flags = in.read();   /*  Flags */
            col.isCaseSensitive = false;
            col.nullable    = ((column_flags & 0x20) != 0)?
                                        ResultSetMetaData.columnNullable:
                                        ResultSetMetaData.columnNoNulls;
            col.isWriteable = (column_flags & 0x10) != 0;
            col.isIdentity  = (column_flags & 0x40) != 0;
            col.isKey       = (column_flags & 0x02) != 0;
            col.isHidden    = (column_flags & 0x01) != 0;

            col.userType    = in.readInt();
            if ((byte)in.peek() == TDS_DONE_TOKEN) {
                // Sybase 11.92 bug data type missing!
                cx.setDynamParamInfo(null);
                cx.setDynamParamData(null);
                // error trapped in sybasePrepare();
                cx.getMessages().addDiagnostic(9999, 16, "Prepare failed");

                return; // Give up
            }
            readType(col);
            // Skip locale information
            in.skip(1);
            params[i] = col;
        }
        cx.setDynamParamInfo(params);
        cx.setDynamParamData(new Object[paramCnt]);
    }

    /**
     * Process a TDS 5.0 result set packet.
     *
     * @param  cx the StatementImpl instance that owns this request.
     * @throws IOException
     */
    private void tds5ResultToken(final StatementImpl cx) throws IOException 
    {
        in.readShort(); // Packet length
        int colCnt = in.readShort();
        ColInfo[] columns = new ColInfo[colCnt];
        this.tables = null;

        for (int colNum = 0; colNum < colCnt; ++colNum) {
            //
            // Get the column name
            //
            ColInfo col = new ColInfo();
            int colNameLen = in.read();
            col.realName  = in.readString(colNameLen, connection.getCharset());
            col.name = col.realName;
            int column_flags = in.read();   /*  Flags */
            col.isCaseSensitive = false;
            col.nullable    = ((column_flags & 0x20) != 0)?
                                   ResultSetMetaData.columnNullable:
                                        ResultSetMetaData.columnNoNulls;
            col.isWriteable = (column_flags & 0x10) != 0;
            col.isIdentity  = (column_flags & 0x40) != 0;
            col.isKey       = (column_flags & 0x02) != 0;
            col.isHidden    = (column_flags & 0x01) != 0;

            col.userType    = in.readInt();
            readType(col);
            // Skip locale information
            in.skip(1);
            columns[colNum] = col;
        }
        cx.setColumns(columns);
    }

    /**
     * Process a DONE, DONEINPROC or DONEPROC token.
     *
     * @param  cx the StatementImpl instance that owns this request.
     * @throws IOException
     */
    private void tdsDoneToken(final StatementImpl cx) throws IOException {
        this.doneStatus = (byte)in.read();
        in.skip(1);
        in.skip(1); // Operation field
        in.skip(1);
        this.doneCount = in.readInt();

        //
        // Check for cancel ack
        //
        if ((this.doneStatus & DONE_CANCEL) != 0) {
            // Synchronize resetting of the cancelPending flag to ensure it
            // doesn't happen during the sending of a cancel request
            synchronized (cancelMonitor) {
                cancelPending = false;
                // Only throw an exception if this was a cancel() call
                if (cancelMonitor[0] == ASYNC_CANCEL) {
                    cx.getMessages().addException(
                        new SQLException(Messages.get("error.generic.cancelled",
                                                      "Statement"),
                                         "HY008"));
                }
            }
        }

        if ((this.doneStatus & DONE_MORE_RESULTS) == 0) {
            //
            // There are no more results or pending cancel packets
            // to process.
            //
            endOfResponse = !cancelPending;
            //
            if (fatalError) {
                // A fatal error has occured, the server has closed the
                // connection
                connection.setClosed();
            }
        }
    }

    /**
     * Execute SQL using TDS 5.0 protocol.
     *
     * @param cx the StatementImpl instance that owns this request.
     * @param sql The SQL statement to execute.
     * @param procName Stored procedure to execute or <code>null</code>.
     * @param parameters Parameters for call or <code>null</code>.
     * @param noMetaData Suppress meta data for cursor calls.
     * @param sendNow true to actually send the packet.
     * @throws SQLException
     */
    protected void localExecuteSQL(final StatementImpl cx,
                                   String sql,
                                   String procName,
                                   ParamInfo[] parameters,
                                   final boolean dummy1,
                                   final boolean dummy2)
        throws IOException, SQLException 
    {
        boolean haveParams    = parameters != null;
        boolean useParamNames = false;
        //
        // Sybase does not allow text or image parameters as parameters
        // to statements or stored procedures. With Sybase 12.5 it is
        // possible to use a new TDS data type to send long data as
        // parameters to statements (but not procedures). This usage
        // replaces the writetext command that had to be used in the past.
        // As we do not support writetext, with older versions of Sybase
        // we just give up and embed all text/image data in the SQL statement.
        //
        for (int i = 0; haveParams && i < parameters.length; i++) {
            if (parameters[i].sqlType.equals("text")
                || parameters[i].sqlType.equals("image")
                || parameters[i].sqlType.equals("unitext")) {
                if (procName != null && procName.length() > 0) {
                    // Call to store proc nothing we can do
                    if (parameters[i].sqlType.equals("text")
                        || parameters[i].sqlType.equals("unitext")) {
                        throw new SQLException(
                                        Messages.get("error.chartoolong"), "HY000");
                    }

                    throw new SQLException(
                                     Messages.get("error.bintoolong"), "HY000");
                }
                if (parameters[i].tdsType != SYBLONGDATA) {
                    // prepared statement substitute parameters into SQL
                    sql = Support.substituteParameters(sql, parameters, connection);
                    haveParams = false;
                    procName = null;
                    break;
                }
            }
        }

        out.setPacketType(SYBQUERY_PKT);

        if (procName == null) {
            // Use TDS_LANGUAGE TOKEN with optional parameters
            out.write((byte)TDS_LANG_TOKEN);

            if (haveParams) {
                sql = Support.substituteParamMarkers(sql, parameters);
            }
            Logger.printSql(sql);
            if (connection.isWideChar()) {
                // Need to preconvert string to get correct length
                ByteBuffer bb = connection.getCharset().encode(sql);
                out.write((int) bb.remaining() + 1);
                out.write((byte)(haveParams ? 1 : 0));
                out.write(bb);
            } else {
                out.write((int) sql.length() + 1);
                out.write((byte) (haveParams ? 1 : 0));
                out.write(sql, connection.getCharset());
            }
        } else if (procName.startsWith("#jtds")) {
            // Dynamic light weight procedure call
            Logger.printRPC(procName);
            out.write((byte) TDS5_DYNAMIC_TOKEN);
            out.write((short) (procName.length() + 4));
            out.write((byte) 2);
            out.write((byte) (haveParams ? 1 : 0));
            out.write((byte) (procName.length() - 1));
            out.write(procName.substring(1), connection.getCharset());
            out.write((short) 0);
        } else {
            ByteBuffer bb = connection.getCharset().encode(procName);
            // RPC call
            Logger.printRPC(procName);
            out.write((byte) TDS_DBRPC_TOKEN);
            out.write((short) (bb.remaining() + 3));
            out.write((byte) bb.remaining());
            out.write(bb);
            out.write((short) (haveParams ? 2 : 0));
            useParamNames = true;
        }

        //
        // Output any parameters
        //
        if (haveParams) {
            //
            int startIndex = cx.getParamIndex();
            // First write parameter descriptors
            out.write((byte) TDS5_PARAMFMT_TOKEN);

            int len = 2;

            for (int i = startIndex + 1; i < parameters.length; i++) {
                len += getTds5ParamSize(parameters[i], useParamNames);
            }

            out.write((short) len);
            out.write((short) ((startIndex < 0) ? parameters.length : parameters.length - 1));

            for (int i = startIndex + 1; i < parameters.length; i++) {
                writeTds5ParamFmt(parameters[i], useParamNames);
            }

            // Now write the actual data
            out.write((byte) TDS5_PARAMS_TOKEN);

            for (int i = startIndex + 1; i < parameters.length; i++) {
                writeTds5Param(parameters[i]);
            }
        }
    }
     
    /**
     * Read the TDS datastream and populate the ColInfo parameter with
     * data type and related information.
     * <p>The type infomation conforms to one of the following formats:
     * <ol>
     * <li> [int1 type]  - eg SYBINT4.
     * <li> [int1 type] [int1 buffersize]  - eg VARCHAR &lt; 256
     * <li> [int1 type] [int2 buffersize]  - eg VARCHAR &gt; 255.
     * <li> [int1 type] [int4 buffersize] [int1 tabnamelen] [int1*n tabname] - eg text.
     * <li> [int1 type] [int4 buffersize] - eg sql_variant.
     * <li> [int1 type] [int1 buffersize] [int1 precision] [int1 scale] - eg decimal.
     * </ol>
     * For TDS 8 large character types include a 5 byte collation field after the buffer size.
     *
     * @param ci The ColInfo column descriptor object.
     * @return The number of bytes read from the input stream.
     * @throws IOException
     */
    private int readType(final ColInfo ci) throws IOException {
        int bytesRead = 1;
        // Get the TDS data type code
        int type = in.read();

        if (types[type] == null || type == SYBLONGDATA) {
            // Trap invalid type or 0x24 received from a Sybase server!
            throw new IOException("Invalid TDS data type " + (type & 0xFF));
        }

        ci.tdsType     = type;
        ci.jdbcType    = types[type].jdbcType;
        ci.scale       = types[type].scale;
        ci.sqlType     = types[type].sqlType;
        ci.charset     = this.connection.getCharset();
        
        // Now get the buffersize if required
        switch (types[type].size) {
            case -5:
                // Sybase long binary
                ci.bufferSize = in.readInt();
                bytesRead += 4;
                break;

            case -4:
                // text or image
                ci.bufferSize = in.readInt();
                int lenName = in.readShort();

                ci.tableName = in.readString(lenName, connection.getCharset());
                bytesRead += 6 + lenName;
                break;

            case -3:
                break;

            case -2:
                // longvarchar longvarbinary
                if (ci.tdsType == XSYBCHAR) {
                    ci.bufferSize = in.readInt();
                    ci.displaySize = ci.bufferSize;
                    ci.precision = ci.displaySize;
                    bytesRead += 4;
                } else {
                    ci.bufferSize = in.readShort();
                    bytesRead += 2;
                }
                break;

            case -1:
                // varchar varbinary decimal etc
                bytesRead += 1;
                ci.bufferSize = in.read();
                break;

            default:
                ci.bufferSize = types[type].size;
                break;
        }

        // Now fine tune sizes for specific types
        switch (type) {
            // Establish actual size of nullable datetime
            case SYBDATETIMN:
                type = (ci.bufferSize == 4)? SYBDATETIME4: SYBDATETIME;
                ci.jdbcType    = types[type].jdbcType;
                ci.scale       = types[type].scale;
                ci.sqlType     = types[type].sqlType;
                break;
            // Establish actual size of nullable float
            case SYBFLTN:
                type = (ci.bufferSize == 4)? SYBREAL: SYBFLT8;
                ci.jdbcType    = types[type].jdbcType;
                ci.sqlType     = types[type].sqlType;
                break;
            // Establish actual size of nullable int
            case SYBINTN:
                switch (ci.bufferSize) {
                    case 8:
                        type = SYBINT8;
                        break;
                    case 4:
                        type = SYBINT4;
                        break;
                    case 2:
                        type = SYBINT2;
                        break;
                    default:
                        type = SYBINT1;
                    break;
                }
                ci.jdbcType    = types[type].jdbcType;
                ci.sqlType     = types[type].sqlType;
                break;
                // Establish actual size of nullable unsigned int
            case SYBUINTN:
                switch (ci.bufferSize) {
                    case 8:
                        type = SYBUINT8;
                        break;
                    case 4:
                        type = SYBUINT4;
                        break;
                    case 2:
                        type = SYBUINT2;
                        break;
                    default:
                        throw new IOException("unsigned tinyint null not supported");
                }
                ci.jdbcType    = types[type].jdbcType;
                ci.sqlType     = types[type].sqlType;
                break;
            // Establish actual size of nullable money
            case SYBMONEYN:
                type = (ci.bufferSize == 8)? SYBMONEY: SYBMONEY4;
                ci.jdbcType    = types[type].jdbcType;
                ci.sqlType     = types[type].sqlType;
                break;

            // Read in scale and precision for decimal types
            case SYBDECIMAL:
            case SYBNUMERIC:
                ci.precision   = in.read();
                ci.scale       = in.read();
                ci.displaySize = ((ci.scale > 0) ? 2 : 1) + ci.precision;
                bytesRead     += 2;
                break;

            // Although a binary type force displaysize to MAXINT
            case SYBIMAGE:
                ci.precision   = Integer.MAX_VALUE;
                ci.displaySize = Integer.MAX_VALUE;
                break;
                
            case SYBTEXT:
                ci.precision   = Integer.MAX_VALUE;
                ci.displaySize = Integer.MAX_VALUE;
                break;
                                
            case SYBNTEXT:
            case SYBUNITEXT:
                ci.precision   = Integer.MAX_VALUE / 2;
                ci.displaySize = Integer.MAX_VALUE / 2;
                break;

        }

        // Set default displaySize and precision
        switch (types[ci.tdsType].precision) {
            case 0:
                // Already set
                break;
            case -1:
                ci.precision = ci.bufferSize;
                ci.displaySize = ci.precision;
                break;
            case -2:
                ci.precision = ci.bufferSize / 2;
                ci.displaySize = ci.precision;
                break;
            case -3:
                ci.precision = ci.bufferSize;
                ci.displaySize = ci.precision * 2;
                break;
            default: 
                ci.precision   = types[type].precision;
                ci.displaySize = types[type].displaySize;
                break;
        }

        // For numeric types add 'identity' for auto inc data type
        if (ci.isIdentity) {
            ci.sqlType += " identity";
        }

        // Fine tune Sybase data types
        switch (ci.userType) {
                case  UDT_CHAR:
                    ci.sqlType      = "char";
                    ci.jdbcType    = java.sql.Types.CHAR;
                    break;
                case  UDT_VARCHAR:
                    ci.sqlType     = "varchar";
                    ci.jdbcType    = java.sql.Types.VARCHAR;
                    break;
                case  UDT_BINARY:
                    ci.sqlType     = "binary";
                    ci.displaySize = ci.bufferSize * 2;
                    ci.jdbcType    = java.sql.Types.BINARY;
                    break;
                case  UDT_VARBINARY:
                    ci.sqlType     = "varbinary";
                    ci.displaySize = ci.bufferSize * 2;
                    ci.jdbcType    = java.sql.Types.VARBINARY;
                    break;
                case UDT_SYSNAME:
                    ci.sqlType     = "sysname";
                    ci.jdbcType    = java.sql.Types.VARCHAR;
                    break;
                case UDT_TIMESTAMP:
                    ci.sqlType     = "timestamp";
                    ci.displaySize = ci.bufferSize * 2;
                    ci.jdbcType    = java.sql.Types.VARBINARY;
                    break;
                case UDT_NCHAR:
                    ci.sqlType     = "nchar";
                    ci.jdbcType    = java.sql.Types.CHAR;
                    break;
                case UDT_NVARCHAR:
                    ci.sqlType     = "nvarchar";
                    ci.jdbcType    = java.sql.Types.VARCHAR;
                    break;
                case UDT_UNICHAR:
                    ci.sqlType     = "unichar";
                    ci.displaySize = ci.bufferSize / 2;
                    ci.precision   = ci.displaySize;
                    ci.jdbcType    = java.sql.Types.CHAR;
                    break;
                case UDT_UNIVARCHAR:
                    ci.sqlType     = "univarchar";
                    ci.displaySize = ci.bufferSize / 2;
                    ci.precision   = ci.displaySize;
                    ci.jdbcType    = java.sql.Types.VARCHAR;
                    break;            
                case UDT_LONGSYSNAME:
                    ci.sqlType     = "longsysname";
                    ci.jdbcType    = java.sql.Types.VARCHAR;
                    break;
        }
        
        return bytesRead;
    }

    /**
     * Read the TDS data item from the Response Stream.
     * <p> The data size is either implicit in the type for example
     * fixed size integers, or a count field precedes the actual data.
     * The size of the count field varies with the data type.
     *
     * @param ci The ColInfo column descriptor object.
     * @return The data item Object or null.
     * @throws IOException
     */
    private Object readData(final ColInfo ci) throws IOException {
        int len;

        switch (ci.tdsType) {
            case SYBINTN:
                switch (in.read()) {
                    case 1:
                        return new Integer(in.read() & 0xFF);
                    case 2:
                        return new Integer(in.readShort());
                    case 4:
                        return new Integer(in.readInt());
                    case 8:
                        return new Long(in.readLong());
                }

                break;

            // Sybase ASE 15+ supports unsigned null smallint, int and bigint
            case SYBUINTN:
                switch (in.read()) {
                    case 1:
                        return new Integer(in.read() & 0xFF);
                    case 2:
                        return new Integer(in.readShort() & 0xFFFF);
                    case 4:
                        return new Long(in.readInt() & 0xFFFFFFFFL );
                    case 8:
                        return in.readUnsignedLong();
                }
                break;

            case SYBINT1:
                return new Integer(in.read() & 0xFF);

            case SYBINT2:
                return new Integer(in.readShort());

            case SYBINT4:
                return new Integer(in.readInt());

            // SQL Server bigint
            case SYBINT8:
                return new Long(in.readLong());

            // Sybase ASE 15+ bigint
            case SYBSINT8:
                return new Long(in.readLong());

            // Sybase ASE 15+ unsigned smallint
            case SYBUINT2:
                return new Integer(in.readShort() & 0xFFFF);

            // Sybase ASE 15+ unsigned int
            case SYBUINT4:
                return new Long(in.readInt() & 0xFFFFFFFFL);

            // Sybase ASE 15+ unsigned bigint
            case SYBUINT8:
                return in.readUnsignedLong();

            case SYBIMAGE:
                len = in.read();

                if (len > 0) {
                    in.skip(24); // Skip textptr and timestamp
                    int dataLen = in.readInt();
                    return new BlobImpl(this.connection, in.getInputStream(dataLen), dataLen);
                }

                break;

            case SYBTEXT:
                len = in.read();

                if (len > 0) {
                    in.skip(24); // Skip textptr and timestamp
                    int dataLen = in.readInt();
                    return new ClobImpl(this.connection, 
                            new InputStreamReader(in.getInputStream(dataLen), ci.charset), dataLen);
                }

                break;

            case SYBUNITEXT: // ASE 15+ unicode text type
            case SYBNTEXT:
                len = in.read();

                if (len > 0) {
                    in.skip(24); // Skip textptr and timestamp
                    int dataLen = in.readInt();
                    return new ClobImpl(this.connection, in.getInputStream(dataLen), dataLen);
                }

                break;

            case SYBCHAR:
            case SYBVARCHAR:
                len = in.read();

                if (len > 0) {
                    String value = in.readString(len, ci.charset);

                    if (len == 1 && ci.tdsType == SYBVARCHAR) {
                        // In TDS 4/5 zero length varchars are stored as a
                        // single space to distinguish them from nulls.
                        return (value.equals(" ")) ? "" : value;
                    }

                    return value;
                }

                break;

            case SYBNVARCHAR:
                len = in.read();

                if (len > 0) {
                    return in.readString(len, connection.getCharset());
                }

                break;

            case XSYBCHAR:
                // This is a Sybase wide table String
                len = in.readInt();
                if (len > 0) {
                    String tmp = in.readString(len, ci.charset);
                    if (tmp.equals(" ") && !ci.sqlType.equals("char")) {
                        tmp = "";
                    }
                    return tmp;
                }
                break;

            case SYBVARBINARY:
            case SYBBINARY:
                len = in.read();

                if (len > 0) {
                    byte[] bytes = new byte[len];

                    in.read(bytes);

                    return bytes;
                }

                break;
                                
            case XSYBVARBINARY:
            case XSYBBINARY:
                len = in.readShort();

                if (len != -1) {
                    byte[] bytes = new byte[len];

                    in.read(bytes);

                    return bytes;
                }
                break;

            case SYBLONGBINARY:
                len = in.readInt();
                if (len != 0) {
                    if (ci.sqlType.equals("unichar") ||
                        ci.sqlType.equals("univarchar")) {
                        String s = in.readUnicode(len / 2);
                        if ((len & 1) != 0) {
                            // Bad length should be divisible by 2
                            in.skip(1); // Deal with it anyway.
                        }
                        if (s.equals(" ")) {
                            return "";
                        }
                        return s;
                    }
                    byte[] bytes = new byte[len];
                    in.read(bytes);
                    return bytes;
                }
                break;

            case SYBMONEY4:
                return in.readMoney(4);
            case SYBMONEY:
                return in.readMoney(8);
            case SYBMONEYN:
                return in.readMoney(in.read());

            case SYBDATETIME4:
                return in.readDatetime(4);
            case SYBDATETIMN:
                return in.readDatetime(in.read());
            case SYBDATETIME:
                return in.readDatetime(8);

            case SYBDATEN:
            case SYBDATE:
                len = (ci.tdsType == SYBDATEN)? in.read(): 4;
                if (len == 4) {
                    return new DateTime(in.readInt(), DateTime.TIME_NOT_USED);
                }
                // Invalid length or 0 for null
                in.skip(len);
                break;

            case SYBTIMEN:
            case SYBTIME:
                len = (ci.tdsType == SYBTIMEN)? in.read(): 4;
                if (len == 4) {
                    return new DateTime(DateTime.DATE_NOT_USED, in.readInt());
                }
                // Invalid length or 0 for null
                in.skip(len);
                break;

            case SYBBIT:
                return (in.read() != 0) ? Boolean.TRUE : Boolean.FALSE;

            case SYBBITN:
                len = in.read();

                if (len > 0) {
                    return (in.read() != 0) ? Boolean.TRUE : Boolean.FALSE;
                }

                break;

            case SYBREAL:
                return new Float(Float.intBitsToFloat(in.readInt()));

            case SYBFLT8:
                return new Double(Double.longBitsToDouble(in.readLong()));

            case SYBFLTN:
                len = in.read();

                if (len == 4) {
                    return new Float(Float.intBitsToFloat(in.readInt()));
                } else if (len == 8) {
                    return new Double(Double.longBitsToDouble(in.readLong()));
                }

                break;

            case SYBUNIQUE:
                len = in.read();

                if (len > 0) {
                    byte[] bytes = new byte[len];

                    in.read(bytes);

                    return new UniqueIdentifier(bytes);
                }

                break;

            case SYBNUMERIC:
            case SYBDECIMAL:
                len = in.read();

                if (len > 0) {
                    int sign = in.read();

                    len--;
                    byte[] bytes = new byte[len];
                    BigInteger bi;

                    // Sybase order is MSB first!
                    for (int i = 0; i < len; i++) {
                        bytes[i] = (byte) in.read();
                    }

                    bi = new BigInteger((sign == 0) ? 1 : -1, bytes);

                    return new BigDecimal(bi, ci.scale);
                }

                break;

            default:
                throw new IllegalStateException("Unsupported TDS data type 0x" + 
                                        Integer.toHexString(ci.tdsType & 0xFF));
        }

        return null;
    }

    /**
     * Set the TDS native type code for the parameter.
     *
     * @param pi         the parameter descriptor
     */
    void setNativeType(final ParamInfo pi) throws SQLException {
        
        try {
            int jdbcType = pi.jdbcType;

            if (jdbcType == java.sql.Types.OTHER) {
                jdbcType = Support.getJdbcType(pi.value);
            }

            switch (jdbcType) {
                case java.sql.Types.CHAR:
                case java.sql.Types.CLOB:
                case java.sql.Types.VARCHAR:
                case java.sql.Types.LONGVARCHAR:
                    getTds50Varchar(pi);
                    if (pi.isOutput 
                        && (pi.sqlType.equals("text") 
                        || pi.sqlType.equals("unitext") 
                        || pi.sqlType.equals("ntext"))) {
                        throw new SQLException(
                                    Messages.get("error.textoutparam"), "HY000");
                    }
                    break;

                case java.sql.Types.TINYINT:
                case java.sql.Types.SMALLINT:
                case java.sql.Types.INTEGER:
                    pi.tdsType = SYBINTN;
                    pi.sqlType = "int";
                    break;

                case java.sql.Types.BOOLEAN:
                case java.sql.Types.BIT:
                    pi.tdsType = SYBBIT;
                    pi.sqlType = "bit";
                    break;

                case java.sql.Types.REAL:
                    pi.tdsType = SYBFLTN;
                    pi.sqlType = "real";
                    break;

                case java.sql.Types.FLOAT:
                case java.sql.Types.DOUBLE:
                    pi.tdsType = SYBFLTN;
                    pi.sqlType = "float";
                    break;

                case java.sql.Types.DATE:
                    if ((this.sybaseInfo & SYB_DATETIME) != 0) {
                        pi.tdsType = SYBDATEN;
                        pi.sqlType = "date";
                    } else {
                        pi.tdsType = SYBDATETIMN;
                        pi.sqlType = "datetime";
                    }
                    break;

                case java.sql.Types.TIME:
                    if ((this.sybaseInfo & SYB_DATETIME) != 0) {
                        pi.tdsType = SYBTIMEN;
                        pi.sqlType = "time";
                    } else {
                        pi.tdsType = SYBDATETIMN;
                        pi.sqlType = "datetime";
                    }
                    break;
                    
                case java.sql.Types.TIMESTAMP:
                    pi.tdsType = SYBDATETIMN;
                    pi.sqlType = "datetime";
                    break;

                case java.sql.Types.BLOB:
                case java.sql.Types.BINARY:
                case java.sql.Types.VARBINARY:
                case java.sql.Types.LONGVARBINARY:
                    getTds50Varbinary(pi);
                    if (pi.isOutput && (pi.sqlType.equals("image"))) {
                        throw new SQLException(
                                    Messages.get("error.textoutparam"), "HY000");
                    }
                    break;

                case java.sql.Types.BIGINT:
                    if ((this.sybaseInfo & SYB_BIGINT) != 0) {
                        pi.tdsType = SYBINTN;
                        pi.sqlType = "bigint";
                    } else {
                        // int8 not supported send as a decimal field
                        pi.tdsType  = SYBDECIMAL;
                        pi.sqlType = "decimal(" + this.connection.getMaxPrecision() + ')';
                        pi.scale = 0;
                    }

                    break;

                case java.sql.Types.DECIMAL:
                case java.sql.Types.NUMERIC:
                    pi.tdsType  = SYBDECIMAL;
                    int prec = this.connection.getMaxPrecision();
                    int scale = ConnectionImpl.DEFAULT_SCALE;
                    if (pi.value instanceof BigDecimal) {
                        scale = ((BigDecimal)pi.value).scale();
                    } else if (pi.scale >= 0 && pi.scale <= prec) {
                        scale = pi.scale;
                    }
                    pi.sqlType = "decimal(" + prec + ',' + scale + ')';

                    break;

                case java.sql.Types.OTHER:
                case java.sql.Types.NULL:
                    // Send a null String in the absence of anything better
                    pi.tdsType = SYBVARCHAR;
                    pi.sqlType = "varchar(255)";
                    break;

                default:
                    throw new SQLException(Messages.get(
                            "error.baddatatype",
                            Integer.toString(pi.jdbcType)), "HY000");
            }
        } catch (IOException e) {
            throw new SQLException(
                            Messages.get("error.generic.ioerror", e.getMessage()), "HY000");
        }
    }
    
    /**
     * Get the TDS 5.0 specific varchar native datatype.
     * 
     * @param pi         the parameter descriptor
     * @throws SQLException
     * @throws IOException
     */
    private void getTds50Varchar(final ParamInfo pi) 
        throws SQLException, IOException 
    {
        int len;
        if (pi.value == null) {
            len = 0;
        } else {
            len = pi.length;
        }                   

        if (len > 0
            && (len <= SYB_LONGVAR_MAX / 2 || (this.sybaseInfo & SYB_UNITEXT) != 0)
            && (this.sybaseInfo & SYB_UNICODE) != 0
            && this.connection.getSendUnicode()
            && !this.connection.getCharset().name().equals("UTF-8")) {
            // Sybase can send values as unicode if conversion to the
            // server charset fails.
            // One option to determine if conversion will fail is to use
            // the CharSetEncoder class but this is only available from
            // JDK 1.4.
            // This behaviour can be disabled by setting the connection
            // property sendParametersAsUnicode=false.
            // TODO: Find a better way of testing for convertable charset.
            // With ASE 15 this code will read a CLOB into memory just to
            // check for unicode characters. This is wasteful if no unicode
            // data is present and we are targetting a text column. The option
            // of always sending unicode does not work as the server will
            // complain about image to text conversions unless the target
            // column actually is unitext.
            //
            if (!pi.isConvertable(this.connection)) {
                // Conversion fails need to send as unicode.
                if (pi.length > SYB_LONGVAR_MAX / 2) {
                    pi.sqlType = "unitext";
                    pi.tdsType = SYBLONGDATA;
                } else {
                    pi.sqlType = "univarchar("+pi.length+')';
                    pi.tdsType = SYBLONGBINARY;
                }
                return;
            }
        }
        len = pi.getAnsiLength(this.connection);

        if (len <= VAR_MAX) {
            pi.tdsType = SYBVARCHAR;
            pi.sqlType = "varchar(255)";
        } else {
            if ((this.sybaseInfo & SYB_LONGDATA) != 0) {
                if (len > SYB_LONGVAR_MAX) {
                    // Use special Sybase long data type which
                    // allows text data to be sent as a statement parameter
                    // (although not as a SP parameter).
                    pi.tdsType = SYBLONGDATA;
                    pi.sqlType = "text";
                } else {
                    // Use Sybase 12.5+ long varchar type which
                    // is limited to 16384 bytes.
                    pi.tdsType = XSYBCHAR;
                    pi.sqlType = "varchar(" + len + ')';
                }
            } else {
                pi.tdsType = SYBTEXT;
                pi.sqlType = "text";
            }
        }
    }
    
    /**
     * Get the TDS 5.0 specific varbinary native datatype.
     * 
     * @param pi the parameter descriptor
     * @throws SQLException
     * @throws IOException
     */
    private void getTds50Varbinary(final ParamInfo pi) 
    throws SQLException, IOException {
        int len = pi.length;
        if (len <= VAR_MAX) {
            pi.tdsType = SYBVARBINARY;
            pi.sqlType = "varbinary(255)";
        } else {
            if ((this.sybaseInfo & SYB_LONGDATA) != 0) {
                if (len > SYB_LONGVAR_MAX) {
                    // Need to use special Sybase long binary type
                    pi.tdsType = SYBLONGDATA;
                    pi.sqlType = "image";
                } else {
                    // Sybase long binary that can be used as a SP parameter
                    pi.tdsType = SYBLONGBINARY;
                    pi.sqlType = "varbinary(" + len + ')';
                }
            } else {
                // Sybase < 12.5
                pi.tdsType = SYBIMAGE;
                pi.sqlType = "image";
            }
        }
    }

    /**
     * Calculate the size of the parameter descriptor array for TDS 5 packets.
     *
     * @param pi The parameter to describe.
     * @param useParamNames True if named parameters should be used.
     * @return The size of the parameter descriptor as an <code>int</code>.
     */
    private int getTds5ParamSize(final ParamInfo pi, boolean useParamNames) {
        int size = 8;
        if (pi.name != null && useParamNames) {
            // Size of parameter name
            if (connection.isWideChar()) {
                size += connection.getCharset().encode(pi.name).remaining();
            } else {
                size += pi.name.length();
            }
        }

        switch (pi.tdsType) {
            case SYBVARCHAR:
            case SYBVARBINARY:
            case SYBINTN:
            case SYBFLTN:
            case SYBDATETIMN:
            case SYBDATEN:
            case SYBTIMEN:
                size += 1;
                break;
            case SYBDECIMAL:
            case SYBLONGDATA:
                size += 3;
                break;
            case XSYBCHAR:
            case SYBLONGBINARY:
                size += 4;
                break;
            case SYBBIT:
                break;
            default:
                throw new IllegalStateException("Unsupported output TDS type 0x"
                        + Integer.toHexString(pi.tdsType));
        }

        return size;
    }

    /**
     * Write a TDS 5 parameter format descriptor.
     *
     * @param pi The parameter to describe.
     * @param useParamNames True if named parameters should be used.
     * @throws IOException
     */
    private void writeTds5ParamFmt(final ParamInfo pi, boolean useParamNames)
    throws IOException {
        if (pi.name != null && useParamNames) {
            // Output parameter name.
            if (connection.isWideChar()) {
                ByteBuffer bb = connection.getCharset().encode(pi.name);
                out.write((byte) bb.remaining());
                out.write(bb);
            } else {
                out.write((byte) pi.name.length());
                out.write(pi.name, connection.getCharset());
            }
        } else {
            out.write((byte)0);
        }

        out.write((byte) (pi.isOutput ? 1 : 0)); // Output param
        if (pi.sqlType.startsWith("univarchar")) {
            out.write((int) UDT_UNIVARCHAR);
        } else if (pi.sqlType.equals("unitext")) {
            out.write((int) UDT_UNITEXT);
        } else {
            out.write((int) 0); // user type
        }
        out.write((byte) pi.tdsType); // TDS data type token

        // Output length fields
        switch (pi.tdsType) {
            case SYBVARCHAR:
            case SYBVARBINARY:
                out.write((byte) VAR_MAX);
                break;
            case XSYBCHAR:
                out.write((int)0x7FFFFFFF);
                break;
            case SYBLONGDATA:
                // It appears that type 3 = send text data
                // and type 4 = send image or unitext data
                // No idea if there is a type 1/2 or what they are.
                out.write(pi.sqlType.equals("text") ? (byte) 3 : (byte) 4);
                out.write((byte)0);
                out.write((byte)0);
                break;
            case SYBLONGBINARY:
                out.write((int)0x7FFFFFFF);
                break;
            case SYBBIT:
                break;
            case SYBINTN:
                out.write(pi.sqlType.equals("bigint") ? (byte) 8: (byte) 4);
                break;
            case SYBFLTN:
                if (pi.value instanceof Float) {
                    out.write((byte) 4);
                } else {
                    out.write((byte) 8);
                }
                break;
            case SYBDATETIMN:
                out.write((byte) 8);
                break;
            case SYBDATEN:
            case SYBTIMEN:
                out.write((byte)4);
                break;
            case SYBDECIMAL:
                out.write((byte) 17);
                out.write((byte) 38);

                if (pi.jdbcType == java.sql.Types.BIGINT) {
                    out.write((byte) 0);
                } else {
                    if (pi.value instanceof BigDecimal) {
                        out.write((byte) ((BigDecimal) pi.value).scale());
                    } else {
                        if (pi.scale >= 0 && pi.scale <= ConnectionImpl.DEFAULT_PRECISION_38) {
                            out.write((byte) pi.scale);
                        } else {
                            out.write((byte) ConnectionImpl.DEFAULT_SCALE);
                        }
                    }
                }

                break;
            default:
                throw new IllegalStateException(
                        "Unsupported output TDS type " + Integer.toHexString(pi.tdsType));
        }

        out.write((byte) 0); // Locale information
    }

    /**
     * Write the actual TDS 5 parameter data.
     *
     * @param pi          the parameter to output
     * @throws IOException
     * @throws SQLException
     */
    private void writeTds5Param(final ParamInfo pi)
    throws IOException {

        switch (pi.tdsType) {

            case SYBVARCHAR:
                if (pi.value == null) {
                    out.write((byte) 0);
                } else {
                    int len = pi.getAnsiLength(connection);
                    if (len > VAR_MAX) {
                        throw new IOException(Messages.get("error.generic.truncmbcs"));
                    }
                    if (len == 0) {
                        out.write((byte)1);
                        out.write((byte)' ');
                    } else {
                        out.write((byte)len);
                        pi.writeAnsi(out, connection);
                    }
                }

                break;

            case SYBVARBINARY:
                if (pi.value == null) {
                    out.write((byte) 0);
                } else {
                    if (pi.length == 0) {
                        // Sybase does not allow zero length binary
                        out.write((byte) 1); out.write((byte) 0);
                    } else {
                        out.write((byte) pi.length);
                        pi.writeBytes(out, connection);
                    }
                }

                break;

            case XSYBCHAR:
                if (pi.value == null) {
                    out.write((byte) 0);
                } else {
                    int len = pi.getAnsiLength(connection);
                    if (len > SYB_LONGVAR_MAX) {
                        throw new IOException(Messages.get("error.generic.truncmbcs"));
                    }
                    if (len == 0) {
                        out.write((int)1);
                        out.write((byte)' ');
                    } else {
                        out.write((int)len);
                        pi.writeAnsi(out, connection);
                    }
                }
                break;

            case SYBLONGDATA:
                //
                // Write a three byte prefix usage unknown
                //
                out.write((byte)0);
                out.write((byte)0);
                out.write((byte)0);
                //
                // Write BLOB direct from input stream
                //
                if (pi.value instanceof InputStream) {
                    byte buffer[] = new byte[SYB_CHUNK_SIZE];
                    int len = ((InputStream) pi.value).read(buffer);
                    while (len > 0) {
                        out.write((byte) len);
                        out.write((byte) (len >> 8));
                        out.write((byte) (len >> 16));
                        out.write((byte) ((len >> 24) | 0x80)); // 0x80 means more to come
                        out.write(buffer, 0, len);
                        len = ((InputStream) pi.value).read(buffer);
                    }
                } else
                //
                // Write CLOB direct from input Reader
                //
                if (pi.value instanceof Reader && !this.connection.isWideChar()) {
                    // For ASE 15+ the getNativeType() routine will already have
                    // read the data from the reader so this code will not be
                    // reached unless sendStringParametersAsUnicode=false.
                    char buffer[] = new char[SYB_CHUNK_SIZE];
                    int len = ((Reader) pi.value).read(buffer);
                    while (len > 0) {
                        out.write((byte) len);
                        out.write((byte) (len >> 8));
                        out.write((byte) (len >> 16));
                        out.write((byte) ((len >> 24) | 0x80)); // 0x80 means more to come
                        out.write(buffer, len, connection.getCharset());
                        len = ((Reader) pi.value).read(buffer);
                    }
                } else
                //
                // Write data from memory buffer
                //
                if (pi.value != null) {
                    //
                    // Actual data needs to be written out in chunks of
                    // 8192 bytes.
                    //
                    if (pi.sqlType.equals("unitext")) {
                        // Write out String as unicode bytes
                        pi.loadString(connection);
                        String buf = (String)pi.value;
                        int pos = 0;
                        while (pos < buf.length()) {
                            int clen = (buf.length() - pos >= SYB_CHUNK_SIZE / 2)?
                                                SYB_CHUNK_SIZE / 2: buf.length() - pos;
                            int len = clen * 2;
                            out.write((byte) len);
                            out.write((byte) (len >> 8));
                            out.write((byte) (len >> 16));
                            out.write((byte) ((len >> 24) | 0x80)); // 0x80 means more to come
                            // Write data
                            out.writeUnicode(buf.substring(pos, pos+clen));
                            pos += clen;
                        }
                    } else {
                        // Write text as bytes
                        pi.loadBytes(connection);
                        byte buf[] = (byte[])pi.value;
                        int pos = 0;
                        while (pos < buf.length) {
                            int len = (buf.length - pos >= SYB_CHUNK_SIZE)
                                    ? SYB_CHUNK_SIZE : buf.length - pos;
                            out.write((byte) len);
                            out.write((byte) (len >> 8));
                            out.write((byte) (len >> 16));
                            out.write((byte) ((len >> 24) | 0x80)); // 0x80 means more to come
                            // Write data
                            for (int i = 0; i < len; i++) {
                                out.write(buf[pos++]);
                            }
                        }
                    }
                }
                // Write terminator
                out.write((int) 0);
                break;

            case SYBLONGBINARY:
                // Sybase data <= 16284 bytes long
                if (pi.value == null) {
                    out.write((int) 0);
                } else {
                    if (pi.sqlType.startsWith("univarchar")){
                        pi.loadString(connection);
                        String tmp = (String)pi.value;
                        if (tmp.length() == 0) {
                            tmp = " ";
                        }
                        out.write((int)tmp.length() * 2);
                        out.writeUnicode(tmp);
                    } else {
                        if (pi.length > 0) {
                            out.write((int) pi.length);
                            pi.writeBytes(out, connection);
                        } else {
                            out.write((int) 1);
                            out.write((byte) 0);
                        }
                    }
                }
                break;

            case SYBINTN:
                if (pi.value == null) {
                    out.write((byte) 0);
                } else {
                    if (pi.sqlType.equals("bigint")) {
                        out.write((byte) 8);
                        out.write((long) ((Number) pi.value).longValue());
                    } else {
                        out.write((byte) 4);
                        out.write((int) ((Number) pi.value).intValue());
                    }
                }

                break;

            case SYBFLTN:
                if (pi.value == null) {
                    out.write((byte) 0);
                } else {
                    if (pi.value instanceof Float) {
                        out.write((byte) 4);
                        out.write(((Number) pi.value).floatValue());
                    } else {
                        out.write((byte) 8);
                        out.write(((Number) pi.value).doubleValue());
                    }
                }

                break;

            case SYBDATETIMN:
                out.write((DateTime) pi.value);
                break;

            case SYBDATEN:
                if (pi.value == null) {
                    out.write((byte)0);
                } else {
                    out.write((byte)4);
                    out.write((int)((DateTime) pi.value).getDate());
                }
                break;

           case SYBTIMEN:
               if (pi.value == null) {
                   out.write((byte)0);
               } else {
                   out.write((byte)4);
                   out.write((int)((DateTime) pi.value).getTime());
               }
               break;

            case SYBBIT:
                if (pi.value == null) {
                    out.write((byte) 0);
                } else {
                    out.write((byte) (((Boolean) pi.value).booleanValue() ? 1 : 0));
                }

                break;

            case SYBNUMERIC:
            case SYBDECIMAL:
                BigDecimal value = null;

                if (pi.value != null) {
                    if (pi.value instanceof Long) {
                        // Long to BigDecimal conversion is buggy. It's actually
                        // long to double to BigDecimal.
                        value = new BigDecimal(pi.value.toString());
                    } else {
                        value = (BigDecimal) pi.value;
                    }
                }

                out.write(value, TdsCore.SYBASE);
                break;

            default:
                throw new IllegalStateException(
                        "Unsupported output TDS type " + Integer.toHexString(pi.tdsType));
        }
    }

}
