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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import net.sourceforge.jtds.jdbc.cache.SQLCacheKey;
import net.sourceforge.jtds.jdbc.cache.SimpleLRUCache;

/**
 * Process JDBC escape strings and parameter markers in the SQL string.
 * <p>
 * This code recognizes the following escapes:
 * <ol>
 * <li>Date      {d 'yyyy-mm-dd'}
 * <li>Time      {t 'hh:mm:ss'}
 * <li>Timestamp {ts 'yyyy-mm-dd hh:mm:ss.nnn'}
 * <li>ESCAPE    {escape 'x'}
 * <li>Function  {fn xxxx([arg,arg...])}
 * NB The concat(arg, arg) operator is converted to (arg + arg)
 * <li>OuterJoin {oj .....}
 * <li>Call      {?=call proc [arg, arg...]}
 * or        {call proc [arg, arg...]}
 * </ol>
 * Notes:
 * <ol>
 * <li>This code is designed to be as efficient as possible and as
 * result the validation done here is limited.
 *
 * @author
 *    Mike Hutchinson, Holger Rehn
 */
class SQLParser {
    /**
     * Serialized version of a parsed SQL query (the value stored in the cache
     * for a parsed SQL).
     * <p/>
     * Holds the parsed SQL query and the names, positions and return value and
     * unicode flags for the parameters.
     */
    private static class CachedSQLQuery {
        final String[]  parsedSql;
        final String[]  paramNames;
        final int[]     paramMarkerPos;
        final boolean[] paramIsRetVal;
        final boolean[] paramIsUnicode;

        CachedSQLQuery(String[] parsedSql, ArrayList params) {
            this.parsedSql = parsedSql;

            if (params != null) {
                final int size = params.size();
                paramNames     = new String[size];
                paramMarkerPos = new int[size];
                paramIsRetVal  = new boolean[size];
                paramIsUnicode = new boolean[size];

                for (int i = 0; i < size; i++) {
                    ParamInfo paramInfo = (ParamInfo) params.get(i);
                    paramNames[i]     = paramInfo.name;
                    paramMarkerPos[i] = paramInfo.markerPos;
                    paramIsRetVal[i]  = paramInfo.isRetVal;
                    paramIsUnicode[i] = paramInfo.isUnicode;
                }
            } else {
                paramNames = null;
                paramMarkerPos = null;
                paramIsRetVal = null;
                paramIsUnicode = null;
            }
        }
    }

   /**
    * a LRU cache for the last 500 parsed SQL statements
    */
   private final static SimpleLRUCache<SQLCacheKey,CachedSQLQuery> _Cache = new SimpleLRUCache( 1000 );

    /** Original SQL string */
    private final String sql;
    /** Input buffer with SQL statement. */
    private final char[] in;
    /** Current position in input buffer. */
    private int s;
    /** Length of input buffer. */
    private final int len;
    /** Output buffer to contain parsed SQL. */
    private char[] out;
    /** Current position in output buffer. */
    private int d;
    /**
     * Parameter list to be populated or <code>null</code> if no parameters
     * are expected.
     */
    private final ArrayList params;
    /** Current expected terminator character. */
    private char terminator;
    /** Procedure name in call escape. */
    private String procName;
    /** First SQL keyword or identifier in statement. */
    private String keyWord;
    /** First table name in from clause */
    private String tableName;
    /** Connection object for server specific parsing. */
    private final JtdsConnection connection;

   /**
    * <p> Parse the SQL statement processing JDBC escapes and parameter markers.
    * </p>
    *
    * @param extractTable
    *    {@code true} to return the first table name in the FROM clause of a
    *    SELECT
    *
    * @return
    *    the processed SQL statement, any procedure name, the first SQL keyword
    *    and (optionally) the first table name as elements 0, 1, 2 and 3 of the
    *    returned {@code String[]}.
    *
    * @throws SQLException
    *    if a parse error occurs
    */
   static String[] parse( String sql, ArrayList paramList, JtdsConnection connection, boolean extractTable )
      throws SQLException
   {
      String[] ret;

      // don't cache extract table parse requests
      if( extractTable )
      {
         ret = new SQLParser( sql, paramList, connection ).parse( extractTable );
      }
      else
      {
         // By not synchronizing on the cache, we're admitting that the possibility
         // of multiple parses of the same statement can occur. However, it is
         //   1) unlikely under normal usage, and
         //   2) harmless to the cache.
         // By avoiding a synchronization block around the get()-parse()-put(), we
         // reduce the contention greatly in the nominal case.

         SQLCacheKey cacheKey = new SQLCacheKey( sql, connection );
         CachedSQLQuery cachedQuery = _Cache.get( cacheKey );

         if( cachedQuery == null )
         {
            // parse statement
            ret = new SQLParser( sql, paramList, connection ).parse( extractTable );

            // update LRU cache
            _Cache.put( cacheKey, new CachedSQLQuery( ret, paramList ) );
         }
         else
         {
            ret = cachedQuery.parsedSql;

            // create ParamInfo objects from CachedSQLQuery
            int length = cachedQuery.paramNames == null ? 0 : cachedQuery.paramNames.length;

            for( int i = 0; i < length; i ++ )
            {
               paramList.add( new ParamInfo( cachedQuery.paramNames[i], cachedQuery.paramMarkerPos[i], cachedQuery.paramIsRetVal[i], cachedQuery.paramIsUnicode[i] ) );
            }
         }
      }

      return ret;
   }

    // --------------------------- Private Methods --------------------------------

    /** Lookup table to test if character is part of an identifier. */
    private static boolean identifierChar[] = {
            false, false, false, false, false, false, false, false,
            false, false, false, false, false, false, false, false,
            false, false, false, false, false, false, false, false,
            false, false, false, false, false, false, false, false,
            false, false, false, true,  true,  false, false, false,
            false, false, false, false, false, false, false, false,
            true,  true,  true,  true,  true,  true,  true,  true,
            true,  true,  false, false, false, false, false, false,
            true,  true,  true,  true,  true,  true,  true,  true,
            true,  true,  true,  true,  true,  true,  true,  true,
            true,  true,  true,  true,  true,  true,  true,  true,
            true,  true,  true,  false, false, false, false, true,
            false, true,  true,  true,  true,  true,  true,  true,
            true,  true,  true,  true,  true,  true,  true,  true,
            true,  true,  true,  true,  true,  true,  true,  true,
            true,  true,  true,  false, false, false, false, false
    };

    /**
     * Determines if character could be part of an SQL identifier.
     * <p/>
     * Characters > 127 are assumed to be unicode letters in other
     * languages than english which is reasonable in this application.
     * @param ch the character to test.
     * @return <code>boolean</code> true if ch in A-Z a-z 0-9 @ $ # _.
     */
    private static boolean isIdentifier(int ch) {
        return ch > 127 || identifierChar[ch];
    }

    /**
     * Constructs a new parser object to process the supplied SQL.
     *
     * @param sqlIn     the SQL statement to parse
     * @param paramList the parameter list array to populate or
     *                  <code>null</code> if no parameters are expected
     * @param connection the parent Connection object
     */
    private SQLParser(String sqlIn, ArrayList paramList, JtdsConnection connection) {
        sql = sqlIn;
        in  = sql.toCharArray();
        len = in.length;
        out = new char[len];
        params = paramList;
        procName = "";

        this.connection = connection;
    }

    /**
     * Inserts a String literal in the output buffer.
     *
     * @param txt The text to insert.
     */
    private void copyLiteral(String txt) throws SQLException {
        final int len = txt.length();

        for (int i = 0; i < len; i++) {
            final char c = txt.charAt(i);

            if (c == '?') {
                if (params == null) {
                    throw new SQLException(
                            Messages.get("error.parsesql.unexpectedparam",
                                    String.valueOf(s)),
                            "2A000");
                }
                // param marker embedded in escape
                ParamInfo pi = new ParamInfo(d, connection.getUseUnicode());
                params.add(pi);
            }

            append(c);
        }
    }

    /**
     * Copies over an embedded string literal unchanged.
     */
    private void copyString() {
        char saveTc = terminator;
        char tc = in[s];

        if (tc == '[') {
            tc = ']';
        }

        terminator = tc;

        append(in[s++]);

        while (in[s] != tc) {
           append(in[s++]);
        }

        append(in[s++]);

        terminator = saveTc;
    }

    /**
     * Copies over possible SQL keyword eg 'SELECT'
     */
    private String copyKeyWord() {
        int start = d;

        while (s < len && isIdentifier(in[s])) {
           append(in[s++]);
        }

        return String.valueOf(out, start, d - start).toLowerCase();
    }

    /**
     * Builds a new parameter item.
     *
     * @param name Optional parameter name or null.
     * @param pos The parameter marker position in the output buffer.
     */
    private void copyParam(String name, int pos) throws SQLException {
        if (params == null) {
            throw new SQLException(
                    Messages.get("error.parsesql.unexpectedparam",
                            String.valueOf(s)),
                    "2A000");
        }

        ParamInfo pi = new ParamInfo(pos, connection.getUseUnicode());
        pi.name = name;

        if (pos >= 0) {
           append(in[s++]);
        } else {
            pi.isRetVal = true;
            s++;
        }

        params.add(pi);
    }

    /**
     * Copies an embedded stored procedure identifier over to the output buffer.
     *
     * @return The identifier as a <code>String</code>.
     */
    private String copyProcName() throws SQLException {
        int start = d;

        do {
            if (in[s] == '"' || in[s] == '[') {
                copyString();
            } else {
                char c = in[s++];

                while (isIdentifier(c) || c == ';') {
                    append(c);
                    c = in[s++];
                }

                s--;
            }

            if (in[s] == '.') {
                while (in[s] == '.') {
                    append(in[s++]);
                }
            } else {
                break;
            }
        } while (true);

        if (d == start) {
            // Procedure name expected but found something else
            throw new SQLException(
                    Messages.get("error.parsesql.syntax",
                            "call",
                            String.valueOf(s)),
                    "22025");
        }

        return new String(out, start, d - start);
    }

    /**
     * Copies an embedded parameter name to the output buffer.
     *
     * @return The identifier as a <code>String</code>.
     */
    private String copyParamName() {
        int start = d;
        char c = in[s++];

        while (isIdentifier(c)) {
            append(c);
            c = in[s++];
        }

        s--;

        return new String(out, start, d - start);
    }

    /**
     * Copies over white space.
     */
    private void copyWhiteSpace() {
        while (s < in.length && Character.isWhitespace(in[s])) {
            append(in[s++]);
        }
    }

    /**
     * Checks that the next character is as expected.
     *
     * @param c The expected character.
     * @param copy True if found character should be copied.
     * @throws SQLException if expected characeter not found.
     */
    private void mustbe(char c, boolean copy)
        throws SQLException {
        if (in[s] != c) {
            throw new SQLException(
                    Messages.get("error.parsesql.mustbe",
                            String.valueOf(s),
                            String.valueOf(c)),
                    "22019");
        }

        if (copy) {
            append(in[s++]);
        } else {
            s++;
        }
    }

   private void skipWhiteSpace()
      throws SQLException
   {
      for( ; s < len; )
      {
         // skip whitespace
         while( Character.isWhitespace( sql.charAt( s ) ) )
         {
            // skip white space without copying it
            s ++;
         }

         // check for comments to copy
         switch( sql.charAt( s ) )
         {
            case '-': // skip (and copy) single comment
                      if( s + 1 < len && in[s + 1] == '-' )
                      {
                         append( in[s ++] );
                         append( in[s ++] );

                         while( s < len && in[s] != '\n' && in[s] != '\r' )
                         {
                            append( in[s ++] );
                         }
                      }
                      else
                      {
                         return;
                      }

                      break;

            case '/': // skip (and copy) multi line comment
                      if( s + 1 < len && in[s + 1] == '*' )
                      {
                         append( in[s ++] );
                         append( in[s ++] );
                         int level = 1;

                        do
                        {
                           // ensure at least 2 chars available, otherwise */ cannot be found anymore
                           if( s >= len -1 )
                              throw new SQLException( Messages.get( "error.parsesql.missing", "*/" ), "22025" );

                           if( in[s] == '/' && s + 1 < len && in[s + 1] == '*' )
                           {
                              append( in[s ++] );
                              level ++;
                           }
                           else if( in[s] == '*' && s + 1 < len && in[s + 1] == '/' )
                           {
                              append( in[s ++] );
                              level --;
                           }

                           append( in[s ++] );
                        }
                        while( level > 0 );
                     }
                     else
                     {
                        return;
                     }

                     break;

            default: return;
         }
      }
   }

    /**
     * Skips single-line comments.
     */
    private void skipSingleComments() {
        while (s < len && in[s] != '\n' && in[s] != '\r') {
            // comments should be passed on to the server
            append(in[s++]);
        }
    }

    /**
     * Skips multi-line comments
     */
    private void skipMultiComments() throws SQLException {
        int block = 0;

        do {
            if (s < len - 1) {
                if (in[s] == '/' && in[s + 1] == '*') {
                   append(in[s++]);
                   block++;
                } else if (in[s] == '*' && in[s + 1] == '/') {
                   append(in[s++]);
                   block--;
                }
                // comments should be passed on to the server
                append(in[s++]);
            } else {
                throw new SQLException(
                        Messages.get("error.parsesql.missing", "*/"),
                        "22025");
            }
        } while (block > 0);
    }

    /**
     * Processes the JDBC {call procedure [(&#63;,&#63;,&#63;)]} type escape.
     *
     * @throws SQLException if an error occurs
     */
    private void callEscape() throws SQLException {
        // Insert EXECUTE into SQL so that proc can be called as normal SQL
        copyLiteral("EXECUTE ");
        keyWord = "execute";
        // Process procedure name
        procName = copyProcName();
        skipWhiteSpace();

        if (in[s] == '(') { // Optional ( )
            s++;
            terminator = ')';
            skipWhiteSpace();
        } else {
            terminator = '}';
        }

        append(' ');

        // Process any parameters
        while (in[s] != terminator) {
            String name = null;

            if (in[s] == '@') {
                // Named parameter
                name = copyParamName();
                skipWhiteSpace();
                mustbe('=', true);
                skipWhiteSpace();

                if (in[s] == '?') {
                    copyParam(name, d);
                } else {
                    // Named param has literal value can't call as RPC
                    procName = "";
                }
            } else if (in[s] == '?') {
                copyParam(name, d);
            } else {
                // Literal parameter can't call as RPC
                procName = "";
            }

            skipWhiteSpace();

            // Now find terminator or comma
            while (in[s] != terminator && in[s] != ',') {
                if (in[s] == '{') {
                    escape();
                } else if (in[s] == '\'' || in[s] == '[' || in[s] == '"') {
                    copyString();
                } else {
                    append(in[s++]);
                }
            }

            if (in[s] == ',') {
                append(in[s++]);
            }

            skipWhiteSpace();
        }

        if (terminator == ')') {
            s++; // Elide
        }

        terminator = '}';
        skipWhiteSpace();
    }

    /**
     * Utility routine to validate date and time escapes.
     *
     * @param mask The validation mask
     * @return True if the escape was valid and processed OK.
     */
    private boolean getDateTimeField(byte[] mask) throws SQLException {
        skipWhiteSpace();
        if (in[s] == '?') {
            // Allow {ts ?} type construct
            copyParam(null, d);
            skipWhiteSpace();
            return in[s] == terminator;
        }

        // fix for bug #682, CONVERT not allowed in procedure or function calls
        boolean sel = keyWord.equals( "select" );
        if( sel )
        {
           append( "convert(datetime,".toCharArray() );
        }

        append('\'');
        terminator = (in[s] == '\'' || in[s] == '"') ? in[s++] : '}';
        skipWhiteSpace();
        int ptr = 0;

        while (ptr < mask.length) {
            char c = in[s++];
            if (c == ' ' && out[d - 1] == ' ') {
                continue; // Eliminate multiple spaces
            }

            if (mask[ptr] == '#') {
                if (!Character.isDigit(c)) {
                    return false;
                }
            } else if (mask[ptr] != c) {
                return false;
            }

            if (c != '-') {
                append(c);
            }

            ptr++;
        }

        if (mask.length == 19) { // Timestamp
            int digits = 0;

            if (in[s] == '.') {
                append(in[s++]);

                while (Character.isDigit(in[s])) {
                    if (digits < 3) {
                        append(in[s++]);
                        digits++;
                    } else {
                        s++;
                    }
                }
            } else {
                append('.');
            }

            for (; digits < 3; digits++) {
                append('0');
            }
        }

        skipWhiteSpace();

        if (in[s] != terminator) {
            return false;
        }

        if (terminator != '}') {
            s++; // Skip terminator
        }

        skipWhiteSpace();
        append('\'');

        if( sel )
        {
           append(')');
        }


        return true;
    }

    /** Syntax mask for time escape. */
    private static final byte[] timeMask = {
        '#','#',':','#','#',':','#','#'
    };

    /** Syntax mask for date escape. */
    private static final byte[] dateMask = {
        '#','#','#','#','-','#','#','-','#','#'
    };

    /** Syntax mask for timestamp escape. */
    static final byte[] timestampMask = {
        '#','#','#','#','-','#','#','-','#','#',' ',
        '#','#',':','#','#',':','#','#'
    };

    /**
     * Processes the JDBC escape {oj left outer join etc}.
     *
     * @throws SQLException
     */
    private void outerJoinEscape()
        throws SQLException {
        while (in[s] != '}') {
            final char c = in[s];

            switch (c) {
                case '\'':
                case '"':
                case '[':
                    copyString();
                    break;
                case '{':
                    // Nested escape!
                    escape();
                    break;
                case '?':
                    copyParam(null, d);
                    break;
                default:
                    append(c);
                    s++;
                    break;
            }
        }
    }

    /** Map of jdbc to sybase function names. */
    private static HashMap fnMap = new HashMap();
    /** Map of jdbc to sql server function names. */
    private static HashMap msFnMap = new HashMap();
    /** Map of jdbc to server data types for convert */
    private static HashMap cvMap = new HashMap();

    static {
        // Microsoft only functions
        msFnMap.put("length", "len($)");
        msFnMap.put("truncate", "round($, 1)");
        // Common functions
        fnMap.put("user",     "user_name($)");
        fnMap.put("database", "db_name($)");
        fnMap.put("ifnull",   "isnull($)");
        fnMap.put("now",      "getdate($)");
        fnMap.put("atan2",    "atn2($)");
        fnMap.put("mod",      "($)");
        fnMap.put("length",   "char_length($)");
        fnMap.put("locate",   "charindex($)");
        fnMap.put("repeat",   "replicate($)");
        fnMap.put("insert",   "stuff($)");
        fnMap.put("lcase",    "lower($)");
        fnMap.put("ucase",    "upper($)");
        fnMap.put("concat",   "($)");
        fnMap.put("curdate",  "convert(datetime, convert(varchar, getdate(), 112))");
        fnMap.put("curtime",  "convert(datetime, convert(varchar, getdate(), 108))");
        fnMap.put("dayname",  "datename(weekday,$)");
        fnMap.put("dayofmonth", "datepart(day,$)");
        fnMap.put("dayofweek", "((datepart(weekday,$)+@@DATEFIRST-1)%7+1)");
        fnMap.put("dayofyear",  "datepart(dayofyear,$)");
        fnMap.put("hour",       "datepart(hour,$)");
        fnMap.put("minute",     "datepart(minute,$)");
        fnMap.put("second",     "datepart(second,$)");
        fnMap.put("year",       "datepart(year,$)");
        fnMap.put("quarter",    "datepart(quarter,$)");
        fnMap.put("month",      "datepart(month,$)");
        fnMap.put("week",       "datepart(week,$)");
        fnMap.put("monthname",  "datename(month,$)");
        fnMap.put("timestampadd", "dateadd($)");
        fnMap.put("timestampdiff", "datediff($)");
        // convert jdbc to sql types
        cvMap.put("binary", "varbinary");
        cvMap.put("char", "varchar");
        cvMap.put("date", "datetime");
        cvMap.put("double", "float");
        cvMap.put("longvarbinary", "image");
        cvMap.put("longvarchar", "text");
        cvMap.put("time", "datetime");
        cvMap.put("timestamp", "timestamp");
    }

    /**
     * Processes the JDBC escape {fn function()}.
     *
     * @throws SQLException
     */
    private void functionEscape() throws SQLException {
        char tc = terminator;
        skipWhiteSpace();
        StringBuilder nameBuf = new StringBuilder();
        //
        // Capture name
        //
        while (isIdentifier(in[s])) {
            nameBuf.append(in[s++]);
        }

        String name = nameBuf.toString().toLowerCase();
        //
        // Now collect arguments
        //
        skipWhiteSpace();
        mustbe('(', false);
        int parenCnt = 1;
        int argStart = d;
        int arg2Start = 0;
        terminator = ')';
        while (in[s] != ')' || parenCnt > 1) {
            final char c = in[s];

            switch (c) {
                case '\'':
                case '"':
                case '[':
                    copyString();
                    break;
                case '{':
                    // Process nested escapes!
                    escape();
                    break;
                case ',':
                     if( parenCnt == 1 )
                     {
                        if( arg2Start == 0 )
                        {
                           arg2Start = d - argStart;
                        }
                        if( "concat".equals( name ) )
                        {
                           append( '+' );
                           s++;
                        }
                        else if( "mod".equals( name ) )
                        {
                           append( '%' );
                           s++;
                        }
                        else
                        {
                           append( c );
                           s++;
                        }
                     }
                     else
                     {
                        append( c );
                        s++;
                     }
                     break;
                case '(':
                    parenCnt++;
                    append(c); s++;
                    break;
                case ')':
                    parenCnt--;
                    append(c); s++;
                    break;
                default:
                    append(c); s++;
                    break;
            }
        }

        String args = String.valueOf(out, argStart, d - argStart).trim();

        d = argStart;
        mustbe(')', false);
        terminator = tc;
        skipWhiteSpace();

        //
        // Process convert scalar function.
        // Arguments need to be reversed and the data type
        // argument converted to an SQL server type
        //
        if ("convert".equals(name) && arg2Start < args.length() - 1) {
            String arg2 = args.substring(arg2Start + 1).trim().toLowerCase();
            String dataType = (String) cvMap.get(arg2);

            if (dataType == null) {
                // Will get server error if invalid type passed
                dataType = arg2;
            }

            copyLiteral("convert(");
            copyLiteral(dataType);
            append(',');
            copyLiteral(args.substring(0, arg2Start));
            append(')');

            return;
        }

        //
        // See if function mapped
        //
        String fn;
        if (connection.getServerType() == Driver.SQLSERVER) {
            fn = (String) msFnMap.get(name);
            if (fn == null) {
                fn = (String) fnMap.get(name);
            }
        } else {
            fn = (String) fnMap.get(name);
        }
        if (fn == null) {
            // Not mapped so assume simple case
            copyLiteral(name);
            append('(');
            copyLiteral(args);
            append(')');
            return;
        }
        //
        // Process timestamp interval constants
        //
        if (args.length() > 8
            && args.substring(0, 8).equalsIgnoreCase("sql_tsi_")) {
            args = args.substring(8);
            if (args.length() > 11
                && args.substring(0, 11).equalsIgnoreCase("frac_second")) {
                args = "millisecond" + args.substring(11);
            }
        }
        //
        // Substitute mapped function name and arguments
        //
        final int len = fn.length();
        for (int i = 0; i < len; i++) {
            final char c = fn.charAt(i);
            if (c == '$') {
                // Substitute arguments
                copyLiteral(args);
            } else {
                append(c);
            }
        }
    }

    /**
     * Processes the JDBC escape {escape 'X'}.
     *
     * @throws SQLException
     */
    private void likeEscape() throws SQLException {
        copyLiteral("escape ");
        skipWhiteSpace();

        if (in[s] == '\'' || in[s] == '"') {
            copyString();
        } else {
            mustbe('\'', true);
        }

        skipWhiteSpace();
    }

    /**
     * Processes the JDBC escape sequences.
     *
     * @throws SQLException
     */
    private void escape() throws SQLException {
        char tc = terminator;
        terminator = '}';
        StringBuilder escBuf = new StringBuilder();
        s++;
        skipWhiteSpace();

        if (in[s] == '?') {
            copyParam("@return_status", -1);
            skipWhiteSpace();
            mustbe('=', false);
            skipWhiteSpace();

            while (Character.isLetter(in[s])) {
                escBuf.append(Character.toLowerCase(in[s++]));
            }

            skipWhiteSpace();
            String esc = escBuf.toString();

            if ("call".equals(esc)) {
                callEscape();
            } else {
                throw new SQLException(
                        Messages.get("error.parsesql.syntax",
                                "call",
                                String.valueOf(s)),
                        "22019");
            }
        } else {
            while (Character.isLetter(in[s])) {
                escBuf.append(Character.toLowerCase(in[s++]));
            }

            skipWhiteSpace();
            String esc = escBuf.toString();

            if ("call".equals(esc)) {
                callEscape();
            } else if ("t".equals(esc)) {
                if (!getDateTimeField(timeMask)) {
                    throw new SQLException(
                            Messages.get("error.parsesql.syntax",
                                         "time",
                                         String.valueOf(s)),
                            "22019");
                }
            } else if ("d".equals(esc)) {
                if (!getDateTimeField(dateMask)) {
                    throw new SQLException(
                            Messages.get("error.parsesql.syntax",
                                         "date",
                                         String.valueOf(s)),
                            "22019");
                }
            } else if ("ts".equals(esc)) {
                if (!getDateTimeField(timestampMask)) {
                    throw new SQLException(
                            Messages.get("error.parsesql.syntax",
                                         "timestamp",
                                         String.valueOf(s)),
                            "22019");
                }
            } else if ("oj".equals(esc)) {
                outerJoinEscape();
            } else if ("fn".equals(esc)) {
                functionEscape();
            } else if ("escape".equals(esc)) {
                likeEscape();
            } else {
                throw new SQLException(
                        Messages.get("error.parsesql.badesc",
                                esc,
                                String.valueOf(s)),
                        "22019");
            }
        }

        mustbe('}', false);
        terminator = tc;
    }

    /**
     * Extracts the first table name following the keyword FROM.
     *
     * @return the table name as a <code>String</code>
     */
    private String getTableName() throws SQLException {
        StringBuilder name = new StringBuilder(128);
        copyWhiteSpace();
        char c = (s < len) ? in[s] : ' ';
        if (c == '{') {
            // Start of {oj ... } we can assume that there is
            // more than one table in select and therefore
            // it would not be updateable.
            return "";
        }
        //
        // Skip any leading comments before first table name
        //
        while (c == '/' || c == '-' && s + 1 < len) {
            if (c == '/') {
                if (in[s + 1] == '*') {
                    skipMultiComments();
                } else {
                    break;
                }
            } else {
                if (in[s + 1] == '-') {
                    skipSingleComments();
                } else {
                    break;
                }
            }
            copyWhiteSpace();
            c = (s < len) ? in[s] : ' ';
        }

        if (c == '{') {
            // See comment above
            return "";
        }
        //
        // Now process table name
        //
        while (s < len) {
            if (c == '[' || c == '"') {
                int start = d;
                copyString();
                name.append(String.valueOf(out, start, d - start));
                copyWhiteSpace();
                c = (s < len) ? in[s] : ' ';
            } else {
                int start = d;
                c = (s < len) ? in[s++] : ' ';
                while ((isIdentifier(c))
                       && c != '.'
                       && c != ',') {
                    append(c);
                    c = (s < len) ? in[s++] : ' ';
                }
                name.append(String.valueOf(out, start, d - start));
                s--;
                copyWhiteSpace();
                c = (s < len) ? in[s] : ' ';
            }
            if (c != '.') {
                break;
            }
            name.append(c);
            append(c); s++;
            copyWhiteSpace();
            c = (s < len) ? in[s] : ' ';
        }
        return name.toString();
    }

   private final void append( char[] chars )
   {
      for( char c : chars )
      {
         append( c );
      }
   }

   /**
    * <p> Adds the given character to {@link #out}, incrementing {@link #d} by
    * {@code 1} and expanding {@link #out} by a fixed number of characters if
    * necessary. </p>
    */
   private final void append( char character )
   {
      try
      {
         out[d ++] = character;
      }
      catch( ArrayIndexOutOfBoundsException e )
      {
         // expand output array by a fixed amount
         char[] expanded = new char[out.length + 256];
         System.arraycopy( out, 0, expanded, 0, out.length );

         //
         out = expanded;
         out[d - 1] = character;
      }
   }

    /**
     * Parses the SQL statement processing JDBC escapes and parameter markers.
     *
     * @param extractTable true to return the first table name in the FROM clause of a select
     * @return The processed SQL statement, any procedure name, the first
     * SQL keyword and (optionally) the first table name as elements 0 1, 2 and 3 of the
     * returned <code>String[]</code>.
     * @throws SQLException
     */
    String[] parse(boolean extractTable) throws SQLException {
        boolean isSelect   = false;
        boolean isModified = false;
        boolean isSlowScan = true;
        try {
            while (s < len) {
                final char c = in[s];

                switch (c) {
                    case '{':
                        escape();
                        isModified = true;
                        break;
                    case '[':
                    case '"':
                    case '\'':
                        copyString();
                        break;
                    case '?':
                        copyParam(null, d);
                        break;
                    case '/':
                        if (s+1 < len && in[s+1] == '*') {
                            skipMultiComments();
                        } else {
                            append(c); s++;
                        }
                        break;
                    case '-':
                        if (s+1 < len && in[s+1] == '-') {
                            skipSingleComments();
                        } else {
                            append(c); s++;
                        }
                        break;
                    default:
                        if (isSlowScan && Character.isLetter(c)) {
                            if (keyWord == null) {
                                keyWord = copyKeyWord();
                                if ("select".equals(keyWord)) {
                                    isSelect = true;
                                }
                                isSlowScan = extractTable && isSelect;
                                break;
                            }
                            if (extractTable && isSelect) {
                                String sqlWord = copyKeyWord();
                                if ("from".equals(sqlWord)) {
                                    // Ensure only first 'from' is processed
                                    isSlowScan = false;
                                    tableName = getTableName();
                                }
                                break;
                            }
                        }

                        append(c); s++;
                        break;
                }
            }

            //
            // Impose a reasonable maximum limit on the number of parameters
            // unless the connection is sending statements unprepared (i.e. by
            // building a plain query) and this is not a procedure call.
            //
            if (params != null && params.size() > 255
                    && connection.getPrepareSql() != TdsCore.UNPREPARED
                    && procName != null) {
                int limit = 255; // SQL 6.5 and Sybase < 12.50
                if (connection.getServerType() == Driver.SYBASE) {
                    if (connection.getDatabaseMajorVersion() > 12 ||
                            connection.getDatabaseMajorVersion() == 12 &&
                            connection.getDatabaseMinorVersion() >= 50) {
                        limit = 2000; // Actually 2048 but allow some head room
                    }
                } else {
                    if (connection.getDatabaseMajorVersion() == 7) {
                        limit = 1000; // Actually 1024
                    } else
                    if (connection.getDatabaseMajorVersion() > 7) {
                        limit = 2000; // Actually 2100
                    }

                }
                if (params.size() > limit) {
                    throw new SQLException(
                        Messages.get("error.parsesql.toomanyparams",
                                Integer.toString(limit)),
                        "22025");
                }
            }
            String result[] = new String[4];

            // return sql and procname
            result[0] = (isModified) ? new String(out, 0, d) : sql;
            result[1] = procName;
            result[2] = (keyWord == null) ? "" : keyWord;
            result[3] = tableName;
            return result;
        } catch (IndexOutOfBoundsException e) {
            // Should only come here if string is invalid in some way.
            throw new SQLException(
                    Messages.get("error.parsesql.missing",
                            String.valueOf(terminator)),
                    "22025");
        }
    }
}