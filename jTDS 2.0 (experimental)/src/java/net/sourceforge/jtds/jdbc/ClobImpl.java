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
import java.sql.Clob;
import java.sql.SQLException;

/**
 * An in-memory or disk based representation of character data.
 * <p/>
 * Implementation note:
 * <ol>
 *   <li>This implementation stores the CLOB data in a byte array managed by
 *     the <code>BlobBuffer</code> class. Each character is stored in 2
 *     sequential bytes using UTF-16LE encoding.
 *   <li>As a consequence of using UTF-16LE, Unicode 3.1 supplementary
 *     characters may require an additional 2 bytes of storage. This
 *     implementation assumes that character position parameters supplied to
 *     <code>getSubstring</code>, <code>position</code> and the
 *     <code>set</code> methods refer to 16 bit characters only. The presence
 *     of supplementary characters will cause the wrong characters to be
 *     accessed.
 *   <li>For the same reasons although the position method will return the
 *     correct start position for any given pattern in the array, the returned
 *     value may be different to that expected if supplementary characters
 *     exist in the text preceding the pattern.
 * </ol>
 *
 * @author Brian Heineman
 * @author Mike Hutchinson
 * @version $Id: ClobImpl.java,v 1.3 2009-09-27 12:59:02 ickzon Exp $
 */

public class ClobImpl extends BlobBuffer implements Clob, Serializable {
    static final long serialVersionUID = 4695282417227073857L;
    /**
     * 0 length <code>String</code> as initial value for empty
     * <code>Clob</code>s.
     */
    private static final String EMPTY_CLOB = "";

    /**
     * Constructs a new empty <code>Clob</code> instance.
     *
     * @param connection a reference to the parent connection object
     */
    ClobImpl(final ConnectionImpl connection) {
        this(connection, EMPTY_CLOB);
    }

    /**
     * Constructs a new initialized <code>Clob</code> instance.
     *
     * @param connection a reference to the parent connection object
     * @param str        the <code>String</code> object to encapsulate
     */
    ClobImpl(final ConnectionImpl connection, final String str) {
        super(connection.getLobBuffer());
        if (str == null) {
            throw new IllegalArgumentException("str cannot be null");
        }
        try {
            byte[] data = str.getBytes("UTF-16LE");
            setBuffer(data, false);
        } catch (UnsupportedEncodingException e) {
            // This should never happen!
            throw new IllegalStateException("UTF-16LE encoding is not supported.");
        }
    }

    /**
     * Creates a CLOB from an InputStream.
     *
     * @param connection the parent Connection Object.
     * @param in the InputStream with the CLOB data.
     * @param length the length of the CLOB data or -2 if not known.
     */
    ClobImpl(final ConnectionImpl connection, 
             final InputStream in, 
             final int length) throws IOException 
    {
        super(connection, in, length);
    }
    
    /**
     * Creates a CLOB from a Reader.
     *
     * @param connection the parent Connection Object.
     * @param rdr the Reader with the CLOB data.
     * @param length the length of the CLOB data or -2 if not known.
     */
    ClobImpl(final ConnectionImpl connection, 
             final Reader rdr, 
             final int length) throws IOException 
    {
        super(connection.getLobBuffer());
        
        if (length >= 0 && length <= maxMemSize) {
            // OK to cache in memory
            byte[] data = new byte[length * 2];
            int p = 0;
            int c;
            while ((c = rdr.read()) >= 0) {
                data[p++] = (byte)c;
                data[p++] = (byte)(c >> 8);
            }
            rdr.close();
            setBuffer(data, false);
            if (p == 2 && data[0] == 0x20 && data[1] == 0
                && connection.getTdsVersion() < TdsCore.TDS70) {
                // Single space with Sybase equates to empty string
                p = 0;
            }
            // Explicitly set length as multi byte character sets
            // may not fill array completely.
            setLength(p);
        } else {
            // Too big, need to write straight to disk
            OutputStream out = setBinaryStream(1, false);
            int c;
            while ((c = rdr.read()) >= 0) {
                out.write(c);
                out.write(c >> 8);
            }
            out.close();
            rdr.close();
        }
    }

    //
    // ---- java.sql.Blob interface methods from here ----
    //

    public InputStream getAsciiStream() throws SQLException {
        return getBinaryStream(true);
    }

    public Reader getCharacterStream() throws SQLException {
        try {
            return new BufferedReader(new InputStreamReader(
                    getBinaryStream(false), "UTF-16LE"));
        } catch (UnsupportedEncodingException e) {
            // This should never happen!
            throw new IllegalStateException(
                    "UTF-16LE encoding is not supported.");
        }
    }

    public String getSubString(long pos, int length) throws SQLException {
        if (length == 0) {
            return EMPTY_CLOB;
        }
        try {
            byte data[] = getBytes((pos - 1) * 2 + 1, length * 2);
            return new String(data, "UTF-16LE");
        } catch (IOException e) {
            throw new SQLException(Messages.get("error.generic.ioerror",
                    e.getMessage()),
                    "HY000");
        }
    }

    public long length() throws SQLException {
        return super.getLength() / 2;
    }

    public long position(String searchStr, long start) throws SQLException {
        if (searchStr == null) {
            throw new SQLException(
                    Messages.get("error.clob.searchnull"), "HY009");
        }
        try {
            byte[] pattern = searchStr.getBytes("UTF-16LE");
            int pos = (int)position(pattern, (start - 1) * 2 + 1);
            return (pos < 0) ? pos : (pos - 1) / 2 + 1;
        } catch (UnsupportedEncodingException e) {
            // This should never happen!
            throw new IllegalStateException(
                    "UTF-16LE encoding is not supported.");
        }
    }

    public long position(Clob searchStr, long start) throws SQLException {
        if (searchStr == null) {
            throw new SQLException(
                    Messages.get("error.clob.searchnull"), "HY009");
        }
        byte[] pattern = ((ClobImpl)searchStr).getBytes(1, (int) ((ClobImpl)searchStr).getLength());
        int pos = (int)position(pattern, (start - 1) * 2 + 1);
        return (pos < 0) ? pos : (pos - 1) / 2 + 1;
    }

    public OutputStream setAsciiStream(final long pos) throws SQLException {
        try {
            return setBinaryStream((pos - 1) * 2 + 1, true);
        } catch (IllegalArgumentException e) {
            throw new SQLException(e.getMessage(), "HY090");
        } catch (IOException e) {
            throw new SQLException(Messages.get("error.generic.ioerror",
                    e.getMessage()),
                    "HY000");
        }
    }

    public Writer setCharacterStream(final long pos) throws SQLException {
        try {
            return new BufferedWriter(new OutputStreamWriter(
                    setBinaryStream((pos - 1) * 2 + 1, false),
                    "UTF-16LE"));
        } catch (IllegalArgumentException e) {
            throw new SQLException(e.getMessage(), "HY090");
        } catch (IOException e) {
            throw new SQLException(Messages.get("error.generic.ioerror",
                    e.getMessage()),
                    "HY000");
        }
    }

    public int setString(long pos, String str) throws SQLException {
        if (str == null) {
            throw new SQLException(
                    Messages.get("error.clob.strnull"), "HY009");
        }
        return setString(pos, str, 0, str.length());
    }

    public int setString(long pos, String str, int offset, int len)
            throws SQLException {
        if (offset < 0 || offset > str.length()) {
            throw new SQLException(Messages.get(
                    "error.blobclob.badoffset"), "HY090");
        }
        if (len < 0 || offset + len > str.length()) {
            throw new SQLException(
                    Messages.get("error.blobclob.badlen"), "HY090");
        }
        try {
            byte[] data = str.substring(offset, offset + len)
                    .getBytes("UTF-16LE");
            // No need to force BlobBuffer to copy the bytes as this is a local
            // buffer and cannot be corrupted by the user.
            return setBytes((pos - 1) * 2 + 1, data, 0, data.length, false);
        } catch (UnsupportedEncodingException e) {
            // This should never happen!
            throw new IllegalStateException(
                    "UTF-16LE encoding is not supported.");
        }
    }

    public void truncate(long len) throws SQLException {
        super.truncate(len * 2);
    }

    @Override
    public void free() throws SQLException {
        throw new AbstractMethodError();
    }

    @Override
    public Reader getCharacterStream(long pos, long length) throws SQLException {
        throw new AbstractMethodError();
    }
}
