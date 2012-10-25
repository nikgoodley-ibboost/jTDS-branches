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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Properties;

import junit.framework.TestCase;

/**
 * @author  builder
 * @version $Id: TestBase.java,v 1.21.2.1 2009-08-04 10:33:54 ickzon Exp $
 */
public abstract class TestBase extends TestCase {

    private static final String CONNECTION_PROPERTIES = "conf/connection.properties";
    public static final Properties props = loadProperties(CONNECTION_PROPERTIES);
    Connection con;

    public TestBase(String name) {
        super(name);
    }

   public void setUp() throws Exception {
        super.setUp();
        connect();
    }

   public void tearDown() throws Exception {
        assertFalse( Thread.currentThread().isInterrupted() );
        disconnect();
        super.tearDown();
    }

    public Connection getConnection() throws Exception {
        Class.forName(props.getProperty("driver"));
        String url = props.getProperty("url");
        props.setProperty( Messages.get(Driver.LANGUAGE), "us_english" );
        return DriverManager.getConnection(url, props);
    }

    public Connection getConnection(Properties override) throws Exception {
        Properties newProps = new Properties(props);
        for (Iterator it = override.keySet().iterator(); it.hasNext();) {
            String key = (String) it.next();
            newProps.setProperty(key, override.getProperty(key));
        }
        newProps.setProperty( Messages.get(Driver.LANGUAGE), "us_english" );
        Class.forName(newProps.getProperty("driver"));
        String url = newProps.getProperty("url");
        return DriverManager.getConnection(url, newProps);
    }

    private void disconnect() throws Exception {
        if (con != null) {
            con.close();
            con = null;
        }
    }

    protected void connect() throws Exception {
        disconnect();
        con = getConnection();
    }

    public void dump(ResultSet rs) throws SQLException {
        dump( rs, false );
    }

    public void dump(ResultSet rs, boolean silent) throws SQLException {
        ResultSetMetaData rsm = rs.getMetaData();
        int cols = rsm.getColumnCount();

        for (int i = 1; i <= cols; i++) {
            if (i > 1) {
                if( ! silent ) {
                   System.out.print(", ");
                }
            }

            String col = rsm.getColumnName(i);
            if( ! silent ) {
                System.out.print(col);
            }
        }

        if( ! silent ) {
            System.out.println();
        }

        while (rs.next()) {
            dumpRow(rs,silent);
        }
    }

    public void dumpRow(ResultSet rs) throws SQLException {
        dumpRow( rs, false );
    }

    public void dumpRow(ResultSet rs, boolean silent) throws SQLException {
        ResultSetMetaData rsm = rs.getMetaData();
        int cols = rsm.getColumnCount();

        for (int i = 1; i <= cols; i++) {
            if (i > 1 && ! silent) {
                System.out.print(", ");
            }

            Object o = rs.getObject(i);
            if( ! silent ) {
                System.out.print(o);
            }
        }

        if( ! silent ) {
            System.out.println();
        }
    }

    private static Properties loadProperties(String fileName) {

        File propFile = new File(fileName);

        if (!propFile.exists()) {
            fail("Connection properties not found (" + propFile + ").");
        }

        try {
            Properties props = new Properties();
            props.load(new FileInputStream(propFile));
            return props;
        }
        catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    protected void makeTestTables(Statement stmt) throws SQLException {
        String sql = "CREATE TABLE #test ("
                     + " f_int INT,"
                     + " f_varchar VARCHAR(255))";

        stmt.execute(sql);
    }

    public void makeObjects(Statement stmt, int count) throws SQLException {
        stmt.execute("TRUNCATE TABLE #test");

        for (int i = 0; i < count; i++) {
            String sql = "INSERT INTO #test(f_int, f_varchar)"
                         + " VALUES (" + i + ", 'Row " + i + "')";
            stmt.execute(sql);
        }
    }

    public void compareInputStreams(InputStream is1, InputStream is2) throws IOException {
        try {
            if (is1 == null && is2 == null) {
                return;
            } else if (is1 == null) {
                fail("is1 == null && is2 != null");
                return;
            } else if (is2 == null) {
                fail("is1 != null && is2 == null");
                return;
            }

            long count = 0;
            int res1 = 0, res2 = 0;
            byte buf1[] = new byte[1024], buf2[] = new byte[1024];

            while (res1 != 0 || (res1 = is1.read(buf1)) != -1) {
                if (res2 == 0) {
                    res2 = is2.read(buf2);
                }

                if (res2 == -1) {
                    fail("stream 2 EOF at: " + count);
                }

                int min = Math.min(res1, res2);
                for (int i = 0; i < min; i++) {
                    // Do the check first rather than using assertTrue()
                    // assertTrue() would create a String at each iteration
                    if (buf1[i] != buf2[i]) {
                        fail("stream 1 value [" + buf1[i]
                                + "] differs from stream 2 value ["
                                + buf2[i] + "] at: " + (count + i));
                    }
                }

                count += min;

                if (res1 != min) {
                    System.arraycopy(buf1, min, buf1, 0, res1 - min);
                    res1 -= min;
                } else {
                    res1 = 0;
                }

                if (res2 != min) {
                    System.arraycopy(buf2, min, buf2, 0, res2 - min);
                    res2 -= min;
                } else {
                    res2 = 0;
                }
            }

            if (is2.read() != -1) {
                fail("stream 1 EOF at: " + count);
            }
        } finally {
            if (is1 != null) {
                is1.close();
            }

            if (is2 != null) {
                is2.close();
            }
        }
    }

    public void compareReaders(Reader r1, Reader r2) throws IOException {
        try {
            if (r1 == null && r2 == null) {
                return;
            } else if (r1 == null) {
                fail("r1 == null && r2 != null");
                return;
            } else if (r2 == null) {
                fail("r1 != null && r2 == null");
                return;
            }

            long count = 0;
            int res1 = 0, res2 = 0;
            char buf1[] = new char[1024], buf2[] = new char[1024];

            while (res1 != 0 || (res1 = r1.read(buf1)) != -1) {
                if (res2 == 0) {
                    res2 = r2.read(buf2);
                }

                if (res2 == -1) {
                    fail("reader 2 EOF at: " + count);
                }

                int min = Math.min(res1, res2);
                for (int i = 0; i < min; i++) {
                    // Do the check first rather than using assertTrue()
                    // assertTrue() would create a String at each iteration
                    if (buf1[i] != buf2[i]) {
                        fail("stream 1 value [" + buf1[i]
                                + "] differs from stream 2 value ["
                                + buf2[i] + "] at: " + (count + i));
                    }
                }

                count += min;

                if (res1 != min) {
                    System.arraycopy(buf1, min, buf1, 0, res1 - min);
                    res1 -= min;
                } else {
                    res1 = 0;
                }

                if (res2 != min) {
                    System.arraycopy(buf2, min, buf2, 0, res2 - min);
                    res2 -= min;
                } else {
                    res2 = 0;
                }
            }

            if (r2.read() != -1) {
                fail("reader 1 EOF at: " + count);
            }
        } finally {
            if (r1 != null) {
                r1.close();
            }

            if (r2 != null) {
                r2.close();
            }
        }
    }

   public void dropDatabase( String name )
      throws SQLException
   {
      Statement stm = con.createStatement();

      try
      {
         stm.executeUpdate( "if exists (select * from sys.sysdatabases where name = '" + name + "') drop database " + name );
      }
      catch( SQLException sqle )
      {
         // assume the database didn't exist
      }
      finally
      {
         stm.close();
      }
   }

   public void dropTable( String name )
      throws SQLException
   {
      Statement stm = con.createStatement();

      try
      {
         stm.executeUpdate( "if exists (select * from " + ( name.startsWith( "#" ) ? "tempdb.dbo.sysobjects" : "sysobjects" ) + " where name like '" + name + "%' and type = 'U') drop table " + name );
      }
      catch( SQLException sqle )
      {
         // assume the table didn't exist
      }
      finally
      {
         stm.close();
      }
   }

   public void dropView( String name )
      throws SQLException
   {
      Statement stm = con.createStatement();

      try
      {
         stm.executeUpdate( "if exists (select * from sysobjects where name like '" + name + "%' and type = 'V') drop view " + name );
      }
      catch( SQLException sqle )
      {
         // assume the view didn't exist
      }
      finally
      {
         stm.close();
      }
   }

   public void dropFunction( String name )
      throws SQLException
   {
      Statement stm = con.createStatement();

      try
      {
         stm.executeUpdate( "if exists (select * from sysobjects where name like '" + name + "%' and type = 'FN') drop function " + name );
      }
      catch( SQLException sqle )
      {
         // assume the function didn't exist
      }
      finally
      {
         stm.close();
      }
   }

   public void dropProcedure( String name )
      throws SQLException
   {
      Statement stm = con.createStatement();

      try
      {
         stm.executeUpdate( "if exists (select * from " + ( name.startsWith( "#" ) ? "tempdb.dbo.sysobjects" : "sysobjects" ) + " where name like '" + name + "%' and type = 'P') drop procedure " + name );
      }
      catch( SQLException sqle )
      {
        // assume the procedure didn't exist
      }
      finally
      {
         stm.close();
      }
   }

   public void dropType( String name )
      throws Exception
   {
      CallableStatement stm = con.prepareCall( "{call sp_droptype '" + name + "'}" );

      try
      {
         stm.executeUpdate();
      }
      catch( SQLException sqle )
      {
         // assume the type didn't exist
      }
      finally
      {
         stm.close();
      }
   }

}