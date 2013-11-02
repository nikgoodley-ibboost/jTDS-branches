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

import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;

import junit.framework.Assert;
import junit.framework.AssertionFailedError;
//
// MJH - Changes for new jTDS version
// Added registerOutParameter to testCallableStatementParsing2
//
/**
 * @version 1.0
 */
public class CallableStatementTest extends TestBase
{

   /** set to false to enable verbose console output */
   private final boolean SILENT = true;

   public CallableStatementTest( String name )
   {
      super( name );
   }

   /**
    * Test for infinite loop in comment processing, bug #715.
    */
   public void testBug715()
      throws SQLException, InterruptedException
   {
      dropProcedure( "sp_bug715" );

      Statement st = con.createStatement();
      st.execute( "create procedure sp_bug715 @data1 int as select 'bug715'" );
      st.close();

      final Throwable[] err = new Throwable[1];
      final Statement[] sta = new Statement[1];

      // start a separate thread to be able to abort if running into infinite loop
      Thread t = new Thread( new Runnable()
      {
         @Override
         public void run()
         {
            try
            {
               sta[0] = con.prepareCall( "{call sp_bug715(-1)}" );
            }
            catch( SQLException e )
            {
               err[0] = e;
            }
         }
      } );
      t.start();

      t.join( 10000 );

      if( t.isAlive() )
      {
         sta[0].cancel();
         Assert.fail( "locked in infinite loop, thread still running" );
      }
   }

   /**
    * Test stored procedure with string parameter, bug #713.
    */
   public void testBug713()
      throws SQLException, InterruptedException
   {
      dropProcedure( "sp_bug713" );

      Statement st = con.createStatement();
      st.execute( "create procedure sp_bug713 @data varchar as select 713" );
      st.close();

      CallableStatement sta = con.prepareCall( "{call sp_bug713('Bug713')}" );
      ResultSet         res = sta.executeQuery();

      assertTrue( res.next() );
      assertEquals( 713, res.getInt( 1 ) );

      res.close();
      sta.close();
   }

   /**
    * Test comment processing, bug #634 (and #676).
    */
   public void testCommentProcessing()
      throws SQLException
   {
      dropProcedure( "sp_bug634" );

      Statement st = con.createStatement();
      st.executeUpdate( "create procedure sp_bug634 @data1 int, @data2 int as select @data1 + @data2" );
      st.close();

      String[] variants = new String[]
      {
         "{?=call sp_bug634(?, ?)}",

         "/*/ comment '\"?@[*-} /**/*/?=call sp_bug634(?, ?)",
         "?/*/ comment '\"?@[*-} /**/*/=call sp_bug634(?, ?)",
         "?/*/ comment '\"?@[*-} /**/*/=call sp_bug634(?, ?)",
         "?=/*/ comment '\"?@[*-} /**/*/call sp_bug634(?, ?)",
         "?=call /*/ comment '\"?@[*-} /**/*/sp_bug634(?, ?)",
         "?=call sp_bug634/*/ comment '\"?@[*-} /**/*/(?, ?)",
         "?=call sp_bug634(/*/ comment '\"?@[*-} /**/*/?, ?)",
         "?=call sp_bug634(?/*/ comment '\"?@[*-} /**/*/, ?)",
         "?=call sp_bug634(?,/*/ comment '\"?@[*-} /**/*/ ?)",
         "?=call sp_bug634(?, ?/*/ comment '\"?@[*-} /**/*/)",
         "?=call sp_bug634(?, ?)/*/ comment '\"?@[*-} /**/*/",
         "?=call sp_bug634(?, ?)/*/ comment '\"?@[*-} /**/*/",
         "?=call sp_bug634(?, ?) -- comment '\"?@[*-",
         "?=call -- comment '\"?@[*-}\n sp_bug634(?, ?)",
         "?=call sp_bug634(-- comment '\"?@[*-}\n ?, ?)",

         "/*/ comment '\"?@[*-} /**/*/{?=call sp_bug634(?, ?)}",
         "{/*/ comment '\"?@[*-} /**/*/?=call sp_bug634(?, ?)}",
         "{?/*/ comment '\"?@[*-} /**/*/=call sp_bug634(?, ?)}",
         "{?=/*/ comment '\"?@[*-} /**/*/call sp_bug634(?, ?)}",
         "{?=call /*/ comment '\"?@[*-} /**/*/sp_bug634(?, ?)}",
         "{?=call sp_bug634/*/ comment '\"?@[*-} /**/*/(?, ?)}",
         "{?=call sp_bug634(/*/ comment '\"?@[*-} /**/*/?, ?)}",
         "{?=call sp_bug634(?/*/ comment '\"?@[*-} /**/*/, ?)}",
         "{?=call sp_bug634(?,/*/ comment '\"?@[*-} /**/*/ ?)}",
         "{?=call sp_bug634(?, ?/*/ comment '\"?@[*-} /**/*/)}",
         "{?=call sp_bug634(?, ?)/*/ comment '\"?@[*-} /**/*/}",
         "{?=call sp_bug634(?, ?)}/*/ comment '\"?@[*-} /**/*/",
         "{?=call sp_bug634(?, ?)} -- comment '\"?@[*-}",
         "{?=call -- comment '\"?@[*-}\n sp_bug634(?, ?)}",
         "{?=call sp_bug634(-- comment '\"?@[*-}\n ?, ?)}"
      };

      for( int i = 0; i < variants.length;  i ++ )
      {
         CallableStatement cst = null;
         ResultSet         res = null;

         try
         {
            cst = con.prepareCall( variants[i] );
            cst.registerOutParameter( 1, Types.INTEGER );
            cst.setInt( 2, i );
            cst.setInt( 3, i );
            res = cst.executeQuery();

            assertTrue  ( res.next()             );
            assertEquals( 2 * i, res.getInt( 1 ) );
            assertFalse ( res.next()             );
         }
         catch( SQLException e )
         {
            AssertionFailedError error = new AssertionFailedError( "variant \"" + variants[i] + "\" failed: " + e.getMessage() );
            error.initCause( e );
            throw error;
         }
         finally
         {
            if( res != null ) res.close();
            if( cst != null ) cst.close();
         }
      }
   }

    public void testCallableStatement() throws Exception {
        CallableStatement cstmt = con.prepareCall("{call sp_who}");

        cstmt.close();
    }

    public void testCallableStatement1() throws Exception {
        CallableStatement cstmt = con.prepareCall("sp_who");

        ResultSet rs = cstmt.executeQuery();
        dump(rs,SILENT);

        rs.close();
        cstmt.close();
    }

    public void testCallableStatementCall1() throws Exception {
        CallableStatement cstmt = con.prepareCall("{call sp_who}");

        ResultSet rs = cstmt.executeQuery();
        dump( rs,SILENT );

        rs.close();
        cstmt.close();
    }

    public void testCallableStatementCall2() throws Exception {
        CallableStatement cstmt = con.prepareCall("{CALL sp_who}");

        ResultSet rs = cstmt.executeQuery();
        dump( rs,SILENT );

        rs.close();
        cstmt.close();
    }

    public void testCallableStatementCall3() throws Exception {
        CallableStatement cstmt = con.prepareCall("{cAlL sp_who}");

        ResultSet rs = cstmt.executeQuery();
        dump( rs,SILENT );

        rs.close();
        cstmt.close();
    }

   /**
    * Test for bug [974801] stored procedure error in Northwind
    */
   public void testCallableStatementCall4() throws Exception
   {
      dropProcedure( "test space" );

      Statement stmt;

      stmt = con.createStatement();
      stmt.execute( "create procedure \"test space\" as SELECT COUNT(*) FROM sysobjects" );
      stmt.close();

      CallableStatement cstmt = con.prepareCall( "{call \"test space\"}" );

      ResultSet rs = cstmt.executeQuery();
      dump( rs, SILENT );

      rs.close();
      cstmt.close();
   }

    public void testCallableStatementExec1() throws Exception {
        CallableStatement cstmt = con.prepareCall("exec sp_who");

        ResultSet rs = cstmt.executeQuery();
        dump( rs,SILENT );

        rs.close();
        cstmt.close();
    }

    public void testCallableStatementExec2() throws Exception {
        CallableStatement cstmt = con.prepareCall("EXEC sp_who");

        ResultSet rs = cstmt.executeQuery();
        dump( rs,SILENT );

        rs.close();
        cstmt.close();
    }

    public void testCallableStatementExec3() throws Exception {
        CallableStatement cstmt = con.prepareCall("execute sp_who");

        ResultSet rs = cstmt.executeQuery();
        dump( rs,SILENT );

        rs.close();
        cstmt.close();
    }

    public void testCallableStatementExec4() throws Exception {
        CallableStatement cstmt = con.prepareCall("EXECUTE sp_who");

        ResultSet rs = cstmt.executeQuery();
        dump( rs,SILENT );

        rs.close();
        cstmt.close();
    }

    public void testCallableStatementExec5() throws Exception {
        CallableStatement cstmt = con.prepareCall("eXeC sp_who");

        ResultSet rs = cstmt.executeQuery();
        dump( rs,SILENT );

        rs.close();
        cstmt.close();
    }

    public void testCallableStatementExec6() throws Exception {
        CallableStatement cstmt = con.prepareCall("ExEcUtE sp_who");

        ResultSet rs = cstmt.executeQuery();
        dump( rs,SILENT );

        rs.close();
        cstmt.close();
    }

    public void testCallableStatementExec7() throws Exception {
        CallableStatement cstmt = con.prepareCall("execute \"master\"..sp_who");

        ResultSet rs = cstmt.executeQuery();
        dump( rs,SILENT );

        rs.close();
        cstmt.close();
    }

   public void testCallableStatementExec8()
      throws Exception
   {
      dropProcedure( "test" );

      Statement stmt;

      stmt = con.createStatement();
      stmt.execute( "create procedure test as SELECT COUNT(*) FROM sysobjects" );
      stmt.close();

      CallableStatement cstmt = con.prepareCall( "execute test" );

      ResultSet rs = cstmt.executeQuery();
      dump( rs, SILENT );

      rs.close();
      cstmt.close();
   }

    /**
     * Test for bug [978175] 0.8: Stored Procedure call doesn't work anymore
     */
    public void testCallableStatementExec9() throws Exception {
        CallableStatement cstmt = con.prepareCall("{call sp_who}");

        assertTrue(cstmt.execute());

        ResultSet rs = cstmt.getResultSet();

        if (rs == null) {
            fail("Null ResultSet returned");
        } else {
            dump( rs,SILENT );
            rs.close();
        }

        cstmt.close();
    }

    public void testCallableStatementParsing1() throws Exception {
        dropProcedure( "sp_csp1" );
        dropTable( "csp1" );

        String data = "New {order} plus {1} more";
        Statement stmt = con.createStatement();

        stmt.execute("CREATE TABLE csp1 (data VARCHAR(32))");
        stmt.close();

        stmt = con.createStatement();
        stmt.execute("create procedure sp_csp1 @data VARCHAR(32) as INSERT INTO csp1 (data) VALUES(@data)");
        stmt.close();

        CallableStatement cstmt = con.prepareCall("{call sp_csp1(?)}");

        cstmt.setString(1, data);
        cstmt.execute();
        cstmt.close();

        stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT data FROM csp1");

        assertTrue(rs.next());

        assertTrue(data.equals(rs.getString(1)));

        assertTrue(!rs.next());
        rs.close();
        stmt.close();
    }

   /**
    * Test for bug [938632] String index out of bounds error in 0.8rc1.
    */
   public void testCallableStatementParsing2() throws Exception
   {
      dropProcedure( "load_smtp_in_1gr_ls804192" );

      Statement stmt = con.createStatement();

      stmt.execute( "create procedure load_smtp_in_1gr_ls804192 as SELECT name FROM sysobjects" );
      stmt.close();

      CallableStatement cstmt = con.prepareCall( "{?=call load_smtp_in_1gr_ls804192}" );
      cstmt.registerOutParameter( 1, java.sql.Types.INTEGER ); // MJH 01/05/04
      cstmt.execute();
      cstmt.close();
   }

    /**
     * Test for bug [1006845] Stored procedure with 18 parameters.
     */
    public void testCallableStatementParsing3() throws Exception {
        CallableStatement cstmt = con.prepareCall("{Call Test(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)}");
        cstmt.close();
    }

    /**
     * Test for incorrect exception thrown/no exception thrown when invalid
     * call escape is used.
     * <p/>
     * See https://sourceforge.net/forum/forum.php?thread_id=1144619&forum_id=104389
     * for more detail.
     */
    public void testCallableStatementParsing4() throws SQLException {
        try {
            con.prepareCall("{call ? = sp_create_employee (?, ?, ?, ?, ?, ?)}");
            fail("Was expecting an invalid escape sequence error");
        } catch (SQLException ex) {
            assertEquals("22025", ex.getSQLState());
        }
    }

    /**
     * Test for bug [1052942] Error processing JDBC call escape. (A blank
     * before the final <code>}</code> causes the parser to fail).
     */
    public void testCallableStatementParsing5() throws Exception {
        CallableStatement cstmt = con.prepareCall(" { Call Test(?,?) } ");
        cstmt.close();
    }

    /**
     * Test for incorrect exception thrown/no exception thrown when invalid
     * call escape is used.
     * <p/>
     * A message containing the correct missing terminator should be generated.
     */
    public void testCallableStatementParsing6() throws SQLException {
        try {
            con.prepareCall("{call sp_test(?, ?)");
            fail("Was expecting an invalid escape error");
        } catch (SQLException ex) {
            assertEquals("22025", ex.getSQLState());
            assertTrue(ex.getMessage().indexOf('}') != -1);
        }
    }

    /**
     * Test for incorrect exception thrown/no exception thrown when invalid
     * call escape is used.
     * <p/>
     * A message containing the correct missing terminator should be generated.
     */
    public void testCallableStatementParsing7() throws SQLException {
        try {
            con.prepareCall("{call sp_test(?, ?}");
            fail("Was expecting an invalid escape error");
        } catch (SQLException ex) {
            assertEquals("22025", ex.getSQLState());
            assertTrue(ex.getMessage().indexOf(')') != -1);
        }
    }

   /**
    * Test for reature request [956800] setNull(): Not implemented.
    */
   public void testCallableSetNull1()
      throws Exception
   {
      dropProcedure( "procCallableSetNull1" );
      dropTable( "callablesetnull1" );

      Statement stmt = con.createStatement();
      stmt.execute( "CREATE TABLE callablesetnull1 (data CHAR(1) NULL)" );
      stmt.close();

      stmt = con.createStatement();
      stmt.execute( "create procedure procCallableSetNull1 @data char(1) as INSERT INTO callablesetnull1 (data) VALUES (@data)" );
      stmt.close();

      CallableStatement cstmt = con.prepareCall( "{call procCallableSetNull1(?)}" );
      // Test CallableStatement.setNull(int,Types.NULL)
      cstmt.setNull( 1, Types.NULL );
      cstmt.execute();
      cstmt.close();

      stmt = con.createStatement();
      ResultSet rs = stmt.executeQuery( "SELECT data FROM callablesetnull1" );

      assertTrue( rs.next() );

      // Test ResultSet.getString()
      assertNull( rs.getString( 1 ) );
      assertTrue( rs.wasNull() );

      assertTrue( !rs.next() );
      stmt.close();
      rs.close();
   }

   /**
    * Test for bug [974284] retval on callable statement isn't handled correctly
    */
   public void testCallableRegisterOutParameter1() throws Exception
   {
      dropProcedure( "rop1" );
      Statement stmt = con.createStatement();
      stmt.execute( "create procedure rop1 @a varchar(1), @b varchar(1) as begin return 1 end" );
      stmt.close();

      CallableStatement cstmt = con.prepareCall( "{? = call rop1(?, ?)}" );

      cstmt.registerOutParameter( 1, Types.INTEGER );
      cstmt.setString( 2, "a" );
      cstmt.setString( 3, "b" );
      cstmt.execute();

      assertEquals( 1, cstmt.getInt( 1 ) );
      assertEquals( "1", cstmt.getString( 1 ) );

      cstmt.close();
   }

   /**
    * Test for bug [994888] Callable statement and Float output parameter
    */
   public void testCallableRegisterOutParameter2()
      throws Exception
   {
      dropProcedure( "rop2" );

      Statement stmt = con.createStatement();
      stmt.execute( "create procedure rop2 @data float OUTPUT as begin set @data = 1.1 end" );
      stmt.close();

      CallableStatement cstmt = con.prepareCall( "{call rop2(?)}" );

      cstmt.registerOutParameter( 1, Types.FLOAT );
      cstmt.execute();

      assertTrue( cstmt.getFloat( 1 ) == 1.1f );
      cstmt.close();
   }

   /**
    * Test for bug [994988] Network error when null is returned via int output
    * parm
    */
   public void testCallableRegisterOutParameter3()
      throws Exception
   {
      dropProcedure( "rop3" );

      Statement stmt = con.createStatement();
      stmt.execute( "create procedure rop3 @data int OUTPUT as begin set @data = null end" );
      stmt.close();

      CallableStatement cstmt = con.prepareCall( "{call rop3(?)}" );

      cstmt.registerOutParameter( 1, Types.INTEGER );
      cstmt.execute();

      cstmt.getInt( 1 );
      assertTrue( cstmt.wasNull() );
      cstmt.close();
   }

   /**
    * Test for bug [983432] Prepared call doesn't work with jTDS 0.8
    */
   public void testCallableRegisterOutParameter4()
      throws Exception
   {
      // cleanup remains from last run
      dropProcedure( "rop4" );
      dropType( "T_INTEGER" );

      CallableStatement cstmt = con.prepareCall( "{call sp_addtype T_INTEGER, int, 'NULL'}" );
      Statement stmt = con.createStatement();

      try
      {
         cstmt.execute();
         cstmt.close();

         stmt.execute( "create procedure rop4 @data T_INTEGER OUTPUT as\r\n " + "begin\r\n" + "set @data = 1\r\n" + "end" );
         stmt.close();

         cstmt = con.prepareCall( "{call rop4(?)}" );

         cstmt.registerOutParameter( 1, Types.VARCHAR );
         cstmt.execute();

         assertEquals( cstmt.getInt( 1 ), 1 );
         assertTrue( !cstmt.wasNull() );
         cstmt.close();

         cstmt = con.prepareCall( "rop4 ?" );

         cstmt.registerOutParameter( 1, Types.VARCHAR );
         cstmt.execute();

         assertEquals( cstmt.getInt( 1 ), 1 );
         assertTrue( !cstmt.wasNull() );
         cstmt.close();
      }
      finally
      {
         // cleanup
         dropProcedure( "rop4" );
         dropType( "T_INTEGER" );
      }
   }

   /**
    * Test for bug [946171] null boolean in CallableStatement bug
    */
   public void testCallableRegisterOutParameter5()
      throws Exception
   {
      dropProcedure( "rop1" );

      Statement stmt = con.createStatement();
      stmt.execute( "create procedure rop1 @bool bit, @whatever int OUTPUT as begin set @whatever = 1 end" );
      stmt.close();

      CallableStatement cstmt = con.prepareCall( "{call rop1(?,?)}" );

      cstmt.setNull( 1, Types.BOOLEAN );
      cstmt.registerOutParameter( 2, Types.INTEGER );
      cstmt.execute();

      assertTrue( cstmt.getInt( 2 ) == 1 );
      cstmt.close();
   }

   /**
    * Test for bug [992715] wasnull() always returns false
    */
   public void testCallableRegisterOutParameter6()
      throws Exception
   {
      dropProcedure( "rop2" );

      Statement stmt = con.createStatement();
      stmt.execute( "create procedure rop2 @bool bit, @whatever varchar(1) OUTPUT as\r\n " + "begin\r\n" + "set @whatever = null\r\n" + "end" );
      stmt.close();

      CallableStatement cstmt = con.prepareCall( "{call rop2(?,?)}" );

      cstmt.setNull( 1, Types.BOOLEAN );
      cstmt.registerOutParameter( 2, Types.VARCHAR );
      cstmt.execute();

      assertTrue( cstmt.getString( 2 ) == null );
      assertTrue( cstmt.wasNull() );
      cstmt.close();
   }

   /**
    * Test for bug [991640] java.sql.Date error and RAISERROR problem
    */
   public void testCallableError1() throws Exception
   {
      dropProcedure( "ce1" );

      String text = "test message";

      Statement stmt = con.createStatement();
      stmt.execute( "create procedure ce1 as begin RAISERROR('" + text + "', 16, 1 ) end" );
      stmt.close();

      CallableStatement cstmt = con.prepareCall( "{call ce1}" );

      try
      {
         cstmt.execute();
         assertTrue( false );
      }
      catch( SQLException e )
      {
         assertTrue( e.getMessage().equals( text ) );
      }

      cstmt.close();
   }

   /**
    * Test named parameters.
    */
   public void testNamedParameters0001()
      throws Exception
   {
      dropProcedure( "sp_csn1" );
      dropTable( "csn1" );

      final String data = "New {order} plus {1} more";
      final String outData = "test";

      Statement stmt = con.createStatement();

      stmt.execute( "CREATE TABLE csn1 ( data VARCHAR(32) )" );
      stmt.execute( "create procedure sp_csn1 @data VARCHAR(32) OUT as INSERT INTO csn1 (data) VALUES(@data) SET @data = '" + outData + "'" + "RETURN 13" );

      CallableStatement cstmt = con.prepareCall( "{?=call sp_csn1(?)}" );

      cstmt.registerOutParameter( "@return_status", Types.INTEGER );
      cstmt.setString( "@data", data );
      cstmt.registerOutParameter( "@data", Types.VARCHAR );
      assertEquals( 1, cstmt.executeUpdate() );
      assertFalse( cstmt.getMoreResults() );
      assertEquals( -1, cstmt.getUpdateCount() );
      assertEquals( outData, cstmt.getString( "@data" ) );
      cstmt.close();

      ResultSet rs = stmt.executeQuery( "SELECT data FROM csn1" );

      assertTrue( rs.next() );
      assertEquals( data, rs.getString( 1 ) );
      assertTrue( ! rs.next() );

      rs.close();
      stmt.close();
   }

   /**
    * Test named parameters.
    */
   public void testNamedParameters0002()
      throws Exception
   {
      dropProcedure( "spInsert" );
      dropTable( "np0002" );

      final String  A_DEFAULT = "XYZ";
      final Integer B_DEFAULT = 123;
      final Integer C_DEFAULT = 321;

      Statement stmt = con.createStatement();
      stmt.execute( "create table np0002( A varchar(10), B int, C int, D int primary key )" );
      stmt.execute( "create procedure spInsert @A_VAL varchar(10) = " + A_DEFAULT + " out, @B_VAL int = " + B_DEFAULT + ", @C_VAL int = " + C_DEFAULT + " out, @D_VAL int as INSERT INTO np0002 VALUES( @A_VAL, @B_VAL, @C_VAL, @D_VAL ) set @A_VAL = 'RET' set @C_VAL = @B_VAL + @C_VAL return @B_VAL" );

      CallableStatement cstmt = con.prepareCall( "{?=call spInsert(?, ?, ?, ?)}" );

      cstmt.registerOutParameter( 1, Types.INTEGER );
      cstmt.registerOutParameter( "A_VAL", Types.VARCHAR );
      cstmt.registerOutParameter( "C_VAL", Types.INTEGER );

      cstmt.setObject( "A_VAL", A_DEFAULT );
      cstmt.setObject( "B_VAL", B_DEFAULT );
      cstmt.setObject( "C_VAL", C_DEFAULT );
      cstmt.setInt   ( "D_VAL", 0         );

      assertEquals( 1, cstmt.executeUpdate() );
      assertFalse( cstmt.getMoreResults() );
      assertEquals( -1, cstmt.getUpdateCount() );

      assertEquals( B_DEFAULT, cstmt.getObject( 1 ) );
      assertEquals( "RET", cstmt.getObject( "A_VAL" ) );
      assertEquals( B_DEFAULT + C_DEFAULT, cstmt.getObject( "C_VAL" ) );
      cstmt.close();

      ResultSet rs = stmt.executeQuery( "select A, B, C from np0002 where D = 0" );

      assertTrue( rs.next() );
      assertEquals( A_DEFAULT, rs.getObject( "A" ) );
      assertEquals( B_DEFAULT, rs.getObject( "B" ) );
      assertEquals( C_DEFAULT, rs.getObject( "C" ) );
      assertTrue( ! rs.next() );

      rs.close();

      // and once again without setting all parameters

      cstmt = con.prepareCall( "{?=call spInsert(?,?)}" );

      cstmt.registerOutParameter( 1, Types.INTEGER );

      cstmt.setInt( "B_VAL", 9876 );
      cstmt.setInt( "D_VAL", 1    );

      assertEquals( 1, cstmt.executeUpdate() );
      assertFalse( cstmt.getMoreResults() );
      assertEquals( -1, cstmt.getUpdateCount() );

      assertEquals( 9876, cstmt.getObject( 1 ) );
      cstmt.close();

      rs = stmt.executeQuery( "select A, B, C from np0002 where D = 1" );

      assertTrue( rs.next() );
      assertEquals( A_DEFAULT, rs.getObject( "A" ) );
      assertEquals( 9876     , rs.getObject( "B" ) );
      assertEquals( C_DEFAULT, rs.getObject( "C" ) );
      assertTrue( ! rs.next() );

      rs.close();
      stmt.close();
   }

   /**
    * Test that procedure outputs are available immediately for procedures that
    * do not return ResultSets (i.e that update counts are cached).
    */
   public void testProcessUpdateCounts1()
      throws SQLException
   {
      dropProcedure( "procTestProcessUpdateCounts1" );
      dropTable( "testProcessUpdateCounts1" );

      Statement stmt = con.createStatement();
      assertFalse( stmt.execute( "CREATE TABLE testProcessUpdateCounts1 (val INT)" ) );
      assertFalse( stmt.execute( "CREATE PROCEDURE procTestProcessUpdateCounts1 @res INT OUT AS INSERT INTO testProcessUpdateCounts1 VALUES (1) UPDATE testProcessUpdateCounts1 SET val = 2 INSERT INTO testProcessUpdateCounts1 VALUES (1) UPDATE testProcessUpdateCounts1 SET val = 3 SET @res = 13 RETURN 14" ) );
      stmt.close();

      CallableStatement cstmt = con.prepareCall( "{?=call procTestProcessUpdateCounts1(?)}" );
      cstmt.registerOutParameter( 1, Types.INTEGER );
      cstmt.registerOutParameter( 2, Types.INTEGER );

      assertFalse( cstmt.execute() );
      assertEquals( 14, cstmt.getInt( 1 ) );
      assertEquals( 13, cstmt.getInt( 2 ) );

      assertEquals( 1, cstmt.getUpdateCount() ); // INSERT

      assertFalse( cstmt.getMoreResults() );
      assertEquals( 1, cstmt.getUpdateCount() ); // UPDATE

      assertFalse( cstmt.getMoreResults() );
      assertEquals( 1, cstmt.getUpdateCount() ); // INSERT

      assertFalse( cstmt.getMoreResults() );
      assertEquals( 2, cstmt.getUpdateCount() ); // UPDATE

      assertFalse( cstmt.getMoreResults() );
      assertEquals( -1, cstmt.getUpdateCount() );

      cstmt.close();
   }

   /**
    * Test that procedure outputs are available immediately after processing the
    * last ResultSet returned by the procedure (i.e that update counts are
    * cached).
    */
   public void testProcessUpdateCounts2()
      throws SQLException
   {
      dropProcedure( "procTestProcessUpdateCounts2" );
      dropTable( "testProcessUpdateCounts2" );

      Statement stmt = con.createStatement();
      assertFalse( stmt.execute( "CREATE TABLE testProcessUpdateCounts2 (val INT)" ) );
      assertFalse( stmt.execute( "CREATE PROCEDURE procTestProcessUpdateCounts2 @res INT OUT AS INSERT INTO testProcessUpdateCounts2 VALUES (1) UPDATE testProcessUpdateCounts2 SET val = 2 SELECT * FROM testProcessUpdateCounts2 INSERT INTO testProcessUpdateCounts2 VALUES (1) UPDATE testProcessUpdateCounts2 SET val = 3 SET @res = 13 RETURN 14" ) );
      stmt.close();

      CallableStatement cstmt = con.prepareCall( "{?=call procTestProcessUpdateCounts2(?)}" );
      cstmt.registerOutParameter( 1, Types.INTEGER );
      cstmt.registerOutParameter( 2, Types.INTEGER );

      assertFalse( cstmt.execute() );
      try
      {
         assertEquals( 14, cstmt.getInt( 1 ) );
         assertEquals( 13, cstmt.getInt( 2 ) );
         // Don't fail the test if we got here. Another driver or a future
         // version could cache all the results and obtain the output
         // parameter values from the beginning.
      }
      catch( SQLException ex )
      {
         assertEquals( "HY010", ex.getSQLState() );
         assertTrue( ex.getMessage().indexOf( "getMoreResults()" ) >= 0 );
      }

      assertEquals( 1, cstmt.getUpdateCount() ); // INSERT

      assertFalse( cstmt.getMoreResults() );
      assertEquals( 1, cstmt.getUpdateCount() ); // UPDATE

      assertTrue( cstmt.getMoreResults() ); // SELECT

      assertFalse( cstmt.getMoreResults() );
      assertEquals( 14, cstmt.getInt( 1 ) );
      assertEquals( 13, cstmt.getInt( 2 ) );
      assertEquals( 1, cstmt.getUpdateCount() ); // INSERT

      assertFalse( cstmt.getMoreResults() );
      assertEquals( 2, cstmt.getUpdateCount() ); // UPDATE

      assertFalse( cstmt.getMoreResults() );
      assertEquals( -1, cstmt.getUpdateCount() );

      cstmt.close();
   }

   /**
    * Test that procedure outputs are available immediately after processing the
    * last ResultSet returned by the procedure (i.e that update counts are
    * cached) even if getMoreResults() is not called.
    */
   public void testProcessUpdateCounts3()
      throws SQLException
   {
      dropProcedure( "procTestProcessUpdateCounts3" );
      dropTable( "testProcessUpdateCounts3" );

      Statement stmt = con.createStatement();
      assertFalse( stmt.execute( "CREATE TABLE testProcessUpdateCounts3 (val INT)" ) );
      assertFalse( stmt.execute( "CREATE PROCEDURE procTestProcessUpdateCounts3 @res INT OUT AS INSERT INTO testProcessUpdateCounts3 VALUES (1) UPDATE testProcessUpdateCounts3 SET val = 2 SELECT * FROM testProcessUpdateCounts3 INSERT INTO testProcessUpdateCounts3 VALUES (1) UPDATE testProcessUpdateCounts3 SET val = 3 SET @res = 13 RETURN 14" ) );
      stmt.close();

      CallableStatement cstmt = con.prepareCall( "{?=call procTestProcessUpdateCounts3(?)}" );
      cstmt.registerOutParameter( 1, Types.INTEGER );
      cstmt.registerOutParameter( 2, Types.INTEGER );

      assertFalse( cstmt.execute() );
      try
      {
         assertEquals( 14, cstmt.getInt( 1 ) );
         assertEquals( 13, cstmt.getInt( 2 ) );
         // Don't fail the test if we got here. Another driver or a future
         // version could cache all the results and obtain the output
         // parameter values from the beginning.
      }
      catch( SQLException ex )
      {
         assertEquals( "HY010", ex.getSQLState() );
         assertTrue( ex.getMessage().indexOf( "getMoreResults()" ) >= 0 );
      }

      assertEquals( 1, cstmt.getUpdateCount() ); // INSERT

      assertFalse( cstmt.getMoreResults() );
      assertEquals( 1, cstmt.getUpdateCount() ); // UPDATE

      assertTrue( cstmt.getMoreResults() ); // SELECT
      ResultSet rs = cstmt.getResultSet();
      assertNotNull( rs );
      // Close the ResultSet; this should cache the following update counts
      rs.close();

      assertEquals( 14, cstmt.getInt( 1 ) );
      assertEquals( 13, cstmt.getInt( 2 ) );

      assertFalse( cstmt.getMoreResults() );
      assertEquals( 1, cstmt.getUpdateCount() ); // INSERT

      assertFalse( cstmt.getMoreResults() );
      assertEquals( 2, cstmt.getUpdateCount() ); // UPDATE

      assertFalse( cstmt.getMoreResults() );
      assertEquals( -1, cstmt.getUpdateCount() );

      cstmt.close();
   }

   /**
    * Test that procedure outputs are available immediately after processing the
    * last ResultSet returned by the procedure (i.e that update counts are
    * cached) even if getMoreResults() and ResultSet.close() are not called.
    */
   public void testProcessUpdateCounts4()
      throws SQLException
   {
      dropProcedure( "procTestProcessUpdateCounts4" );
      dropTable( "testProcessUpdateCounts4" );

      Statement stmt = con.createStatement();
      assertFalse( stmt.execute( "CREATE TABLE testProcessUpdateCounts4 (val INT)" ) );
      assertFalse( stmt.execute( "CREATE PROCEDURE procTestProcessUpdateCounts4 @res INT OUT AS INSERT INTO testProcessUpdateCounts4 VALUES (1) UPDATE testProcessUpdateCounts4 SET val = 2 SELECT * FROM testProcessUpdateCounts4 INSERT INTO testProcessUpdateCounts4 VALUES (1) UPDATE testProcessUpdateCounts4 SET val = 3 SET @res = 13" + " RETURN 14" ) );
      stmt.close();

      CallableStatement cstmt = con.prepareCall( "{?=call procTestProcessUpdateCounts4(?)}" );
      cstmt.registerOutParameter( 1, Types.INTEGER );
      cstmt.registerOutParameter( 2, Types.INTEGER );

      assertFalse( cstmt.execute() );
      try
      {
         assertEquals( 14, cstmt.getInt( 1 ) );
         assertEquals( 13, cstmt.getInt( 2 ) );
         // Don't fail the test if we got here. Another driver or a future
         // version could cache all the results and obtain the output
         // parameter values from the beginning.
      }
      catch( SQLException ex )
      {
         assertEquals( "HY010", ex.getSQLState() );
         assertTrue( ex.getMessage().indexOf( "getMoreResults()" ) >= 0 );
      }

      assertEquals( 1, cstmt.getUpdateCount() ); // INSERT

      assertFalse( cstmt.getMoreResults() );
      assertEquals( 1, cstmt.getUpdateCount() ); // UPDATE

      assertTrue( cstmt.getMoreResults() ); // SELECT
      ResultSet rs = cstmt.getResultSet();
      assertNotNull( rs );
      // Process all rows; this should cache the following update counts
      assertTrue( rs.next() );
      assertFalse( rs.next() );

      assertEquals( 14, cstmt.getInt( 1 ) );
      assertEquals( 13, cstmt.getInt( 2 ) );

      // Only close the ResultSet now
      rs.close();

      assertFalse( cstmt.getMoreResults() );
      assertEquals( 1, cstmt.getUpdateCount() ); // INSERT

      assertFalse( cstmt.getMoreResults() );
      assertEquals( 2, cstmt.getUpdateCount() ); // UPDATE

      assertFalse( cstmt.getMoreResults() );
      assertEquals( -1, cstmt.getUpdateCount() );

      cstmt.close();
   }

    /**
     * Test for bug [ 1062671 ] SQLParser unable to parse CONVERT(char,{ts ?},102)
     */
    public void testTsEscape() throws Exception {
        Timestamp ts = Timestamp.valueOf("2004-01-01 23:56:56");
        Statement stmt = con.createStatement();
        assertFalse(stmt.execute("CREATE TABLE #testTsEscape (val DATETIME)"));
        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #testTsEscape VALUES({ts ?})");
        pstmt.setTimestamp(1, ts);
        assertEquals(1, pstmt.executeUpdate());
        ResultSet rs = stmt.executeQuery("SELECT * FROM #testTsEscape");
        assertTrue(rs.next());
        assertEquals(ts, rs.getTimestamp(1));
    }

   /**
    * Test for separation of IN and INOUT/OUT parameter values
    */
   public void testInOutParameters()
      throws Exception
   {
      dropProcedure( "testInOut" );

      Statement stmt = con.createStatement();
      stmt.execute( "CREATE PROC testInOut @in int, @out int output as SELECT @out = @out + @in" );
      CallableStatement cstmt = con.prepareCall( "{ call testInOut ( ?,? ) }" );
      cstmt.setInt( 1, 1 );
      cstmt.registerOutParameter( 2, Types.INTEGER );
      cstmt.setInt( 2, 2 );
      cstmt.execute();
      assertEquals( 3, cstmt.getInt( 2 ) );
      cstmt.execute();
      assertEquals( 3, cstmt.getInt( 2 ) );
   }

   /**
    * Test that procedure names containing semicolons are parsed correctly.
    */
   public void testSemicolonProcedures()
      throws Exception
   {
      dropProcedure( "testInOut" );

      Statement stmt = con.createStatement();
      stmt.execute( "CREATE PROC testInOut @in int, @out int output as SELECT @out = @out + @in" );
      CallableStatement cstmt = con.prepareCall( "{call testInOut;1(?,?)}" );
      cstmt.setInt( 1, 1 );
      cstmt.registerOutParameter( 2, Types.INTEGER );
      cstmt.setInt( 2, 2 );
      cstmt.execute();
      assertEquals( 3, cstmt.getInt( 2 ) );
      cstmt.execute();
      assertEquals( 3, cstmt.getInt( 2 ) );
   }

   /**
    * Test that procedure calls with both literal parameters and parameterr
    * markers are executed correctly (bug [1078927] Callable statement fails).
    */
   public void testNonRpcProc1()
      throws Exception
   {
      dropProcedure( "testsp1" );

      Statement stmt = con.createStatement();
      stmt.execute( "create proc testsp1 @p1 int, @p2 int out as set @p2 = @p1" );
      stmt.close();

      CallableStatement cstmt = con.prepareCall( "{call testsp1(100, ?)}" );
      cstmt.setInt( 1, 1 );
      cstmt.execute();
      cstmt.close();
   }

   /**
    * Test that procedure calls with both literal parameters and parameterr
    * markers are executed correctly (bug [1078927] Callable statement fails).
    */
   public void testNonRpcProc2()
      throws Exception
   {
      dropProcedure( "testsp2" );

      Statement stmt = con.createStatement();
      stmt.execute( "create proc testsp2 @p1 int, @p2 int as return 99" );
      stmt.close();

      CallableStatement cstmt = con.prepareCall( "{?=call testsp2(100, ?)}" );
      cstmt.registerOutParameter( 1, java.sql.Types.INTEGER );
      cstmt.setInt( 2, 2 );
      cstmt.execute();
      assertEquals( 99, cstmt.getInt( 1 ) );
      cstmt.close();
   }

   /**
    * Test for bug [1152329] Spurious output params assigned (TIMESTMP).
    * <p/>
    * If a stored procedure execute WRITETEXT or UPDATETEXT commands, spurious
    * output parameter data is returned to the client. This additional data can
    * be confused with the real output parameter data leading to an output
    * string parameter returning the text ?TIMESTMP? on SQL Server 7+ or binary
    * garbage on other servers.
    */
   public void testWritetext()
       throws Exception
   {
      dropProcedure( "testWritetext" );

      Statement stmt = con.createStatement();
      stmt.execute( "create proc testWritetext @p1 varchar(20) output as begin create table #test (id int, txt text) insert into #test (id, txt) values(1, '') declare @ptr binary(16) select @ptr = (select textptr(txt) from #test where id = 1) writetext #test.txt @ptr 'This is a test' select @p1 = 'done' end" );
      stmt.close();

      CallableStatement cstmt = con.prepareCall( "{call testWritetext(?)}" );
      cstmt.registerOutParameter( 1, Types.VARCHAR );
      cstmt.execute();
      assertEquals( "done", cstmt.getString( 1 ) );
      cstmt.close();
   }

   /**
    * Test for bug [1047208] SQLException chaining not implemented correctly:
    * checks that all errors are returned and that output variables are also
    * returned.
    */
   public void testErrorOutputParams()
      throws Exception
   {
      dropProcedure( "error_proc" );

      Statement stmt = con.createStatement();
      stmt.execute( "CREATE PROC error_proc @p1 int out AS RAISERROR ('TEST EXCEPTION', 15, 1) SELECT @P1=100 CREATE TABLE #DUMMY (id int) INSERT INTO #DUMMY VALUES(1) INSERT INTO #DUMMY VALUES(1)" );
      stmt.close();

      CallableStatement cstmt = con.prepareCall( "{call error_proc(?)}" );
      cstmt.registerOutParameter( 1, Types.INTEGER );
      try
      {
         cstmt.execute();
         fail( "Expecting exception" );
      }
      catch( SQLException e )
      {
         assertEquals( "TEST EXCEPTION", e.getMessage() );
      }
      assertEquals( 100, cstmt.getInt( 1 ) );
      cstmt.close();
   }

   /**
    * Test for bug [1236078] Procedure doesn't get called for some BigDecimal
    * values - invalid bug.
    */
   public void testBigDecimal()
      throws Exception
   {
      dropProcedure( "dec_test2" );
      dropTable( "dec_test" );

      Statement stmt = con.createStatement();
      assertEquals( 0, stmt.executeUpdate( "CREATE TABLE dec_test (ColumnVC varchar(50) NULL, ColumnDec decimal(18,4) NULL)" ) );
      assertEquals( 0, stmt.executeUpdate( "CREATE PROCEDURE dec_test2 (@inVc varchar(32), @inBd decimal(18,4)) AS begin update dec_test set columnvc = @inVc, columndec = @inBd end" ) );
      assertEquals( 1, stmt.executeUpdate( "insert dec_test (columnvc, columndec) values (null, null)" ) );
      stmt.close();

      CallableStatement cstmt = con.prepareCall( "{call dec_test2 (?,?)}" );
      cstmt.setString( 1, "D: " + new java.util.Date() );
      cstmt.setBigDecimal( 2, new BigDecimal( "2.9E+7" ) );
      assertEquals( 1, cstmt.executeUpdate() );
      cstmt.close();
   }

    /**
     * Test retrieving multiple resultsets, the return value and an additional
     * output parameter from a single procedure call.
     */
    public void testCallWithResultSet() throws Exception {
        dropProcedure( "testCallWithResultSet" );
        Statement st = con.createStatement();
        st.execute("create proc testCallWithResultSet @in varchar(16), @out varchar(32) output as" +
                   " begin" +
                   "  select 'result set' as ret" +
                   "  set @out = 'Test ' + @in " +
                   "  select 'result set 2' as ret2" +
                   "  return 1" +
                   " end");
        st.close();

        CallableStatement cstmt = con.prepareCall("{?=call testCallWithResultSet(?,?)}");
        cstmt.registerOutParameter(1, Types.INTEGER);
        cstmt.setString(2, "data");
        cstmt.registerOutParameter(3, Types.VARCHAR);
        cstmt.execute();

        // resultset 1
        ResultSet rs = cstmt.getResultSet();
        assertNotNull(rs);
        assertTrue(rs.next());
        assertEquals("result set", rs.getString(1));
        assertFalse(rs.next());
        rs.close();

        // resultset 2
        assertTrue(cstmt.getMoreResults());
        rs = cstmt.getResultSet();
        assertTrue(rs.next());
        assertEquals("result set 2", rs.getString(1));
        assertFalse(rs.next());
        rs.close();

        // return value and output parameter
        assertEquals(1, cstmt.getInt(1));
        assertEquals("Test data", cstmt.getString(3));
        cstmt.close();
    }

   /**
    *
    */
   public void testBug637()
      throws Exception
   {
      Statement stm = con.createStatement();
      stm.executeUpdate( "create table #testBug637( a int, b int )" );

      CallableStatement stmt = null;

      try
      {
         // prepareCall() should fail, this is no procedure call
         stmt = con.prepareCall( "INSERT INTO #testBug637( a, b ) VALUES( ?, ? )" );
         stmt.setInt( 1, 1 );
         // this failed prior to SVN revision 1146
         stmt.setInt( 2, 2 );

         fail();
      }
      catch( SQLException sqle )
      {
         assertEquals( "07000", sqle.getSQLState() );
      }
      finally
      {
         if( stmt != null )
         {
            stmt.close();
         }
      }
      stm.close();
   }

    /**
     * Test that output result sets, return values and output parameters are
     * correctly handled for a remote procedure call.
     * To set up this test you will a local and remote server where the remote
     * server allows logins from the local test server.
     * Install the following stored procedure on the remote server:
     *
     * create proc jtds_remote @in varchar(16), @out varchar(32) output as
     * begin
     *   select 'result set'
     *   set @out = 'Test ' + @in;
     *   return 1
     * end
     *
     * Uncomment this test and amend the remoteserver name in the prepareCall
     * statement below to be the actual name of your remote server.
     *
     * The TDS stream for this test will comprise a result set, a dummy return
     * (0x79) value and then the actual return and output parameter (0xAC) records.
     *
     * This call will fail with jtds 1.1 as the dummy return value of 0 in the
     * TDS stream will preempt the capture of the actual value 1. In addition the
     * return value will be assigned to the output parameter and the actual output
     * parameter value will be lost.
     *
     *
    public void testRemoteCallWithResultSet() throws Exception {
        CallableStatement cstmt = con.prepareCall(
                "{?=call remoteserver.database.user.jtds_remote(?,?)}");
        cstmt.registerOutParameter(1, Types.INTEGER);
        cstmt.setString(2, "data");
        cstmt.registerOutParameter(3, Types.VARCHAR);
        cstmt.execute();
        ResultSet rs = cstmt.getResultSet();
        assertNotNull(rs);
        assertTrue(rs.next());
        assertEquals("result set", rs.getString(1));
        assertFalse(rs.next());
        rs.close();
        assertEquals(1, cstmt.getInt(1));
        assertEquals("Test data", cstmt.getString(3));
        cstmt.close();
    }
    */

    public static void main(String[] args) {
        junit.textui.TestRunner.run(CallableStatementTest.class);
    }
}