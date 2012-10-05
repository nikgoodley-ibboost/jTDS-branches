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

package net.sourceforge.jtds.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * @version 1.0
 */
public class StatementTest extends TestBase {

   public StatementTest( String name )
   {
      super( name );
   }

   /**
    * Test for computed results, bug #678.
    */
   public void testComputeClause()
      throws Exception
   {
      final int VALUES = 150;

      Statement sta = con.createStatement();

      sta.executeUpdate( "create table #Bug678( X int, A varchar(10), B int, C bigint )" );

      for( int i = 0; i < VALUES; i ++ )
      {
         sta.executeUpdate( "insert into #Bug678 values( " + i % Math.max( 1, i / 20 ) + ", 'VAL" + i + "'," + ( VALUES - i ) + ", " + (long)i * Integer.MAX_VALUE + " )" );
      }

      assertTrue( sta.execute( "select * from #Bug678 order by X, A asc compute min( A ), max( A ), min( C ), max( C ), avg( B ), sum( B ), count( A ), count_big( C ) by X" ) );

      do
      {
         dump( sta.getResultSet() );
      }
      while( sta.getMoreResults() );

      // no update count expected
      assertEquals( -1, sta.getUpdateCount() );
      sta.close();
   }

   /**
    * <p> Test to ensure that single results generated as result of aggregation
    * operations (COMPUTE clause) can be closed individually without affecting
    * remaining {@link ResultSet}s. </p>
    */
   public void testCloseComputedResult()
      throws Exception
   {
      Statement sta = con.createStatement();

      sta.executeUpdate( "create table #Bug678( NAME varchar(10), CREDITS int )" );

      sta.executeUpdate( "insert into #Bug678 values( 'Alf'  , 10 )" );
      sta.executeUpdate( "insert into #Bug678 values( 'Alf'  , 20 )" );
      sta.executeUpdate( "insert into #Bug678 values( 'Alf'  , 30 )" );
      sta.executeUpdate( "insert into #Bug678 values( 'Ronny',  5 )" );
      sta.executeUpdate( "insert into #Bug678 values( 'Ronny', 10 )" );

      assertTrue( sta.execute( "select * from #Bug678 order by NAME compute sum( CREDITS ) by NAME" ) );

      ResultSet res = sta.getResultSet();

      // check 1st row of 1st ResultSet
      assertTrue  ( res.next() );
      assertEquals( "Alf", res.getString( 1 ) );
      assertEquals( 10, res.getInt( 2 ) );
      assertTrue  ( res.next() );

      // close 1st ResultSet
      res.close();

      // 3 ResultSets should be left
      assertTrue( sta.getMoreResults() );
      res = sta.getResultSet();

      // close 2nd (computed) ResultSet without processing it
      res.close();

      // 2 ResultSets should be left
      assertTrue( sta.getMoreResults() );
      res = sta.getResultSet();

      // check 1st row of 3rd ResultSet
      assertTrue( res.next() );
      assertEquals( "Ronny", res.getString( 1 ) );
      assertEquals( 5, res.getInt( 2 ) );

      // close 3rd ResultSet
      res.close();

      // 1 ResultSet should be left
      assertTrue( sta.getMoreResults() );
      res = sta.getResultSet();

      // check 1st row of 4th (computed) ResultSet
      assertTrue( res.next() );
      assertEquals( 15, res.getInt( 1 ) );
      assertFalse( res.next() );

      // no ResultSets should be left
      assertFalse( sta.getMoreResults() );

      sta.close();
   }

   /**
    *
    */
   public void testConcurrentClose()
      throws Exception
   {
      final int THREADS    =  10;
      final int STATEMENTS = 200;
      final int RESULTSETS = 100;

      final List errors = new ArrayList();

      final Statement[] stm = new Statement[STATEMENTS];
      final ResultSet[] res = new ResultSet[STATEMENTS*RESULTSETS];

      Connection con = getConnection();

      for( int i = 0; i < STATEMENTS; i ++ )
      {
         stm[i] = con.createStatement();

         for( int r = 0; r < RESULTSETS; r ++ )
         {
            res[i * RESULTSETS + r] = stm[i].executeQuery( "select 1" );
         }
      }

      Thread[] threads = new Thread[THREADS];

      for( int i = 0; i < THREADS; i ++ )
      {
         threads[i] = new Thread( "closer " + i )
         {
            public void run()
            {
               try
               {
                  for( int i = 0; i < STATEMENTS; i ++ )
                  {
                     stm[i].close();
                  }
               }
               catch( Exception e )
               {
                  synchronized( errors )
                  {
                     errors.add( e );
                  }
               }
            }
         };
      }

      for( int i = 0; i < THREADS; i ++ )
      {
         threads[i].start();
      }

      for( int i = 0; i < THREADS; i ++ )
      {
         threads[i].join();
      }

      for( int i = 0; i < errors.size(); i ++ )
      {
         ( (Exception) errors.get( i ) ).printStackTrace();
      }

      assertTrue( errors.toString(), errors.isEmpty() );
   }

   /**
    * Regression test for bug #677, deadlock in {@link JtdsStatement#close()}.
    */
   public void testCloseDeadlock()
      throws Exception
   {
      final int THREADS    =  100;
      final int STATEMENTS = 1000;

      final List errors = new ArrayList();

      Thread[] threads = new Thread[THREADS];

      for( int i = 0; i < THREADS; i ++ )
      {
         threads[i] = new Thread( "deadlock " + i )
         {
            public void run()
            {
               try
               {
                  Connection con = getConnection();
                  final Statement[] stm = new Statement[STATEMENTS];

                  for( int i = 0; i < STATEMENTS; i ++ )
                  {
                     stm[i] = con.createStatement();
                  }

                  new Thread( Thread.currentThread().getName() + " (closer)" )
                  {
                     public void run()
                     {
                        try
                        {
                           for( int i = 0; i < STATEMENTS; i ++ )
                           {
                              stm[i].close();
                           }
                        }
                        catch( SQLException e )
                        {
                           // statements might already be closed by closing the connection
                           if( ! "HY010".equals( e.getSQLState() ) )
                           {
                              synchronized( errors )
                              {
                                 errors.add( e );
                              }
                           }
                        }
                     }
                  }.start();

                  Thread.sleep( 1 );
                  con.close();
               }
               catch( Exception e )
               {
                  synchronized( errors )
                  {
                     errors.add( e );
                  }
               }
            }
         };
      }

      for( int i = 0; i < THREADS; i ++ )
      {
         threads[i].start();
      }

      System.currentTimeMillis();
      int  running = THREADS;

      while( running != 0 )
      {
         Thread.sleep( 2500 );

         int last = running;
         running  = THREADS;

         for( int i = 0; i < THREADS; i ++ )
         {
            if( threads[i].getState() == Thread.State.TERMINATED )
            {
               running --;
            }
         }

         if( running == last )
         {
//             for( int i = 0; i < THREADS; i ++ )
//             {
//                if( threads[i].getState() != Thread.State.TERMINATED )
//                {
//                   Exception e = new Exception();
//                   e.setStackTrace( threads[i].getStackTrace() );
//                   e.printStackTrace();
//                }
//             }

            fail( "deadlock detected, none of the remaining connections closed within 2500 ms" );
         }
      }

//      for( int i = 0; i < errors.size(); i ++ )
//      {
//         ( (Exception) errors.get( i ) ).printStackTrace();
//      }

      assertTrue( errors.toString(), errors.isEmpty() );
   }

    /**
     * Test for #676, error in multi line comment handling.
     */
    public void testMultiLineComment()
       throws Exception
    {
       Statement st = con.createStatement();

       st.executeUpdate( "create table /*/ comment '\"?@[*-} /**/*/ #Bug676a (A int) /* */" );

       try
       {
          // SQL server stacks, instead of ignoring 'inner comments'
          st.executeUpdate( "create table /* /* */ #Bug676b (A int)" );
       }
       catch( SQLException e )
       {
          // thrown by jTDS due to unclosed 'inner comment'
          assertEquals( String.valueOf( 22025 ), e.getSQLState() );
       }

       st.close();
    }

    /**
     * Test for bug #669, no error if violating unique constraint in update.
     */
    public void testDuplicateKey()
       throws Exception
    {
       Statement st = con.createStatement();
       st.executeUpdate( "create table #Bug669 (A int, unique (A))" );
       st.executeUpdate( "insert into #Bug669 values( 1 )" );
       try
       {
          st.executeUpdate( "insert into #Bug669 values( 1 )" );
          fail();
       }
       catch( SQLException e )
       {
         // expected, unique constraint violation
       }
       try
       {
          st.execute( "insert into #Bug669 values( 1 )" );
          fail();
       }
       catch( SQLException e )
       {
         // expected, unique constraint violation
       }
       st.close();
    }

    /**
     * Test for bug [1694194], queryTimeout does not work on MSSQL2005 when
     * property 'useCursors' is set to 'true'. Furthermore, the test also
     * checks timeout with a query that cannot use a cursor. <p>
     *
     * This test requires property 'queryTimeout' to be set to true.
     */
    public void testQueryTimeout() throws Exception {
        Statement st = con.createStatement();
        st.setQueryTimeout(1);

        st.execute("create procedure #testTimeout as begin waitfor delay '00:00:30'; select 1; end");

        long start = System.currentTimeMillis();
        try {
            // this query doesn't use a cursor
            st.executeQuery("exec #testTimeout");
            fail("query did not time out");
        } catch (SQLException e) {
            assertEquals("HYT00", e.getSQLState());
            assertEquals(1000, System.currentTimeMillis() - start, 50);
        }

        st.execute("create table #dummy1(A varchar(200))");
        st.execute("create table #dummy2(B varchar(200))");
        st.execute("create table #dummy3(C varchar(200))");

        // create test data
        con.setAutoCommit(false);
        for(int i = 0; i < 100; i++) {
            st.execute("insert into #dummy1 values('" + i + "')");
            st.execute("insert into #dummy2 values('" + i + "')");
            st.execute("insert into #dummy3 values('" + i + "')");
        }
        con.commit();
        con.setAutoCommit(true);

        start = System.currentTimeMillis();
        try {
            // this query can use a cursor
            st.executeQuery("select * from #dummy1, #dummy2, #dummy3 order by A desc, B asc, C desc");
            fail("query did not time out");
        } catch (SQLException e) {
            assertEquals("HYT00", e.getSQLState());
            assertEquals(1000, System.currentTimeMillis() - start, 50);
        }

        st.close();
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(StatementTest.class);
    }

}