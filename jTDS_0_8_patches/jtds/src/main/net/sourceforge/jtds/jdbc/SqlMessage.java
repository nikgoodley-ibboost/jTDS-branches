//
// Copyright 1998 CDS Networks, Inc., Medford Oregon
//
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
// 1. Redistributions of source code must retain the above copyright
//    notice, this list of conditions and the following disclaimer.
// 2. Redistributions in binary form must reproduce the above copyright
//    notice, this list of conditions and the following disclaimer in the
//    documentation and/or other materials provided with the distribution.
// 3. All advertising materials mentioning features or use of this software
//    must display the following acknowledgement:
//      This product includes software developed by CDS Networks, Inc.
// 4. The name of CDS Networks, Inc.  may not be used to endorse or promote
//    products derived from this software without specific prior
//    written permission.
//
// THIS SOFTWARE IS PROVIDED BY CDS NETWORKS, INC. ``AS IS'' AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED.  IN NO EVENT SHALL CDS NETWORKS, INC. BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
// OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
// HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
// LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
// OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
// SUCH DAMAGE.
//

package net.sourceforge.jtds.jdbc;

import java.util.HashMap;

/**
 * Helper class for handling SQL warnings and errors. Assigns SQL state values
 * in accordance to the native error number returned by the database server.
 */
class SqlMessage {
    public static final String cvsVersion = "$Id: SqlMessage.java,v 1.2 2004-03-21 19:24:11 alin_sinpalean Exp $";

    /**
     * Map to convert Microsoft SQL server error codes to ANSI SQLSTATE codes.
     * The values in this table are derived from the list compiled by the
     * FreeTDS project. Thank you for the hard work.
     */
    private final static HashMap mssqlStates = new HashMap();

    /**
     * Map to convert Sybase SQL server error codes to ANSI SQLSTATE codes.
     * The values in this table are derived from the list compiled by the
     * FreeTDS project. Thank you for the hard work.
     */
    private final static HashMap sybStates = new HashMap();

    static {
        // When adding values into this map please ensure that you maintain
        // the ascending order. This is for readability purposes only, but it's
        // still important.
        mssqlStates.put(new Integer(105), "37000"); // ADDED
        mssqlStates.put(new Integer(109), "21S01");
        mssqlStates.put(new Integer(110), "21S01");
        mssqlStates.put(new Integer(113), "42000");
        mssqlStates.put(new Integer(131), "37000");
        mssqlStates.put(new Integer(168), "22003");
        mssqlStates.put(new Integer(170), "37000");
        mssqlStates.put(new Integer(174), "37000");
        mssqlStates.put(new Integer(201), "37000");
        mssqlStates.put(new Integer(206), "22005"); // ADDED
        mssqlStates.put(new Integer(207), "42S22"); // (i-net is 42000)
        mssqlStates.put(new Integer(208), "42S02"); // (i-net is 42000)
        mssqlStates.put(new Integer(210), "22007"); // ? (i-net is 22008)
        mssqlStates.put(new Integer(211), "22008"); // ADDED
        mssqlStates.put(new Integer(213), "42000"); // MODIFIED: was 21S01
        mssqlStates.put(new Integer(220), "22003");
        mssqlStates.put(new Integer(229), "42000");
        mssqlStates.put(new Integer(230), "42000");
        mssqlStates.put(new Integer(232), "22003");
        mssqlStates.put(new Integer(233), "23000"); // ADDED
        mssqlStates.put(new Integer(234), "22003"); // ADDED
        mssqlStates.put(new Integer(235), "22005"); // ADDED
        mssqlStates.put(new Integer(236), "22003"); // ADDED
        mssqlStates.put(new Integer(237), "22003"); // ADDED
        mssqlStates.put(new Integer(238), "22003"); // ADDED
        mssqlStates.put(new Integer(241), "22007"); // ? (i-net is 22008)
        mssqlStates.put(new Integer(242), "22008");
        mssqlStates.put(new Integer(244), "22003"); // ADDED
        mssqlStates.put(new Integer(245), "22018"); // ? (i-net is 22005)
        mssqlStates.put(new Integer(246), "22003"); // ADDED
        mssqlStates.put(new Integer(247), "22005"); // ADDED
        mssqlStates.put(new Integer(248), "22003"); // ADDED
        mssqlStates.put(new Integer(249), "22005"); // ADDED
        mssqlStates.put(new Integer(256), "22005"); // ADDED
        mssqlStates.put(new Integer(257), "22005"); // ADDED
        mssqlStates.put(new Integer(260), "42000"); // ADDED
        mssqlStates.put(new Integer(262), "42000");
        mssqlStates.put(new Integer(266), "25000");
        mssqlStates.put(new Integer(272), "23000"); // ADDED
        mssqlStates.put(new Integer(273), "23000"); // ADDED
        mssqlStates.put(new Integer(277), "25000"); // ADDED
        mssqlStates.put(new Integer(295), "22007"); // ? (i-net is 22008)
        mssqlStates.put(new Integer(296), "22008");
        mssqlStates.put(new Integer(298), "22008");
        mssqlStates.put(new Integer(305), "22005"); // ADDED
        mssqlStates.put(new Integer(307), "42S12");
        mssqlStates.put(new Integer(308), "42S12");
        mssqlStates.put(new Integer(310), "22025"); // ADDED
        mssqlStates.put(new Integer(409), "22005"); // ADDED
        mssqlStates.put(new Integer(506), "22019"); // ADDED
        mssqlStates.put(new Integer(512), "21000");
        mssqlStates.put(new Integer(515), "23000");
        mssqlStates.put(new Integer(517), "22008"); // ?
        mssqlStates.put(new Integer(518), "22005"); // ADDED
        mssqlStates.put(new Integer(519), "22003"); // ADDED
        mssqlStates.put(new Integer(520), "22003"); // ADDED
        mssqlStates.put(new Integer(521), "22003"); // ADDED
        mssqlStates.put(new Integer(522), "22003"); // ADDED
        mssqlStates.put(new Integer(523), "22003"); // ADDED
        mssqlStates.put(new Integer(524), "22003"); // ADDED
        mssqlStates.put(new Integer(529), "22005"); // ADDED
        mssqlStates.put(new Integer(530), "23000"); // ADDED
        mssqlStates.put(new Integer(532), "01001"); // ADDED
        mssqlStates.put(new Integer(535), "22003"); // MODIFIED: was 22008
        mssqlStates.put(new Integer(542), "22008");
        mssqlStates.put(new Integer(544), "23000");
        mssqlStates.put(new Integer(547), "23000");
        mssqlStates.put(new Integer(550), "44000"); // MODIFIED: was 23000
        mssqlStates.put(new Integer(611), "25000"); // ADDED
        mssqlStates.put(new Integer(626), "25000");
        mssqlStates.put(new Integer(627), "25000");
        mssqlStates.put(new Integer(628), "25000");
        mssqlStates.put(new Integer(911), "08004"); // WRONG: db not found != connection rejected
        mssqlStates.put(new Integer(1007), "22003");
        mssqlStates.put(new Integer(1010), "22019"); // ADDED
        mssqlStates.put(new Integer(1205), "40001"); // ADDED
        mssqlStates.put(new Integer(1211), "40001"); // ADDED
        mssqlStates.put(new Integer(1505), "23000");
        mssqlStates.put(new Integer(1508), "23000");
        mssqlStates.put(new Integer(1774), "21S02");
        mssqlStates.put(new Integer(1911), "42S22");
        mssqlStates.put(new Integer(1913), "42S11");
        mssqlStates.put(new Integer(2526), "37000");
        mssqlStates.put(new Integer(2557), "42000");
        mssqlStates.put(new Integer(2571), "42000");
        mssqlStates.put(new Integer(2601), "23000"); // ADDED
        mssqlStates.put(new Integer(2615), "23000"); // ADDED
        mssqlStates.put(new Integer(2625), "40001"); // ADDED
        mssqlStates.put(new Integer(2626), "23000"); // ADDED
        mssqlStates.put(new Integer(2627), "23000");
        mssqlStates.put(new Integer(2714), "42S01");
        mssqlStates.put(new Integer(2760), "42000");
        mssqlStates.put(new Integer(2812), "37000");
        mssqlStates.put(new Integer(3110), "42000");
        mssqlStates.put(new Integer(3309), "40001"); // ADDED
        mssqlStates.put(new Integer(3604), "23000"); // ADDED
        mssqlStates.put(new Integer(3605), "23000"); // ADDED
        mssqlStates.put(new Integer(3606), "22003");
        mssqlStates.put(new Integer(3607), "22012");
        mssqlStates.put(new Integer(3621), "01000");
        mssqlStates.put(new Integer(3701), "42S02");
        mssqlStates.put(new Integer(3704), "42000");
        mssqlStates.put(new Integer(3725), "23000");
        mssqlStates.put(new Integer(3726), "23000");
        mssqlStates.put(new Integer(3902), "25000");
        mssqlStates.put(new Integer(3903), "25000");
        mssqlStates.put(new Integer(3906), "25000"); // ADDED
        mssqlStates.put(new Integer(3908), "25000"); // ADDED
        mssqlStates.put(new Integer(3915), "25000"); // ADDED
        mssqlStates.put(new Integer(3916), "25000");
        mssqlStates.put(new Integer(3918), "25000");
        mssqlStates.put(new Integer(3919), "25000");
        mssqlStates.put(new Integer(3921), "25000");
        mssqlStates.put(new Integer(3922), "25000");
        mssqlStates.put(new Integer(3926), "25000");
        mssqlStates.put(new Integer(4415), "44000"); // MODIFIED: was 23000
        mssqlStates.put(new Integer(4613), "42000");
        mssqlStates.put(new Integer(4618), "42000");
        mssqlStates.put(new Integer(4712), "23000");
        mssqlStates.put(new Integer(4834), "42000");
        mssqlStates.put(new Integer(4924), "42S22");
        mssqlStates.put(new Integer(4925), "42S21");
        mssqlStates.put(new Integer(4926), "42S22");
        mssqlStates.put(new Integer(5011), "42000");
        mssqlStates.put(new Integer(5116), "42000");
        mssqlStates.put(new Integer(5146), "22003");
        mssqlStates.put(new Integer(5812), "42000");
        mssqlStates.put(new Integer(6004), "42000");
        mssqlStates.put(new Integer(6102), "42000");
        mssqlStates.put(new Integer(6104), "37000");
        mssqlStates.put(new Integer(6401), "25000"); // ADDED
        mssqlStates.put(new Integer(7112), "40001"); // ADDED
        mssqlStates.put(new Integer(7956), "42000");
        mssqlStates.put(new Integer(7969), "25000");
        mssqlStates.put(new Integer(8114), "37000");
        mssqlStates.put(new Integer(8115), "22003");
        mssqlStates.put(new Integer(8134), "22012");
        mssqlStates.put(new Integer(8144), "37000");
        mssqlStates.put(new Integer(8152), "22001");
        mssqlStates.put(new Integer(8153), "01003");
        mssqlStates.put(new Integer(8506), "25000");
        mssqlStates.put(new Integer(10015), "22003"); // ADDED
        mssqlStates.put(new Integer(10033), "42S12");
        mssqlStates.put(new Integer(10055), "23000");
        mssqlStates.put(new Integer(10065), "23000");
        mssqlStates.put(new Integer(10095), "01001"); // ADDED
        mssqlStates.put(new Integer(11010), "42000");
        mssqlStates.put(new Integer(11011), "23000");
        mssqlStates.put(new Integer(11040), "23000");
        mssqlStates.put(new Integer(11045), "42000");
        mssqlStates.put(new Integer(14126), "42000");
        mssqlStates.put(new Integer(15247), "42000");
        mssqlStates.put(new Integer(15323), "42S12");
        mssqlStates.put(new Integer(15605), "42S11");
        mssqlStates.put(new Integer(15622), "42000");
        mssqlStates.put(new Integer(15626), "25000");
        mssqlStates.put(new Integer(15645), "42S22");
        mssqlStates.put(new Integer(16905), "24000");
        mssqlStates.put(new Integer(16909), "24000"); // ADDED
        mssqlStates.put(new Integer(16911), "24000"); // ADDED
        mssqlStates.put(new Integer(16917), "24000");
        mssqlStates.put(new Integer(16946), "24000");
        mssqlStates.put(new Integer(16950), "24000");
        mssqlStates.put(new Integer(16999), "24000");
        mssqlStates.put(new Integer(17308), "42000");
        mssqlStates.put(new Integer(17571), "42000");
        mssqlStates.put(new Integer(18002), "42000");
        mssqlStates.put(new Integer(18456), "28000");
        mssqlStates.put(new Integer(18833), "42S12");
        mssqlStates.put(new Integer(20604), "42000");
        mssqlStates.put(new Integer(21049), "42000");
        mssqlStates.put(new Integer(21166), "42S22");
        mssqlStates.put(new Integer(21255), "42S21");

        // When adding values into this map please ensure that you maintain
        // the ascending order. This is for readability purposes only, but it's
        // still important.
        sybStates.put(new Integer(102), "37000");
        sybStates.put(new Integer(109), "21S01");
        sybStates.put(new Integer(110), "21S01");
        sybStates.put(new Integer(113), "42000");
        sybStates.put(new Integer(168), "22003");
        sybStates.put(new Integer(201), "37000");
        sybStates.put(new Integer(207), "42S22");
        sybStates.put(new Integer(208), "42S02");
        sybStates.put(new Integer(213), "21S01");
        sybStates.put(new Integer(220), "22003");
        sybStates.put(new Integer(227), "22003");
        sybStates.put(new Integer(229), "42000");
        sybStates.put(new Integer(230), "42000");
        sybStates.put(new Integer(232), "22003");
        sybStates.put(new Integer(233), "23000");
        sybStates.put(new Integer(245), "22018");
        sybStates.put(new Integer(247), "22003");
        sybStates.put(new Integer(257), "37000");
        sybStates.put(new Integer(262), "42000");
        sybStates.put(new Integer(277), "25000");
        sybStates.put(new Integer(307), "42S12");
        sybStates.put(new Integer(512), "21000");
        sybStates.put(new Integer(517), "22008");
        sybStates.put(new Integer(535), "22008");
        sybStates.put(new Integer(542), "22008");
        sybStates.put(new Integer(544), "23000");
        sybStates.put(new Integer(545), "23000");
        sybStates.put(new Integer(546), "23000");
        sybStates.put(new Integer(547), "23000");
        sybStates.put(new Integer(548), "23000");
        sybStates.put(new Integer(549), "23000");
        sybStates.put(new Integer(550), "23000");
        sybStates.put(new Integer(558), "24000");
        sybStates.put(new Integer(559), "24000");
        sybStates.put(new Integer(562), "24000");
        sybStates.put(new Integer(565), "24000");
        sybStates.put(new Integer(583), "24000");
        sybStates.put(new Integer(611), "25000");
        sybStates.put(new Integer(627), "25000");
        sybStates.put(new Integer(628), "25000");
        sybStates.put(new Integer(641), "25000");
        sybStates.put(new Integer(642), "25000");
        sybStates.put(new Integer(911), "08004");
        sybStates.put(new Integer(1276), "25000");
        sybStates.put(new Integer(1505), "23000");
        sybStates.put(new Integer(1508), "23000");
        sybStates.put(new Integer(1715), "21S02");
        sybStates.put(new Integer(1720), "42S22");
        sybStates.put(new Integer(1913), "42S11");
        sybStates.put(new Integer(1921), "42S21");
        sybStates.put(new Integer(2526), "37000");
        sybStates.put(new Integer(2714), "42S01");
        sybStates.put(new Integer(2812), "37000");
        sybStates.put(new Integer(3606), "22003");
        sybStates.put(new Integer(3607), "22012");
        sybStates.put(new Integer(3621), "01000");
        sybStates.put(new Integer(3701), "42S02");
        sybStates.put(new Integer(3902), "25000");
        sybStates.put(new Integer(3903), "25000");
        sybStates.put(new Integer(4602), "42000");
        sybStates.put(new Integer(4603), "42000");
        sybStates.put(new Integer(4608), "42000");
        sybStates.put(new Integer(4934), "42S22");
        sybStates.put(new Integer(6104), "37000");
        sybStates.put(new Integer(6235), "24000");
        sybStates.put(new Integer(6259), "24000");
        sybStates.put(new Integer(6260), "24000");
        sybStates.put(new Integer(7010), "42S12");
        sybStates.put(new Integer(7327), "37000");
        sybStates.put(new Integer(9501), "01003");
        sybStates.put(new Integer(9502), "22001");
        sybStates.put(new Integer(10306), "42000");
        sybStates.put(new Integer(10323), "42000");
        sybStates.put(new Integer(10330), "42000");
        sybStates.put(new Integer(10331), "42000");
        sybStates.put(new Integer(10332), "42000");
        sybStates.put(new Integer(11021), "37000");
        sybStates.put(new Integer(11110), "42000");
        sybStates.put(new Integer(11113), "42000");
        sybStates.put(new Integer(11118), "42000");
        sybStates.put(new Integer(11121), "42000");
        sybStates.put(new Integer(17222), "42000");
        sybStates.put(new Integer(17223), "42000");
        sybStates.put(new Integer(18091), "42S12");
        sybStates.put(new Integer(18117), "42S22");
        sybStates.put(new Integer(18350), "42000");
        sybStates.put(new Integer(18351), "42000");
    }

    /** SQL Server error number. */
    int number;
    /** SQL Server state code. */
    int state;
    /** SQL Server serverity > 10 = error. */
    int severity;
    /** SQL Server error message text. */
    String message;
    /** SQL Server name. */
    String server;
    /** SQL Server stored procedure name. */
    String procName;
    /** SQL Server error line number in SQL source. */
    int line;

    /**
     * SQL Server type. Either <code>Tds.SQLSERVER</code> or
     * <code>Tds.SYBASE</code>.
     */
    private final int serverType;

    /**
     * Create an SQL message for a specific server type.
     *
     * @param serverType either <code>Tds.SQLSERVER</code> or
     *                   <code>Tds.SYBASE</code>
     */
    SqlMessage(final int serverType) {
        this.serverType = serverType;
    }

    /**
     * Convert an SQL message from the server into a human readable string.
     *
     * @return human readable string of the SQLServer message
     */
    public String toString() {
        return "Msg " + number + ", "
                + "Severity " + severity + ", "
                + "State " + state + ", "
                + "SQL State " + getStateCode(number, serverType, "S1000") + ", "
                + message + ", "
                + "Server " + server + ", "
                + "Procedure " + procName + ", "
                + "Line " + line;
    }

    /**
     * Convert the SQL message information into an <code>SQLWarning</code>.
     *
     * @return the message information as an <code>SQLWarning</code>
     */
    public java.sql.SQLWarning toSQLWarning() {
        return new java.sql.SQLWarning(
                message, getStateCode(number, serverType, "01000"), number);
    }

    /**
     * Convert the SQL error message information into an
     * <code>SQLException</code>.
     *
     * @return the error information as an <code>SQLException</code>
     */
    public java.sql.SQLException toSQLException() {
        return new java.sql.SQLException(
                message, getStateCode(number, serverType, "S1000"), number);
    }

    /**
     * Map an SQL Server error code to an ANSI SQLSTATE code.
     *
     * @param number     the SQL Server error number
     * @param serverType <code>Tds.SQLSERVER</code> or <code>Tds.SYBASE</code>
     * @param defState   the default state code to return if the mapping fails
     * @return           the SQLSTATE code as a <code>String</code>
     */
    private static String getStateCode(
            final int number, final int serverType, final String defState) {
        final HashMap stateTable =
                (serverType == Tds.SYBASE) ? sybStates : mssqlStates;
        final String state = (String) stateTable.get(new Integer(number));
        if (state != null) {
            return state;
        } else {
            return defState;
        }
    }
}
