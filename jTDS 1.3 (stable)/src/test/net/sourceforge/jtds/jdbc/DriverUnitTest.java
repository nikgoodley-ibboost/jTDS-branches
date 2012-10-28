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

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import net.sourceforge.jtds.jdbc.DefaultProperties;
import net.sourceforge.jtds.jdbc.Driver;
import net.sourceforge.jtds.jdbc.Messages;
import net.sourceforge.jtds.jdbc.TdsCore;

import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;



/**
 * Unit tests for the {@link Driver} class.
 *
 * @author David D. Kilzer
 * @version $Id: DriverUnitTest.java,v 1.20 2007-07-08 18:08:54 bheineman Exp $
 */
public class DriverUnitTest extends UnitTestBase {


    /**
     * Construct a test suite for this class.
     * <p/>
     * The test suite includes the tests in this class, and adds tests
     * from {@link DefaultPropertiesTestLibrary} after creating
     * anonymous {@link DefaultPropertiesTester} objects.
     *
     * @return The test suite to run.
     */
    public static Test suite() {

        TestSuite testSuite = new TestSuite(DriverUnitTest.class);

        testSuite.addTest(
                Test_Driver_setupConnectProperties.suite("test_setupConnectProperties_DefaultProperties"));

        testSuite.addTest(
                Test_Driver_getPropertyInfo.suite("test_getPropertyInfo_DefaultProperties"));

        addParseURLCorrectTests(testSuite);

        return testSuite;
    }

   /**
    * Test to ensure that the version reported by the driver matches the JAR
    * file's name.
    */
   public void testDriverVersion()
      throws Exception
   {
      String file = Driver.class.getResource( '/' + Driver.class.getName().replace( '.', '/' ) + ".class" ).toString();

      // only check if jTDS has been loaded from a jar
      if( file.startsWith( "jar" ) )
      {
         // parse path, e.g. jar:file:/lib/jtds-1.3.0.jar!/net/sourceforge/jtds/jdbc/Driver.class
         file = file.substring( 0, file.indexOf( ".jar!" ) );
         file = file.substring( file.lastIndexOf( '/' ) + 1 );

         assertEquals( Driver.getVersion(), file.substring( file.lastIndexOf( '-' ) + 1 ) );
      }
   }

    /**
     * Constructor.
     *
     * @param name The name of the test.
     */
    public DriverUnitTest(final String name) {
        super(name);
    }


    /**
     * Tests that passing in a null properties argument to
     * {@link Driver#getPropertyInfo(String, Properties)}
     * causes the url to be parsed, which then throws a {@link SQLException}.
     */
    public void test_getPropertyInfo_ThrowsSQLExceptionWithNullProperties() {
        try {
            new Driver().getPropertyInfo("wxyz:", null);
            fail("Expected SQLException to be throw");
        }
        catch (SQLException e) {
            // Expected
        }
    }


    /**
     * Tests that passing in a non-null properties argument to
     * {@link Driver#getPropertyInfo(String, Properties)}
     * causes the url to be parsed, which then throws a {@link SQLException}.
     */
    public void test_getPropertyInfo_ThrowsSQLExceptionWithNonNullProperties() {
        try {
            new Driver().getPropertyInfo("wxyz:", new Properties());
            fail("Expected SQLException to be throw");
        }
        catch (SQLException e) {
            // Expected
        }
    }


    /**
     * Tests that the {@link DriverPropertyInfo} array returned from
     * {@link Driver#getPropertyInfo(String, Properties)}
     * matches the list of properties defined in <code>Messages.properties</code>.
     */
    public void test_getPropertyInfo_MatchesMessagesProperties() {

        final Map infoMap = new HashMap();
        loadDriverPropertyInfoMap(infoMap);

        final Map propertyMap = new HashMap();
        final Map descriptionMap = new HashMap();
        invokeStaticMethod(
                Messages.class, "loadDriverProperties",
                new Class[]{Map.class, Map.class},
                new Object[]{propertyMap, descriptionMap});

        assertEquals(
                "Properties list size (expected) does not equal DriverPropertyInfo array length (actual)",
                propertyMap.size(), infoMap.keySet().size());
        assertEquals(
                "Description list size (expected) does not equal DriverPropertyInfo array length (actual)",
                descriptionMap.size(), infoMap.keySet().size());

        for (Iterator iterator = propertyMap.keySet().iterator(); iterator.hasNext();) {
            final String key = (String) iterator.next();
            final DriverPropertyInfo driverPropertyInfo =
                    (DriverPropertyInfo) infoMap.get(propertyMap.get(key));
            assertNotNull("No DriverPropertyInfo object exists for property '" + key + "'", driverPropertyInfo);
            assertEquals(
                    "Property description (expected) does not match DriverPropertyInfo description (actual)",
                    descriptionMap.get(key), driverPropertyInfo.description);
        }
    }


    /**
     * Tests that the {@link DriverPropertyInfo} array returned from
     * {@link Driver#getPropertyInfo(String, Properties)} contains
     * the correct <code>choices</code> value on each of the objects.
     */
    public void test_getPropertyInfo_Choices() {

        String[] expectedBooleanChoices = new String[]{
            String.valueOf(true),
            String.valueOf(false),
        };
        String[] expectedPrepareSqlChoices = new String[]{
            String.valueOf(TdsCore.UNPREPARED),
            String.valueOf(TdsCore.TEMPORARY_STORED_PROCEDURES),
            String.valueOf(TdsCore.EXECUTE_SQL),
            String.valueOf(TdsCore.PREPARE)
        };
        String[] expectedServerTypeChoices = new String[]{
            String.valueOf(Driver.SQLSERVER),
            String.valueOf(Driver.SYBASE),
        };
        String[] expectedTdsChoices = new String[]{
            DefaultProperties.TDS_VERSION_42,
            DefaultProperties.TDS_VERSION_50,
            DefaultProperties.TDS_VERSION_70,
            DefaultProperties.TDS_VERSION_80,
        };

        Map expectedChoicesMap = new HashMap();
        expectedChoicesMap.put(Messages.get(Driver.LASTUPDATECOUNT), expectedBooleanChoices);
        expectedChoicesMap.put(Messages.get(Driver.NAMEDPIPE), expectedBooleanChoices);
        expectedChoicesMap.put(Messages.get(Driver.PREPARESQL), expectedPrepareSqlChoices);
        expectedChoicesMap.put(Messages.get(Driver.SERVERTYPE), expectedServerTypeChoices);
        expectedChoicesMap.put(Messages.get(Driver.TDS), expectedTdsChoices);
        expectedChoicesMap.put(Messages.get(Driver.SENDSTRINGPARAMETERSASUNICODE), expectedBooleanChoices);
        expectedChoicesMap.put(Messages.get(Driver.CACHEMETA), expectedBooleanChoices);
        expectedChoicesMap.put(Messages.get(Driver.USECURSORS), expectedBooleanChoices);
        expectedChoicesMap.put(Messages.get(Driver.USELOBS), expectedBooleanChoices);

        final Map infoMap = new HashMap();
        loadDriverPropertyInfoMap(infoMap);

        final Iterator iterator = infoMap.keySet().iterator();
        while (iterator.hasNext()) {
            String key = (String) iterator.next();
            DriverPropertyInfo info = (DriverPropertyInfo) infoMap.get(key);
            if (expectedChoicesMap.containsKey(key)) {
                assertEquals("Choices did not match for key " + key,
                             ((String[]) expectedChoicesMap.get(key)), info.choices);
            }
            else {
                assertNull("Expected choices to be null for key " + key,
                           expectedChoicesMap.get(key));
            }
        }
    }


    /**
     * Tests that the {@link DriverPropertyInfo} array returned from
     * {@link Driver#getPropertyInfo(String, Properties)} contains
     * the correct <code>required</code> value on each of the objects.
     */
    public void test_getPropertyInfo_Required() {

        Map requiredTrueMap = new HashMap();
        requiredTrueMap.put(Messages.get(Driver.SERVERNAME), Boolean.TRUE);
        requiredTrueMap.put(Messages.get(Driver.SERVERTYPE), Boolean.TRUE);

        final Map infoMap = new HashMap();
        loadDriverPropertyInfoMap(infoMap);

        final Iterator iterator = infoMap.keySet().iterator();
        while (iterator.hasNext()) {
            String key = (String) iterator.next();
            DriverPropertyInfo info = (DriverPropertyInfo) infoMap.get(key);
            if (requiredTrueMap.containsKey(key)) {
                assertTrue("The 'required' field is not true for key " + key, info.required);
            }
            else {
                assertFalse("The 'required' field is not false for key " + key, info.required);
            }
        }
    }


    /**
     * Retrieve the {@link DriverPropertyInfo} array from
     * {@link Driver#getPropertyInfo(String, Properties)} and convert it
     * into a {@link Map} using the <code>name</code> property for the keys.
     *
     * @param driverPropertyInfoMap The map of {@link DriverPropertyInfo} objects to be populated.
     */
    private void loadDriverPropertyInfoMap(final Map driverPropertyInfoMap) {
        try {
            final DriverPropertyInfo[] driverPropertyInfoArray = new Driver().getPropertyInfo(
                                "jdbc:jtds:sqlserver://servername/databasename", new Properties());
            for (int i = 0; i < driverPropertyInfoArray.length; i++) {
                DriverPropertyInfo driverPropertyInfo = driverPropertyInfoArray[i];
                driverPropertyInfoMap.put(driverPropertyInfo.name, driverPropertyInfo);
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
    }



    /**
     * Class used to test <code>Driver.setupConnectProperties(String, java.util.Properties)</code>.
     */
    public static class Test_Driver_setupConnectProperties extends DefaultPropertiesTestLibrary {

        /**
         * Construct a test suite for this library.
         *
         * @param name The name of the tests.
         * @return The test suite.
         */
        public static Test suite(String name) {
            return new TestSuite(Test_Driver_setupConnectProperties.class, name);
        }

        /**
         * Default constructor.
         */
        public Test_Driver_setupConnectProperties() {
            setTester(
                    new DefaultPropertiesTester() {

                        public void assertDefaultProperty(
                                String message, String url, Properties properties, String fieldName,
                                String key, String expected) {

                            Properties results =
                                    (Properties) invokeInstanceMethod(
                                            new Driver(), "setupConnectProperties",
                                            new Class[]{String.class, Properties.class},
                                            new Object[]{url, properties});

                            assertEquals(message, expected, results.getProperty(Messages.get(key)));
                        }
                    }
            );
        }
    }



    /**
     * Class used to test {@link Driver#getPropertyInfo(String, Properties)}.
     */
    public static class Test_Driver_getPropertyInfo extends DefaultPropertiesTestLibrary {

        /**
         * Construct a test suite for this library.
         *
         * @param name The name of the tests.
         * @return The test suite.
         */
        public static Test suite(String name) {
            return new TestSuite(Test_Driver_getPropertyInfo.class, name);
        }

        /**
         * Default constructor.
         */
        public Test_Driver_getPropertyInfo() {
            setTester(
                    new DefaultPropertiesTester() {

                        public void assertDefaultProperty(
                                String message, String url, Properties properties, String fieldName,
                                String key, String expected) {

                            try {
                                boolean found = false;
                                String messageKey = Messages.get(key);

                                DriverPropertyInfo[] infoArray = new Driver().getPropertyInfo(url, properties);
                                for (int i = 0; i < infoArray.length; i++) {
                                    DriverPropertyInfo info = infoArray[i];
                                    if (info.name.equals(messageKey)) {
                                        assertEquals(message, expected, info.value);
                                        found = true;
                                    }
                                }

                                if (!found) {
                                    fail("DriverPropertyInfo for '" + messageKey + "' not found!");
                                }
                            }
                            catch (SQLException e) {
                                throw new RuntimeException(e.getMessage());
                            }
                        }
                    }
            );
        }
    }


    /**
     * Creates tests for passing all variants of correct URLs.
     * {@link Driver#parseURL(String, Properties)}
     */
    public static void addParseURLCorrectTests(TestSuite suite) {

        final String[] SERVERTYPES = new String[] {
                "sqlserver", "sybase"
        };

        final String[] HOSTS = new String[] {
           "local", "local.com.aa", "100.2.3.400", "fc00::36A", "::ffff:192.168.0.1"
        };

        final String[] PORTS = new String[] {
                   null, "1433", "2332"
        };

        final String[] DATABASES = new String[] {
                  null, "instance", "my_name"
         };

        final String[][][] PROPERTIES = new String[][][] {
                   null,
                   { { "key", "value" } },
                   { { "key1", "value" } , { "ke_y2", "valu.e_|22" } },
                   { { "keyn", null } }
          };

        Map<String, Properties> urls = generateAllURLVariants(SERVERTYPES, HOSTS, PORTS, DATABASES, PROPERTIES);
        Properties prop = new Properties();

        for (Map.Entry<String, Properties> e : urls.entrySet()) {
            String url = e.getKey();
            Properties expectedProp = e.getValue();

            suite.addTest(new ParseURLTest(url, expectedProp));
        }
    }

    private static class ParseURLTest extends TestCase {
        private final String url;
        private final Properties expectedProp;

        public ParseURLTest(String url, Properties expectedProp) {
            super("testParseUrl \"" + url + "\"");
            this.url = url;
            this.expectedProp = expectedProp;
        }

        public void runTest() {
            testParseUrl();
        }

        public void testParseUrl() {
            Properties parsedProp = invokeDriverParseURL(url, new Properties());

            assertNotNull("URL '" + url + "' cannot be parsed", parsedProp);
            if (!expectedProp.equals(parsedProp)) {
                assertEquals("Result from URL '" + url + "' from Driver is not what expected", expectedProp, parsedProp);
            }
        }
    }

    private static Map<String, Properties> generateAllURLVariants(String[] serverTypes,
                String[] hosts, String[] ports, String[] databases,
                String[][][] properties)
    {
        Map<String, Properties> res = new HashMap<String, Properties>();

        for (int is = 0; is < serverTypes.length; is++) {
            String serverType = serverTypes[is];
            for (int ih = 0; ih < hosts.length; ih++) {
                String host = hosts[ih];
                for (int ip = 0; ip < ports.length; ip++) {
                    String port = ports[ip];
                    for (int id = 0; id < databases.length; id++) {
                        String database = databases[id];
                        for (int ir = 0; ir < properties.length; ir++) {
                            String[][] property = properties[ir];

                            StringBuilder url = new StringBuilder();
                            url.append("jdbc:jtds:").append(serverType);
                            url.append("://");
                            if (host.indexOf(':') >= 0) {
                                url.append('[').append(host).append(']');
                            } else {
                                url.append(host);
                            }


                            if (port != null) {
                                url.append(':').append(port);
                            }
                            if (database != null) {
                                url.append('/').append(database);
                            }

                            Properties p = new Properties();
                            p.put("SERVERTYPE", String.valueOf(DefaultProperties.getServerType(serverType)));
                            p.put("SERVERNAME", host);
                            if (port != null) {
                                p.put("PORTNUMBER", String.valueOf(Integer.parseInt(port)));
                            }
                            if (database != null) {
                                p.put("DATABASENAME", database);
                            }

                            for (int j = 0, n = (property == null ? 0 : property.length); j < n; j++) {
                                url.append(';').append(property[j][0]);
                                if (property[j][1] != null) {
                                    url.append('=').append(property[j][1]);
                                }

                                p.put(property[j][0].toUpperCase(), (property[j][1] == null ? "" : property[j][1]));
                            }

                            res.put(url.toString(), p);
                        }
                    }
                }
            }
        }

        return res;
    }

    private static Properties invokeDriverParseURL(String url, Properties prop) {
        return (Properties) invokeStaticMethod(
                    Driver.class, "parseURL",
                    new Class[]{String.class, Properties.class},
                    new Object[]{url, prop});
    }
}
