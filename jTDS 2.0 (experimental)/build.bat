@echo off

if "%JAVA_HOME%" == "" goto error

echo.
echo Building jTDS...
echo ----------------

set LOCALCLASSPATH=%JAVA_HOME%\lib\tools.jar
for %%i in (lib\*.jar) do call :cp "%%i"

echo.
echo Building with classpath %LOCALCLASSPATH%

echo.
echo Starting Ant...

"%JAVA_HOME%\bin\java.exe" -Xmx512M -Dant.home="." -classpath "%LOCALCLASSPATH%" org.apache.tools.ant.Main %1 %2 %3 %4 %5

goto end

:error

echo "ERROR: JAVA_HOME not found in your environment."
echo.
echo "Please, set the JAVA_HOME variable in your environment to match the"
echo "location of the Java Virtual Machine you want to use."

:end

set LOCALCLASSPATH=
set ANT_HOME=
goto:EOF

:cp
set LOCALCLASSPATH=%LOCALCLASSPATH%;%1
goto:EOF