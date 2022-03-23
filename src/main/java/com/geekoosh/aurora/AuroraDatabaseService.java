package com.geekoosh.aurora;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import com.geekoosh.flyway.request.EnvironmentVars;
import com.geekoosh.flyway.request.SecretVars;
import com.geekoosh.flyway.request.SystemEnvironment;
import com.geekoosh.flyway.request.ValueManager;

import org.json.JSONObject;

public class AuroraDatabaseService 
{
  private String DATABASE_EXISTS_QUERY = "SELECT EXISTS(SELECT datname FROM pg_catalog.pg_database WHERE datname = '%s')";
  private SystemEnvironment system;

  public AuroraDatabaseService()
  {
    system = new SystemEnvironment();
  }

  public void Initialize()
  {
    String dbSecret = system.getEnv(SecretVars.DB_SECRET);

    if(dbSecret != null) 
    {
      JSONObject json = ValueManager.latestSecretJson(dbSecret);
      String username = json.get("username").toString();
      String password = json.get("password").toString();
      String host = json.get("host").toString();
      Integer port = Integer.parseInt(json.get("port").toString());

      String jdbc = "jdbc:postgresql://%s:%s/postgres?useSSL=true";
      String database = system.getEnv(EnvironmentVars.pg_database);
      String connectionString = String.format(jdbc, host, port);
      String sql = String.format(DATABASE_EXISTS_QUERY, database);

      Boolean exists = checkExists(connectionString, username, password, sql);

      if (exists)
      {
        System.out.printf("Database %s exists. %n", database);
      }
      else
      {
        System.out.printf("Database %s does not exist. Attempting to create... %n", database);

        createDatabase(connectionString, username, password, database);

        System.out.printf("Creation complete. %n");
      }
    }
    else
    {
      System.out.printf("Secret not found.");
    }
  }

  private Boolean checkExists(String connectionString, String username, String password, String sql)
  {
    Boolean exists = false;

    try
    {
      Connection conn = DriverManager.getConnection(connectionString, username, password);

      Statement statement = conn.createStatement();

      ResultSet result = statement.executeQuery(sql);

      while ( result.next() ) 
      {
         exists = result.getBoolean("exists");
      }

      result.close();

      statement.close();

      conn.close();
    }
    catch (Exception ex)
    {
      System.out.printf("Error! Message: %s %n", ex.getMessage());
    }

    return exists;
  }

  private void createDatabase(String connectionString, String username, String password, String database)
  {
    try
    {
      Connection conn = DriverManager.getConnection(connectionString, username, password);

      Statement statement = conn.createStatement();
      String sql = String.format("CREATE DATABASE %s", database);

      statement.execute(sql);

      statement.close();

      conn.close();
    }
    catch (Exception ex)
    {
      System.out.printf("Error! Message: %s %n", ex.getMessage());
    }
  }
}
