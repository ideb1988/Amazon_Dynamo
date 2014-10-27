package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

public class DataBase extends SQLiteOpenHelper
{
	// CLASS VARIABLES
	
	public static final int db_ver = 1;
	public static final String db_name = "simpleDynamo.db";
	public String db_table;
	Context context;

	// CONSTRUCTOR
	
	public DataBase(Context context , String uri)
	{
		super(context, db_name, null, db_ver);
		this.db_table = "[" + uri.toString() + "]";
		this.context = context;
	}
	
	// METHODS - INDRANIL
	
	// DATABASE - CREATE 
	
	public void onCreate(SQLiteDatabase mydb) 
	{
		mydb.execSQL(createTableStatement());
	}
	
	// DATABASE - UPGRADE
	
	public void onUpgrade(SQLiteDatabase mydb, int db_ver1, int db_ver2) 
	{
		// NOT USED IN THIS PROJECT
	}
	
	// DATABASE HELPER FUNCTIONS
	
	// CREATE TABLE STATEMENT
	
	private String createTableStatement()
	{
		String sql_Statement = "CREATE TABLE " + db_table 
				+ " (key TEXT PRIMARY KEY, value TEXT NOT NULL, type TEXT NOT NULL, avd TEXT NOT NULL);";
		return sql_Statement;
	}
}
