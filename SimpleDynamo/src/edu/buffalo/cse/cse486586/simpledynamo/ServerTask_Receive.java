package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.util.Log;


public class ServerTask_Receive implements Runnable
{
	// CLASS VARIABLES

	public String recvdMsg;
	public Uri uri;
	public Context _context;
	public SQLiteDatabase database;

	// CONSTRUCTOR

	public ServerTask_Receive(String recvdMsg, Uri uri, Context _context, SQLiteDatabase database)
	{
		this.uri = uri;
		this.recvdMsg = recvdMsg;
		this._context = _context;
		this.database = database;
	}

	// ASYNCH THREAD RUN

	public void run() 
	{
		String[] args = recvdMsg.split("\\|");

		if (args[0].equals("GIVE_ME_MY_DATA"))
		{
			Cursor query = database.rawQuery("SELECT key, value "
					+ "FROM [" +uri.toString()+"] WHERE avd = '" + args[1] + "'", null);

			String cursor = makeString(query);
			sender_join(makeStringArray("SUCCESOR_RETURNED_DATA", cursor, "null", "null", "null"), args[1]);
			cursor = null;
		}


		if (args[0].equals("SUCCESOR_RETURNED_DATA"))
		{
			try
			{
				String[] keyVal = args[1].split("\\:");
				for (int i = 0 ; i < keyVal.length ; i=i+2)
				{
					String key = keyVal[i];
					String value = keyVal[i+1];
					ContentValues values = new ContentValues();
					values.put("key", key);
					values.put("value", value);
					values.put("type", "ORIGINAL");
					values.put("avd", SimpleDynamoProvider.node);

					database.replace("["+uri.toString()+"]", null, values);
				}
				keyVal = null;
				SimpleDynamoProvider.joining = false;
			}
			catch (Exception e)
			{
				SimpleDynamoProvider.joining = false;
			}
		}


		if (args[0].equals("ASK_FOR_PREDECESSOR_DATA"))
		{
			Cursor query = database.rawQuery("SELECT key, value "
					+ "FROM [" +uri.toString()+"] WHERE avd = '" + args[2] + "'", null);

			String cursor = makeString(query);
			sender_join(makeStringArray("PRED_RETURNED_DATA", cursor, args[2], "null", "null"), args[1]);
			cursor = null;
		}


		if (args[0].equals("PRED_RETURNED_DATA"))
		{
			try
			{
				String[] keyVal = args[1].split("\\:");
				for (int i = 0 ; i < keyVal.length ; i=i+2)
				{
					String key = keyVal[i];
					String value = keyVal[i+1];
					ContentValues values = new ContentValues();
					values.put("key", key);
					values.put("value", value);
					values.put("type", "DUPLICATE");
					values.put("avd", args[2]);

					database.replace("["+uri.toString()+"]", null, values);
				}
				keyVal = null;
				SimpleDynamoProvider.joining = false;
			}
			catch (Exception e)
			{
				SimpleDynamoProvider.joining = false;
			}
		}


		if (args[0].equals("ASK_FOR_P_PREDECESSOR_DATA"))
		{
			Cursor query = database.rawQuery("SELECT key, value "
					+ "FROM [" +uri.toString()+"] WHERE avd = '" + args[2] + "'", null);

			String cursor = makeString(query);
			sender_join(makeStringArray("P_PRED_RETURNED_DATA", cursor, args[2], "null", "null"), args[1]);
			cursor = null;
		}

		if (args[0].equals("P_PRED_RETURNED_DATA"))
		{
			try
			{
				String[] keyVal = args[1].split("\\:");
				for (int i = 0 ; i < keyVal.length ; i=i+2)
				{
					String key = keyVal[i];
					String value = keyVal[i+1];
					ContentValues values = new ContentValues();
					values.put("key", key);
					values.put("value", value);
					values.put("type", "DUPLICATE");
					values.put("avd", args[2]);

					database.replace("["+uri.toString()+"]", null, values);
				}
				keyVal = null;
				SimpleDynamoProvider.joining = false;
			}
			catch (Exception e)
			{
				SimpleDynamoProvider.joining = false;
			}
		}

		if (args[0].equals("INSERT"))
		{
			String key = args[1].split("\\:")[0];
			String value = args[1].split("\\:")[1];
			ContentValues values = new ContentValues();
			values.put("key", key);
			values.put("value", value);
			values.put("type", "ORIGINAL");
			values.put("avd", SimpleDynamoProvider.node);
			
			database.replace("["+uri.toString()+"]", null, values);
		}

		if (args[0].equals("DUPLICATE_INSERT_S1"))
		{
			String key = args[1].split("\\:")[0];
			String value = args[1].split("\\:")[1];
			ContentValues values = new ContentValues();
			values.put("key", key);
			values.put("value", value);
			values.put("type", "DUPLICATE");
			values.put("avd", args[3]);
			
			database.replace("["+uri.toString()+"]", null, values);
		}

		if (args[0].equals("DUPLICATE_INSERT_S2"))
		{
			String key = args[1].split("\\:")[0];
			String value = args[1].split("\\:")[1];
			ContentValues values = new ContentValues();
			values.put("key", key);
			values.put("value", value);
			values.put("type", "DUPLICATE");
			values.put("avd", args[3]);
			
			database.replace("["+uri.toString()+"]", null, values);
		}

		if (args[0].equals("REL_INSERT_LOCK"))
		{
			SimpleDynamoProvider.inserting = false;
		}

		if (args[0].equals("REL_DUP1_INSERT_LOCK"))
		{
			SimpleDynamoProvider.dup_inserting = false;
		}

		if (args[0].equals("REL_DUP2_INSERT_LOCK"))
		{
			SimpleDynamoProvider.dup2_inserting = false;
		}

		if (args[0].equals("QUERY_STAR"))
		{
			String askingNode = args[1];
			Cursor query = database.rawQuery("SELECT key, value " +	"FROM [" +uri.toString()+"]", null);
			String cursor = makeString(query);
			sender(makeStringArray("QUERY_RESULT", cursor, "null", "null", "null"), askingNode);
			cursor = null;
		}

		if (args[0].equals("QUERY_KEY"))
		{
			Cursor query;
			query = database.rawQuery("SELECT key, value " +	"FROM [" +uri.toString()+"] " +	"WHERE key = '" + args[1] +"'", null);
			String cursor = makeString(query);
			sender(makeStringArray("QUERY_RESULT", cursor, "null", "null", "null"), args[2]);
			cursor = null;
			Log.v("QUERY REPLY", "SENT");
		}

		if (args[0].equals("QUERY_RESULT"))
		{
			SimpleDynamoProvider.finalCursor = makeCursor(args[1]);
			args[1] = null;
			SimpleDynamoProvider.querying = false;
		}

		if (args[0].equals("DELETE_STAR"))
		{
			database.delete("[" +uri.toString()+"]", null, null);
		}

		if (args[0].equals("DELETE_KEY"))
		{
			database.delete("[" +uri.toString()+"]", "key = '" + args[1] + "'", null);
		}

		if (args[0].equals("STOP_QUERY_1"))
		{
			SimpleDynamoProvider.stop_querying_1 = true;
		}
		if (args[0].equals("STOP_QUERY_2"))
		{
			SimpleDynamoProvider.stop_querying_2 = true;
		}
		if (args[0].equals("STOP_QUERY_3"))
		{
			SimpleDynamoProvider.stop_querying_3 = true;
		}
		if (args[0].equals("STOP_QUERY_4"))
		{
			SimpleDynamoProvider.stop_querying_4 = true;
		}
		if (args[0].equals("STOP_QUERY_5"))
		{
			SimpleDynamoProvider.stop_querying_5 = true;
		}
		if (args[0].equals("LET_QUERY_1"))
		{
			SimpleDynamoProvider.stop_querying_1 = false;
		}
		if (args[0].equals("LET_QUERY_2"))
		{
			SimpleDynamoProvider.stop_querying_2 = false;
		}
		if (args[0].equals("LET_QUERY_3"))
		{
			SimpleDynamoProvider.stop_querying_3 = false;
		}
		if (args[0].equals("LET_QUERY_4"))
		{
			SimpleDynamoProvider.stop_querying_4 = false;
		}
		if (args[0].equals("LET_QUERY_5"))
		{
			SimpleDynamoProvider.stop_querying_5 = false;
		}
		if (args[0].equals("STOP"))
		{
			SimpleDynamoProvider.stop = true;
		}
		if (args[0].equals("RESTART"))
		{
			SimpleDynamoProvider.stop = false;
		}
		
		recvdMsg = null;
		args = null;
	}

	// HELPER METHODS

	public MatrixCursor makeCursor(String string)									// CONVERT STRING TO CURSOR
	{
		MatrixCursor cursor = new MatrixCursor(new String[]{"key","value"});
		try
		{
			String[] msg = string.split("\\:");
			for (int i = 0; i < msg.length; i=i+2)
			{
				cursor.newRow().add(msg[i]).add(msg[i+1]);
			}
			return cursor;
		}
		catch (Exception e)
		{
			return cursor;
		}
	}

	public String makeString(Cursor query)											// MAKE CURSOR TO STRING
	{
		String cursorToString = "";
		if (query.moveToFirst()) 
		{
			while (!query.isAfterLast()) 
			{
				int keyIndex = query.getColumnIndex("key");
				int valueIndex = query.getColumnIndex("value");
				String returnKey = query.getString(keyIndex);
				String returnValue = query.getString(valueIndex);
				cursorToString = cursorToString + returnKey + ":" + returnValue + ":";
				query.moveToNext();
			}
		}
		return cursorToString;
	}

	public String[] makeStringArray(String s0, 
			String s1, String s2, String s3, String s4)								// STRING[] BUILDER
	{
		String[] stringArray = new String[5];
		stringArray[0] = s0;
		stringArray[1] = s1;
		stringArray[2] = s2;
		stringArray[3] = s3;
		stringArray[4] = s4;
		return stringArray;
	}

	public void sender(String [] stringArray, String sendTo)						// SEND REQUEST TO OTHER AVD
	{
		while (SimpleDynamoProvider.stop)
		{}
		ClientTask_Send clientTask = new ClientTask_Send(stringArray, sendTo);
		Thread client = new Thread(clientTask, stringArray[0] + Math.random()*200);		
		client.start();
	}
	
	public void sender_join(String [] stringArray, String sendTo)					// SEND REQUEST TO OTHER AVD
	{
		ClientTask_Send clientTask = new ClientTask_Send(stringArray, sendTo);
		Thread client = new Thread(clientTask, stringArray[0] + Math.random()*200);		
		client.start();
	}
}
