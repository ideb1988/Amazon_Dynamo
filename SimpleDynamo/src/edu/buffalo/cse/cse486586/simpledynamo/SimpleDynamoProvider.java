package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider 
{
	// CLASS VARIABLES

	public static TreeMap <String, String> nodes = new TreeMap <String, String>();
	public static TreeMap <String, String> sNodes = new TreeMap <String, String>();
	public static TreeMap <String, String> pNodes = new TreeMap <String, String>();
	public static TreeMap <String, String> portNAvd = new TreeMap <String, String>();

	public static final String ANDROID_AUTHORITY = "edu.buffalo.cse.cse486586.simpledynamo.provider";
	public static final String ANDROID_SCHEME = "content";
	public static final Integer SERVER_PORT = 10000;

	DataBase mydb;
	SQLiteDatabase database;

	public static int noOfNodes = 0;
	public static String sNode;
	public static String pNode;
	public static String node;
	public static Cursor finalCursor = null;

	public static Boolean joining = false;
	public static Boolean c_inserting = false;
	public static Boolean inserting = false;
	public static Boolean dup_inserting = false;
	public static Boolean dup2_inserting = false;
	public static Boolean c_querying = false;
	public static Boolean querying = false;

	public static Boolean lock = false;
	public static Boolean insertInQueue = false;
	public static Boolean queryInQueue = false;
	public static Boolean stop_querying_1 = false;
	public static Boolean stop_querying_2 = false;
	public static Boolean stop_querying_3 = false;
	public static Boolean stop_querying_4 = false;
	public static Boolean stop_querying_5 = false;

	public static Boolean stop = false;

	public static Double JOIN_TIME_OUT = 3.00;		// IN SECONDS
	public static Double QUERY_TIME_OUT = 0.5;		// IN SECONDS


	public boolean onCreate() 																// START
	{
		mydb = new DataBase(getContext() , buildUri(ANDROID_SCHEME, ANDROID_AUTHORITY).toString());
		database = mydb.getWritableDatabase();
		setup5AVDS();
		setupThisAVD();
		startServer();
		stopAllWork();
		join();
		restart();
		return false;
	}


	public Uri insert(Uri uri, ContentValues values) 										// INSERT REQUEST
	{
		insertInQueue = true;
		while (c_inserting){} 
		insertInQueue = false;
		c_inserting = true;

		stopQuery();

		String key = (String) values.get("key");
		String value = (String) values.get("value");
		String hashedKey = "";
		try {hashedKey = genHash(key);} 
		catch (NoSuchAlgorithmException e) {}

		Log.v("INSERT", key);

		String coOrdinator = findCoOrdnator(hashedKey);

		if (coOrdinator.equals(node))
		{
			ContentValues newValues = new ContentValues();
			newValues.put("key", key);
			newValues.put("value", value);
			newValues.put("type", "ORIGINAL");
			newValues.put("avd", SimpleDynamoProvider.node);

			database.replace("["+uri.toString()+"]", null, newValues);
		}
		else
		{
			sender(makeStringArray("INSERT", key+":"+value, node, "null", "null"), coOrdinator);
		}

		sender(makeStringArray("DUPLICATE_INSERT_S1", key+":"+value, node, coOrdinator, "null"), sNodes.get(coOrdinator));
		sender(makeStringArray("DUPLICATE_INSERT_S2", key+":"+value, node, coOrdinator, "null"), sNodes.get(sNodes.get(coOrdinator)));

		startQuery();

		c_inserting = false;
		return null;
	}


	public Cursor query(Uri uri, String[] projection, 
			String selection, String[] selectionArgs, String sortOrder) 						// QUERY REQUEST
	{
		while (c_inserting || c_querying || stop_querying_1 
				|| stop_querying_2 || stop_querying_3 || stop_querying_4 || stop_querying_5){}
		while (c_querying){}
		c_querying = true;

		Cursor query = null;

		if (selection.equals("*"))
		{
			String allKeyValue = "";
			for(Map.Entry<String,String> entry : nodes.entrySet())
			{
				String sendToNode = entry.getValue();
				finalCursor = null;
				sender(makeStringArray("QUERY_STAR", node, "null", "null", "null"), sendToNode);
				querying = true;
				long start = System.nanoTime();
				while (querying)
				{
					long end = System.nanoTime();
					if ((end - start)/1e9 > QUERY_TIME_OUT){break;}
				}
				if (finalCursor != null)
				{
					allKeyValue = allKeyValue + (makeString(finalCursor));
				}
			}
			query = makeCursor(allKeyValue);
		}
		else if (selection.equals("@"))
		{
			query = database.rawQuery("SELECT key, value " + "FROM [" +uri.toString()+"] ", null);
		}
		else 
		{
			String coOrdinator = "";
			try{coOrdinator = findCoOrdnator(genHash(selection));} 
			catch (NoSuchAlgorithmException e) {}

			Log.v("QUERY", selection + " " + coOrdinator);

			if (coOrdinator.equals(node))
			{
				query = database.rawQuery("SELECT key, value " +	"FROM [" +uri.toString()+"] " +	"WHERE key = '" + selection +"'", null);
				c_querying = false;
				return query;
			}

			finalCursor = null;
			sender(makeStringArray("QUERY_KEY", selection, node, "null", "null"), coOrdinator);	
			querying = true;
			long start = System.nanoTime();
			while (querying)
			{
				long end = System.nanoTime();
				if ((end - start)/1e9 > QUERY_TIME_OUT){break;}
			}
			if (querying || finalCursor == null || makeString(finalCursor).contains(":null:"))
			{
				Log.v("QUERY", selection + " " + sNodes.get(coOrdinator));

				finalCursor = null;
				sender(makeStringArray("QUERY_KEY", selection, node, "null", "null"), sNodes.get(coOrdinator));	
				querying = true;
				start = System.nanoTime();
				while (querying)
				{
					long end = System.nanoTime();
					if ((end - start)/1e9 > QUERY_TIME_OUT){break;}
				}
				if (querying || finalCursor == null || makeString(finalCursor).contains(":null:"))
				{
					Log.v("QUERY", selection + " " + sNodes.get(sNodes.get(coOrdinator)));

					finalCursor = null;
					sender(makeStringArray("QUERY_KEY", selection, node, "null", "null"), sNodes.get(sNodes.get(coOrdinator)));	
					querying = true;
					start = System.nanoTime();
					while (querying)
					{
						long end = System.nanoTime();
						if ((end - start)/1e9 > QUERY_TIME_OUT){break;}
					}
					if (!querying)
					{
						// FAILURE HANDLING
						sender(makeStringArray("INSERT", makeString(finalCursor), node, "null", "null"), coOrdinator);
						sender(makeStringArray("DUPLICATE_INSERT_S1", makeString(finalCursor), node, coOrdinator, "null"), sNodes.get(coOrdinator));
					}
					query = finalCursor;
				}
				else
				{
					// FAILURE HANDLING

					sender(makeStringArray("INSERT", makeString(finalCursor), node, "null", "null"), coOrdinator);
					sender(makeStringArray("DUPLICATE_INSERT_S2", makeString(finalCursor), node, coOrdinator, "null"), sNodes.get(sNodes.get(coOrdinator)));

					query = finalCursor;
				}
			}
			else
			{
				query = finalCursor;
			}
		}

		c_querying = false;
		return query;
	}


	public int delete(Uri uri, String selection, String[] selectionArgs) 					// DELETE REQUEST		
	{
		if (selection.equals("*"))
		{
			for(Map.Entry<String,String> entry : nodes.entrySet())
			{
				String sendToNode = entry.getValue();
				sender(makeStringArray("DELETE_STAR", node, "null", "null", "null"), sendToNode);
			}
		}
		else if (selection.equals("@"))
		{
			DataBase mydb = new DataBase(getContext() , uri.toString());
			SQLiteDatabase database = mydb.getWritableDatabase();
			database.delete("[" +uri.toString()+"]", null, null);
		}
		else
		{
			try
			{
				String coOrdinator = findCoOrdnator(genHash(selection));
				sender(makeStringArray("DELETE_KEY", selection, "null", "null", "null"), coOrdinator);
				sender(makeStringArray("DELETE_KEY", selection, "null", "null", "null"), sNodes.get(coOrdinator));
				sender(makeStringArray("DELETE_KEY", selection, "null", "null", "null"), sNodes.get(sNodes.get(coOrdinator)));
			}
			catch (NoSuchAlgorithmException e){}
		}
		return 0;
	}



	// HELPER METHODS

	public MatrixCursor makeCursor(String string)											// CONVERT STRING TO CURSOR
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

	public String makeString(Cursor query)													// CONVERT CURSOR TO STRING
	{
		String cursorToString = "";

		if (query.getCount() == 0)
		{
			return cursorToString;
		}

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

	private String genHash(String input) throws NoSuchAlgorithmException            		// SHA-1 HASH CREATOR
	{
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) 
		{
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

	public void setup5AVDS()																// SETUP DATA FOR 5 AVDS 
	{
		try
		{
			nodes.put(genHash("5554"), "5554");
			nodes.put(genHash("5556"), "5556");
			nodes.put(genHash("5558"), "5558");
			nodes.put(genHash("5560"), "5560");
			nodes.put(genHash("5562"), "5562");

			sNodes.put("5554", "5558");
			sNodes.put("5556", "5554");
			sNodes.put("5558", "5560");
			sNodes.put("5560", "5562");
			sNodes.put("5562", "5556");

			pNodes.put("5554", "5556");
			pNodes.put("5556", "5562");
			pNodes.put("5558", "5554");
			pNodes.put("5560", "5558");
			pNodes.put("5562", "5560");

			portNAvd.put("11108", "5554");
			portNAvd.put("11112", "5556");
			portNAvd.put("11116", "5558");
			portNAvd.put("11120", "5560");
			portNAvd.put("11124", "5562");

		}
		catch (NoSuchAlgorithmException e)
		{
			Log.e("setup5AVDS", "NoSuchAlgorithmException");
		}
	}

	public void setupThisAVD()																// DATA FOR THIS AVD
	{
		node = portNAvd.get(findMyPort());
		sNode = sNodes.get(node);
		pNode = pNodes.get(node);
		noOfNodes = nodes.size();
	}

	public String findMyPort()																// FIND PORT FOR THIS AVD
	{
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		return myPort;
	}

	private Uri buildUri(String scheme, String authority) 									// URI BUILDER
	{
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	public void startServer()																// STARTING SERVER
	{
		ServerTask serverTask;
		try 
		{
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			serverTask = new ServerTask(findMyPort(), buildUri(ANDROID_SCHEME, ANDROID_AUTHORITY), serverSocket, database);
			Thread server = new Thread(serverTask, "SERVERTHREAD");		
			server.start();
		} 
		catch (IOException e) 
		{
			Log.e("ServerSocket", "Can't create a ServerSocket");
		}
	}

	public void join()																		// AVD JOINS OR REJOINS
	{
		// delete(buildUri(ANDROID_SCHEME, ANDROID_AUTHORITY), "@", null);

		// GET MY DATA AFTER FAILURE (SUCCESSOR OR SUCCESSOR'S SUCCESSOR)

		sender_join(makeStringArray("GIVE_ME_MY_DATA", node, "null", "null", "null"), sNodes.get(node));
		joining = true;
		long start = System.nanoTime();
		while (joining)
		{
			long end = System.nanoTime();
			if ((end - start)/1e9 > JOIN_TIME_OUT){break;}
		}
		if (joining)
		{
			sender_join(makeStringArray("GIVE_ME_MY_DATA", node, "null", "null", "null"), sNodes.get(sNodes.get(node)));
			joining = true;
			start = System.nanoTime();
			while (joining)
			{
				long end = System.nanoTime();
				if ((end - start)/1e9 > JOIN_TIME_OUT){break;}
			}
		}

		// GET PREDECESSORS DATA (FROM PREDECESSOR OR SUCCESSOR)

		sender_join(makeStringArray("ASK_FOR_PREDECESSOR_DATA", node, pNodes.get(node), "null", "null"), pNodes.get(node));
		joining = true;
		start = System.nanoTime();
		while (joining)
		{
			long end = System.nanoTime();
			if ((end - start)/1e9 > JOIN_TIME_OUT){break;}
		}
		if (joining)
		{
			sender_join(makeStringArray("ASK_FOR_PREDECESSOR_DATA", node, pNodes.get(node), "null", "null"), sNodes.get(node));
			joining = true;
			start = System.nanoTime();
			while (joining)
			{
				long end = System.nanoTime();
				if ((end - start)/1e9 > JOIN_TIME_OUT){break;}
			}
		}


		// GET PREDECESSOR'S PREDECESSOR'S DATA (FROM PREDECESSOR OR PRED PRED)

		sender_join(makeStringArray("ASK_FOR_P_PREDECESSOR_DATA", node, pNodes.get(pNodes.get(node)), "null", "null"), pNodes.get(pNodes.get(node)));
		joining = true;
		start = System.nanoTime();
		while (joining)
		{
			long end = System.nanoTime();
			if ((end - start)/1e9 > JOIN_TIME_OUT){break;}
		}
		if (joining)
		{
			sender_join(makeStringArray("ASK_FOR_P_PREDECESSOR_DATA", node, pNodes.get(pNodes.get(node)), "null", "null"), pNodes.get(node));
			joining = true;
			start = System.nanoTime();
			while (joining)
			{
				long end = System.nanoTime();
				if ((end - start)/1e9 > JOIN_TIME_OUT){break;}
			}
		}

	}

	public String[] makeStringArray(String s0, 
			String s1, String s2, String s3, String s4)										// STRING[] BUILDER
	{
		String[] stringArray = new String[5];
		stringArray[0] = s0;
		stringArray[1] = s1;
		stringArray[2] = s2;
		stringArray[3] = s3;
		stringArray[4] = s4;
		return stringArray;
	}

	public void sender(String [] stringArray, String sendTo)								// SEND REQUEST TO OTHER AVD
	{
		while (stop)
		{}
		ClientTask_Send clientTask = new ClientTask_Send(stringArray, sendTo);
		Thread client = new Thread(clientTask, stringArray[0] + Math.random()*200);		
		client.start();
	}

	public void sender_join(String [] stringArray, String sendTo)							// SEND REQUEST TO OTHER AVD
	{
		ClientTask_Send clientTask = new ClientTask_Send(stringArray, sendTo);
		Thread client = new Thread(clientTask, stringArray[0] + Math.random()*200);		
		client.start();
	}

	public String findCoOrdnator(String hashedKey)											// FIND CO-ORDINATOR
	{
		String nodeToSend = "";
		try
		{
			for(Map.Entry<String,String> entry : nodes.entrySet())
			{
				String nodeHash = entry.getKey();
				String pNodeHash = genHash(pNodes.get(entry.getValue()));

				if ((hashedKey.compareTo(pNodeHash) > 0) &&
						(hashedKey.compareTo(nodeHash) <= 0) &&
						(nodeHash.compareTo(pNodeHash) > 0))
				{
					nodeToSend = entry.getValue();
					break;
				}
				else if (((hashedKey.compareTo(pNodeHash) > 0) &&
						(hashedKey.compareTo(nodeHash) >= 0) &&
						(pNodeHash.compareTo(nodeHash) > 0)) ||
						((hashedKey.compareTo(pNodeHash) < 0) &&
								(hashedKey.compareTo(nodeHash) <= 0) &&
								(pNodeHash.compareTo(nodeHash) > 0)))
				{
					nodeToSend = entry.getValue();
					break;
				}
			}
		}
		catch (NoSuchAlgorithmException e)
		{
			Log.v("findCoOrdnator", "NoSuchAlgorithmException");
		}
		return nodeToSend;
	}

	public void stopQuery()																	// SIGNAL QUERY STOP
	{
		if (findMyPort().equals("11108"))
		{				
			for(Map.Entry<String,String> entry : nodes.entrySet())
			{
				sender(makeStringArray("STOP_QUERY_1", "null", "null", "null", "null"), entry.getValue());
			}
		}
		if (findMyPort().equals("11112"))
		{				
			for(Map.Entry<String,String> entry : nodes.entrySet())
			{
				sender(makeStringArray("STOP_QUERY_2", "null", "null", "null", "null"), entry.getValue());
			}
		}
		if (findMyPort().equals("11116"))
		{				
			for(Map.Entry<String,String> entry : nodes.entrySet())
			{
				sender(makeStringArray("STOP_QUERY_3", "null", "null", "null", "null"), entry.getValue());
			}
		}
		if (findMyPort().equals("11120"))
		{				
			for(Map.Entry<String,String> entry : nodes.entrySet())
			{
				sender(makeStringArray("STOP_QUERY_4", "null", "null", "null", "null"), entry.getValue());
			}
		}
		if (findMyPort().equals("11124"))
		{				
			for(Map.Entry<String,String> entry : nodes.entrySet())
			{
				sender(makeStringArray("STOP_QUERY_5", "null", "null", "null", "null"), entry.getValue());
			}
		}
	}

	public void startQuery()																// SIGNAL QUERY START
	{
		if (! insertInQueue)
		{
			if (findMyPort().equals("11108"))
			{				
				for(Map.Entry<String,String> entry : nodes.entrySet())
				{
					sender(makeStringArray("LET_QUERY_1", "null", "null", "null", "null"), entry.getValue());
				}
			}
			if (findMyPort().equals("11112"))
			{				
				for(Map.Entry<String,String> entry : nodes.entrySet())
				{
					sender(makeStringArray("LET_QUERY_2", "null", "null", "null", "null"), entry.getValue());
				}
			}
			if (findMyPort().equals("11116"))
			{				
				for(Map.Entry<String,String> entry : nodes.entrySet())
				{
					sender(makeStringArray("LET_QUERY_3", "null", "null", "null", "null"), entry.getValue());
				}
			}
			if (findMyPort().equals("11120"))
			{				
				for(Map.Entry<String,String> entry : nodes.entrySet())
				{
					sender(makeStringArray("LET_QUERY_4", "null", "null", "null", "null"), entry.getValue());
				}
			}
			if (findMyPort().equals("11124"))
			{				
				for(Map.Entry<String,String> entry : nodes.entrySet())
				{
					sender(makeStringArray("LET_QUERY_5", "null", "null", "null", "null"), entry.getValue());
				}
			}
		}
	}

	public void stopAllWork()																// SIGNAL TO STOP EVERTHING
	{
		for(Map.Entry<String,String> entry : nodes.entrySet())
		{
			sender_join(makeStringArray("STOP", "null", "null", "null", "null"), entry.getValue());
		}
	}

	public void restart()																	// SIGNAL TO RESTART
	{
		for(Map.Entry<String,String> entry : nodes.entrySet())
		{
			sender_join(makeStringArray("RESTART", "null", "null", "null", "null"), entry.getValue());
		}
	}


	// INNER CLASS FOR RECEIVING ON SERVERSOCKET - INDRANIL

	public class ServerTask implements Runnable, Serializable   							// SERVER
	{
		// CLASS VARIABLES

		private static final long serialVersionUID = 1L;
		HashMap<String, String> portNAvd= new HashMap<String, String>();
		String myPort;
		Uri uri;
		ServerSocket sockets;
		SQLiteDatabase database;

		// CONSTRUCTOR

		public ServerTask(String myPort, Uri uri, ServerSocket sockets, SQLiteDatabase database)
		{
			this.portNAvd.put("11108", "5554");
			this.portNAvd.put("11112", "5556");
			this.portNAvd.put("11116", "5558");
			this.portNAvd.put("11120", "5560");
			this.portNAvd.put("11124", "5562");
			this.uri = uri;
			this.myPort = myPort;
			this.sockets = sockets;
			this.database = database;
		}

		// SERVER LISTENER

		public void run()
		{
			try
			{
				ServerSocket serverSocket = sockets;
				while (true)
				{        	
					Socket accept = serverSocket.accept();
					ObjectInputStream is = new ObjectInputStream(accept.getInputStream());
					Object obj;
					try 
					{
						obj = is.readObject();
					}
					catch (ClassNotFoundException e)
					{
						obj = null;
						Log.e("ServerTask/Sockets", "ClassNotFound");
					}
					String recvdMsg = (String) obj;

					// STARTING THREAD TO TAKE CARE OF RECEIVED REQUEST

					ServerTask_Receive server = new ServerTask_Receive(recvdMsg, uri, getContext(), database);
					Thread spawn = new Thread(server, "Thread" + Math.random()*200);
					spawn.start();
				}
			}
			catch (IOException e)
			{
				Log.e("ServerSocket", "Accept Failed");
			}
		}
	}

	// UNUSED METHODS

	public String getType(Uri uri) 
	{return null;}

	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) 
	{return 0;}
}
