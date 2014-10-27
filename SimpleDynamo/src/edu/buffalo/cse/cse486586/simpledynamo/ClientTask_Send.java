package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;

import android.util.Log;

public class ClientTask_Send implements Runnable, Serializable
{
	// CLASS VARIABLES

	private static final long serialVersionUID = 1L;
	public HashMap<String, String> avdNPort= new HashMap<String, String>();
	String [] args;
	String sendTo;

	// CONSTRUCTOR

	public ClientTask_Send(String [] args, String sendTo)
	{
		// HARRDCODED 5 AVDS
		this.args = args;
		this.sendTo = sendTo;
		this.avdNPort.put("5554", "11108");
		this.avdNPort.put("5556", "11112");
		this.avdNPort.put("5558", "11116");
		this.avdNPort.put("5560", "11120");
		this.avdNPort.put("5562", "11124");
	}

	// ASYNC METHOD

	public void run()
	{
		String msgToSend = args[0] + "|" + args[1] + "|" + args[2] + "|" + args[3] + "|" + args[4];
		
		try
		{
			Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					Integer.parseInt(avdNPort.get(sendTo)));
			ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
			out.writeObject(msgToSend);
			out.flush();
			out.close();
			socket.close();
		}
		catch (UnknownHostException e) 
		{
			Log.e("CLIENT_SEND", "UnknownHostException");
		} 
		catch (IOException e) 
		{
			Log.e("CLIENT_SEND", "socket IOException");
		}
	}
}