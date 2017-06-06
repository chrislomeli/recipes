package com.temp;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class ProcessMonitor extends Thread {
	   // you may need to customize this for your machine
	   public static int PORT = 0 ; 

	   ServerSocket serverSocket = null;
	   Socket clientSocket = null;

	   public ProcessMonitor(int port) {
		   PORT = port;
	   }
	   
	   
	   public void run() {
		   
		  if (PORT == 0) {
			  System.err.println("listener port is zero -- not configured properly - exiting socket listener");
			  return;
		  }
	    try {
	      // Create the server socket
	      serverSocket = new ServerSocket(PORT, 1);
	      while (true) {
	       clientSocket = serverSocket.accept();
	       clientSocket.close();
	       }
	    }
	    catch (IOException ioe) {
	     System.out.println("Error in ProcessMonitor: " + ioe);
	     }
	    }
	  }