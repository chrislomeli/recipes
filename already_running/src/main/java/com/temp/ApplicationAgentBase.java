package com.temp;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TimeZone;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.ConfigurationException;
import org.joda.time.DateTimeZone;

public class ApplicationAgentBase extends ApplicationBase {

	static ProcessMonitor sds = null;
	
	/**
	 * 	
	 */
		public static void checkRunning() {
	
			int localPort=0;
			
			localPort= ApplicationContext.getIntegerValue("producer_port",  0);
			if (localPort == 0)
					return;
			
			ApplicationContext.addProperty("socket-lock", "true");
			Socket clientSocket = null;
	    try {
	        clientSocket = new Socket("localhost", localPort);
	        System.out.println(String.format("*** Start agent status = Already running at port %d. Exit gracefully *****",localPort));
	        Runtime.getRuntime().halt(0);
	        }
	      catch (Exception e) {
	    	  sds = new ProcessMonitor(localPort);
	    	  sds.start();
	      } finally {
	    	  if (clientSocket != null)
				try {
					clientSocket.close();
				} catch (IOException e) {
				}
	      }
		}
	
	/**
	 * 
	 */
	public static boolean verifyRuntime() {
		return true;
	}
	
	/**
	 * Main executable method - get Gnu command line arguments
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ConfigurationException 
	 * 
	 */
	public static Properties processCommandLine(final String[] commandLineArguments, ApplicationProperties.applicationClass appclass) throws IOException, ConfigurationException, InterruptedException {

		//
		DateTimeZone.setDefault(DateTimeZone.UTC);
		TimeZone.setDefault(TimeZone.getTimeZone("GMT"));		
		
		// command line
		final CommandLineParser cmdLineGnuParser = new GnuParser();
		final Options gnuOptions = getCommandLineOptions();
		CommandLine commandLine = null;

		try {
			System.out.println("parse command line..");
			commandLine = cmdLineGnuParser.parse(gnuOptions, commandLineArguments);
			
		} catch (ParseException e1) {
			System.err.println(e1);
			System.exit(1);
		}
		
		/*
		 */
		if (commandLineArguments != null && 
				commandLineArguments.length > 0 && commandLine.hasOption('h')) {
			printHelp(getCommandLineOptions(), 80, "USAGE", "End of Help", 5, 3,
					true, System.out);
			System.exit(1);
		}
		
		/* set configurable properties
		 * 
		 */
		String userPropertiesFile = null;
		String topic=null;
		String envString=null;
		ApplicationProperties.environmentClass environment = ApplicationProperties.environmentClass.PRODUCTION;
		boolean listProperties = false;
		boolean initialize = false;
		boolean run = true;
		Properties commandProperties = new Properties();
		
		if (commandLine.hasOption('l')) {
			listProperties = true;
		}
		if (commandLine.hasOption('p')) {
			userPropertiesFile = commandLine.getOptionValue("p");
		}
		if (commandLine.hasOption('t')) {
			topic = commandLine.getOptionValue("t");
			ApplicationProperties.CURRENT_TOPIC = topic;
		}
		if (commandLine.hasOption('e')) {
			envString = commandLine.getOptionValue("e");
			if (envString !=null && envString.toLowerCase().startsWith("dev"))
				environment = ApplicationProperties.environmentClass.DEVELOPMENT;
		}
		if (commandLine.hasOption('i')) {
			initialize = true;
		}
		if (commandLine.hasOption('v')) {
			System.out.println("version: "+ApplicationProperties.cerberus_version);
			Runtime.getRuntime().exit(0);
		}
		if (commandLine.hasOption('C')) {  // -C input_path=xyz,output_path=abc --no spaces
			String args = commandLine.getOptionValue("C");
			if (args!=null) {
				String[] nv = args.split(",");
				for (String nvpair : nv) {
					String[] map = nvpair.split("=");
					if (map.length == 2) {
						commandProperties.put(map[0].trim(), map[1].trim());
					}
				}
			}
			
		}

		
		if (topic == null ) {
			System.err.println(String.format("must specify a topic with --topic <topic>"));
			System.exit(1);		
		}

		
		/*
		 * Set properties from topic maps: overlapped by command file, or internal resource
		 */
		System.out.println("set properties into context...");
		Properties allProperties = setProperties(userPropertiesFile, topic, environment, appclass, commandProperties);
		if (allProperties == null) {
			System.err.println("no properties set.  Set properties by using an external file or an application.properties resource");
			System.exit(1);
		}

		String configtopic = ApplicationContext.getStringValue(ApplicationContext.TOPIC_FIELD);
		if (!topic.equalsIgnoreCase(topic)) {
			System.err.println(String.format("Topic misconfiguration %s is not equal to $%s", topic, configtopic));
			System.exit(1);
		}
		
		
		/*
		 * Add shutdown hook
		 * 
		 */
	    Runtime.getRuntime().addShutdownHook(new Thread()
	    {
	        @Override
	        public void run()
	        {
	            ApplicationContext.SHUTDOWN = true;
	            
	        }
	    });

    
		/*
		 * optionally dump to screen for troubleshooting
		 */
    	if (listProperties || initialize) {
    		StringJoiner sj = new StringJoiner(",\n");
    		for (Entry<Object, Object> s: allProperties.entrySet()) {
				sj.add(String.format("\"%s\" : \"%s\"", ""+s.getKey(), ""+s.getValue()));
			}
    		System.out.println(String.format("{\n%s\n}", sj.toString()));
			Runtime.getRuntime().exit(0);
		}

		/*
		 * Check running - must be run after properties are set
		 */
		checkRunning();
		
		if (!run) {
			Runtime.getRuntime().exit(0);
		}
		
		if (topic ==null || !ApplicationProperties.validProfileName(topic)) {
			System.err.println(String.format("%s is not a valid topic ...", topic));
			Set<String> topics = ApplicationProperties.validProfileNames();
			System.err.println("Valid topics are :");
			for (String top : topics) {
				System.err.println(top);
			}
			System.exit(1);		
		}
		return allProperties; // pass back all properties to guice?
	}

	
	/**
	 * Declare GNU-compatible Options.
	 * 
	 * @return Options expected from command-line of GNU form.
	 */

	public static Options getCommandLineOptions() {
		
		final Options gnuOptions = new Options();
		
		Option opt = new Option("p","properties",	true, "App Properties file for this  program");
		opt.setRequired(false);
		gnuOptions.addOption(opt);
		
		Option opt2 = new Option("t","topic",	true, "profile to use - named topic for backwards compat (e.g. 'procera') for assigning internal properties");
		opt2.setRequired(false);
		gnuOptions.addOption(opt2);

		Option opt3 = new Option("e","environment",	true, "environment ('dev' or 'prod') of this instance");
		opt3.setRequired(false);
		gnuOptions.addOption(opt3);

		Option opt4 = new Option("l","listproperties",	false, "print properties");
		opt4.setRequired(false);
		gnuOptions.addOption(opt4);
		
		Option opt5 = new Option("i","initialize",	false, "initialize properties");
		opt5.setRequired(false);
		gnuOptions.addOption(opt5);

		Option opt6 = new Option("v","version",	false, "version");
		opt6.setRequired(false);
		gnuOptions.addOption(opt6);
		
		Option opt7 = new Option("C","configs",	true, "insert configuration 'n1=v1,n2=v2' no spaces!");
		opt7.setRequired(false);
		gnuOptions.addOption(opt7);

		return gnuOptions;
	}

	
	/**
	 * Write "help" to the provided OutputStream.
	 */
	public static void printHelp(final Options options,
			final int printedRowWidth, final String header,
			final String footer, final int spacesBeforeOption,
			final int spacesBeforeOptionDescription,
			final boolean displayUsage, final OutputStream out) {
		
		final String commandLineSyntax = "java -cp ApacheCommonsCLI.jar";
		final PrintWriter writer = new PrintWriter(out);
		HelpFormatter helpFormatter = new HelpFormatter();

		helpFormatter.printHelp(writer, printedRowWidth, commandLineSyntax,
				header, options, spacesBeforeOption,
				spacesBeforeOptionDescription, footer, displayUsage);
		writer.flush();
	}
	


}