package temp;

import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;


public class FileCrawler  {

	static Map<Integer,String> actions = new HashMap<>();
	static {
		actions.put(1, "celebrate");
		actions.put(2, "delete");
	}
	
	
	
	/**
	 * 
	 */
	public static void performFilesystemRead(String directory, String filePattern) {

		long starttime = System.currentTimeMillis();
		Path dataPath = Paths.get(directory);

		System.out.println(String.format("DIRECTORY %s pattern :%s   ", dataPath, filePattern));

		// skip the path if it does not exist
		if (Files.exists(dataPath)) {

			try {
				System.out.println(String.format("START DIRECTORY WALKER : %s pattern :%s begin walker", dataPath, filePattern));
				
				// walkFileTree takes a directory Path, An EnumSet of configurable options, the depth of directory recursion, and our FileVisitor class
				EnumSet<FileVisitOption> fileVisitOptions = EnumSet.of(FileVisitOption.FOLLOW_LINKS);
				final int maxDepth = 1;
				Files.walkFileTree(dataPath, fileVisitOptions, maxDepth, new FileVisitor(filePattern, actions));

			} catch (IOException e) {
				e.printStackTrace();
			}
		} else {
			System.out.println(String.format("WALKER [%s] pattern :[%s] no files exist ", dataPath, filePattern));
		}

		long millis = (System.currentTimeMillis() - starttime);
		long seconds = (millis > 60) ? millis / 1000 : 0;
		System.out.println("END DIRECTORY WALKER|runtimesec=" + seconds);
	}	
	
	
	/**
	 * Main executable method - Stripped down to bare essentials 
	 * This recipe takes a directory and a file pattern 
	 * 
	 * @throws IOException 
	 * 
	 */
		public static void main(final String[] args) throws IOException {
			
			String directory= "c:\\temp";
			if (args.length > 0 )
				directory = args[0];
			
			String pattern= "*.*";
			if (args.length > 2 )
				pattern = args[2];
			
			SimpleDAO.setup();
			performFilesystemRead( directory,  pattern ) ;
			
		}
	
	
}
