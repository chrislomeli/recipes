package temp;

import static java.nio.file.FileVisitResult.CONTINUE;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;

/*************************************
* FileVisitor Class 
* 
*/
	public class FileVisitor extends SimpleFileVisitor<Path> {

		private PathMatcher matcher = null;
		Map<Integer,String> actions=null;

		/**
		 * Constructor
		 * 
		 * @param pattern
		 */
		FileVisitor(String pattern, Map<Integer,String> requested_actions) {
			matcher = FileSystems.getDefault().getPathMatcher("glob:" + pattern);
			actions = requested_actions;
		}


		/**
		 * Visit
		 * @throws IOException 
		 */
		@Override
		public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {

			Path name = file.getFileName();
			if (name == null || name.getFileName().toString().startsWith(".") || !matcher.matches(name)) {
				return CONTINUE;
			}

			String sourceFileName = file.toAbsolutePath().toString();
			attrs.creationTime();
			attrs.lastModifiedTime();
			attrs.size();
			sourceFileName = sourceFileName.replace("\\", "/");
			String baseName = FilenameUtils.getName(name.toString());

			int status = SimpleDAO.fileExists(baseName);
			
			String action = actions.get(status);
			if (action == null) {
				System.out.println(String.format("file %s add ", baseName));
				SimpleDAO.insertfile(baseName, 1);
			} else if (action.equals("delete")) {
				System.out.println(String.format("DELETE file %s  ", baseName));
				//Files.delete(file);
			} else if (action.equals("celebrate")) {
				System.out.println(String.format("Yahoo! file %s exists ", baseName));
			} else {
				System.out.println(String.format("file %s - unhandled action ", baseName));
			}
			return CONTINUE;
		}

		/**
		 * Pre visit directory - decide whether to recurse
		 */
		@Override
		public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
			if (attrs.isDirectory() && dir.getFileName().toString().toLowerCase().contains("archive"))		
				return java.nio.file.FileVisitResult.SKIP_SUBTREE;
			else 
				return CONTINUE;
		}

		/**
		 * Handle visit failed
		 */
		@Override
		public FileVisitResult visitFileFailed(Path file, IOException exc) {
			System.err.println(exc);
			return CONTINUE;
		}
	}

