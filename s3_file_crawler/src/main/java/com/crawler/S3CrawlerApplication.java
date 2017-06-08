package com.crawler;


import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.StringJoiner;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.SystemUtils;
import org.joda.time.DateTime;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class S3CrawlerApplication {

	static boolean GLOB_TEST = true;

	static String AWS_ACCESS_KEY = "[ACCESS-KEY]";

	static String AWS_SECRET_KEY = "[AWS-SECRET-KEY]";

	/**
	 * S3 client
	 */
	public static AmazonS3Client connectS3() throws ConfigurationException {
		// connect
		AmazonS3Client s3 = new AmazonS3Client(new AWSCredentials() {
			@Override
			public String getAWSAccessKeyId() {
				return AWS_ACCESS_KEY;
			}

			@Override
			public String getAWSSecretKey() {
				return AWS_SECRET_KEY;
			}
		});
		s3.setRegion(com.amazonaws.regions.Region.getRegion(Regions.US_WEST_2));
		return s3;
	}

	/*
	 * main logic
	 */
	public static int walkS3Folder(String bucketName, String prefix, String pattern) {

		PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + pattern);

		try {
			AmazonS3 s3client = connectS3();

			System.out.println("Getting objects for day " + prefix);
			final ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucketName).withMaxKeys(500)
					.withPrefix(prefix);

			ListObjectsV2Result result;

			do {

				result = s3client.listObjectsV2(req);

				for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {

					String key = objectSummary.getKey();

					if (key.endsWith("/"))
						continue;

					// does this file match what we are looking for ?
					// for other uses we could just use a java or regex match instead
					if (GLOB_TEST && !pattern.equals("*")) {
						String testkey = key;
						String testName = FilenameUtils.getName(testkey);
						Path basePath = Paths.get(testName);
						if (SystemUtils.IS_OS_WINDOWS) {    
							// apache spark, windows don't like colons in the file name, but linux and s3 allow them
							testkey = key.replaceAll(":", "?");
						}
						if (testName == null || testName.startsWith("_") || testName.startsWith(".")
								|| !matcher.matches(basePath)) {
							continue;
						}
					}

					// just print something out for this example
					StringJoiner sj = new StringJoiner(",");
					sj.add(String.format("bucket : \"%s\"", objectSummary.getBucketName()));
					sj.add(String.format("\"key\" : \"%s\"", objectSummary.getKey()));
					sj.add(String.format("\"modified_time\" : \"%s\"",
							new DateTime(objectSummary.getLastModified()).toString()));
					sj.add(String.format("\"file_size\" : \"%s\"", objectSummary.getSize()));
					System.out.println(String.format("{%s}", sj.toString()));

				}
				req.setContinuationToken(result.getNextContinuationToken());

			} while (result.isTruncated() == true);

		} catch (AmazonServiceException ase) {
			System.err.println("Caught an AmazonServiceException, " + "which means your request made it "
					+ "to Amazon S3, but was rejected with an error response " + "for some reason.");
			System.err.println("Error Message:    " + ase.getMessage());
			System.err.println("HTTP Status Code: " + ase.getStatusCode());
			System.err.println("AWS Error Code:   " + ase.getErrorCode());
			System.err.println("Error Type:       " + ase.getErrorType());
			System.err.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.err.println("Caught an AmazonClientException, " + "which means the client encountered "
					+ "an internal error while trying to communicate" + " with S3, "
					+ "such as not being able to access the network.");
			System.err.println("Error Message: " + ace.getMessage());
		} catch (Exception e) {
			System.err.println(e);
		}

		return -1;
	}
		
}
