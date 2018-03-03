package com.sap.datacloud.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.sap.datacloud.serviceImpl.CollectServiceImpl;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.sap.datacloud.service.CollectService;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.validation.Valid;

@RestController
@CrossOrigin(origins = "*", methods = { RequestMethod.GET, RequestMethod.POST, RequestMethod.PUT, RequestMethod.DELETE,
		RequestMethod.HEAD })
@RequestMapping("/CollectAndCall")
final class CollectController {

	private final CollectService service;

	// TODO: replace this with actual property file
	@Value("${dnb.fieldMapping}")
	String mapping;
	String accessKey = "AKIAIWXN35YEXZZAQD7Q";
	String secretKey = "Y30US8EsGcfhQrC3K3rOzJ3r8IabucYBxHgcetsI";
	//final private AmazonS3 s3Client = new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey));
			
	@Autowired
	CollectController(CollectService service) {
		this.service = service;
	}

	@RequestMapping(method = RequestMethod.POST)
	@ResponseStatus(HttpStatus.CREATED)
	// what is the use of Multipart file???
	// add valid annotation to the file
	// TODO: the name of the file is not needed; store this file to S3 and then
	// fetch it from there and process
	public @ResponseBody String create(@RequestParam("name") String fileName,
			@RequestParam(name = "file", required = false) MultipartFile file) throws IOException {

		// mapping of sap to dnb fields
		//currently using instance of service impl, change to service 
		Map<String, String> mapDNBFields = CollectServiceImpl.processRow(mapping);

		// Preparing the result!!!
		List<Map<String, String>> table = CollectServiceImpl.readFile(file);
		String result = ""; // final result to be sent
		
		//mapSAPFields : sap field to values
		for (Map<String, String> mapSAPFields : table) {
			String resultTemp = "";
			for (Map.Entry<String, String> entry : mapSAPFields.entrySet()) {
				if (mapDNBFields.containsKey(entry.getKey())) {
					String mappedField = mapDNBFields.get(entry.getKey());
					resultTemp = resultTemp + mappedField + "=" + entry.getValue() + "&";
				}
				// TODO: if the field is not found, it'll be an error scenario
				// and needs to be logged
			}
			//TODO: streaming the result on the fly
			//the name of the file - logical separation tenant id/ job id/ batch id
			result = result + resultTemp.substring(0, resultTemp.length() - 1) + "\n"; // use
		}
		
		
		AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
		ClientConfiguration clientConfig = new ClientConfiguration();
		clientConfig.setProtocol(Protocol.HTTP);
		
		//Use this format
		// upload file to folder and set it to public
		//String fileName = folderName + SUFFIX + "testvideo.mp4";
		AmazonS3 s3Client = new AmazonS3Client(credentials, clientConfig);
		String bucket_name = "hcp-bee25cfb-242a-4f1c-92d5-ddad94c58e66";
        
        //In S3, the new data overwrites the existing one, there is no provision for APPENDING        
		// save on s3 with public read access i.e. Upload the file
		// for public read
        //DOING IT BY STORING IN A TEMP FILE
        File resFile = File.createTempFile("res",null);
        BufferedWriter bw = new BufferedWriter(new FileWriter(resFile));
        bw.write(result);
        bw.close();
       
		ObjectMetadata objectMetadata = new ObjectMetadata();
		objectMetadata.setContentType(file.getContentType());

		s3Client.putObject(new PutObjectRequest(bucket_name, "TestCollectAndCall1", resFile)
				.withCannedAcl(CannedAccessControlList.PublicRead));
		
		//incorporate error handling - so success of storage of the file is taken care of
		//Amazon S3 never stores partial objects; if during this call an exception wasn't thrown, the entire object was stored.
		String s3Url = ((AmazonS3Client) s3Client).getResourceUrl(bucket_name, "TestCollectAndCall1");
		System.out.println("New upload url: "+s3Url);
		
		//http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/model/PutObjectRequest.html
		return result;
	}
}