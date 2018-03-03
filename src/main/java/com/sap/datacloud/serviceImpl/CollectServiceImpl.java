package com.sap.datacloud.serviceImpl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.sap.datacloud.service.CollectService;

//why is this class final ?
@Service
public final class CollectServiceImpl implements CollectService {
	
	// return the list of records in a file
	public static List<Map<String, String>> readFile(MultipartFile file) throws IOException {
		List<Map<String, String>> table = new ArrayList<>();
		// mapping sap field name to values
		Map<String, String> mapSAPFields = null;
		BufferedReader zBr = null;
		InputStream is = null;
		try {
			// flush the stream when you are done
			if (file != null && !(file).isEmpty()) {
				is = (file).getInputStream();
				zBr = new BufferedReader(new InputStreamReader(is));
				String zCurrentLine;
				while ((zCurrentLine = zBr.readLine()) != null) {
					mapSAPFields = processRow(zCurrentLine);
					table.add(mapSAPFields);
				}
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		} finally {
			if (zBr != null && is != null) {
				zBr.close();
				is.close();
			}
			zBr = null;
			is = null;
		}
		return table;
	}

	public static Map<String, String> processRow(String rowVal) {
		String pairs[] = rowVal.split(",");
		Map<String, String> mapFields = new HashMap<>();
		for (String str : pairs) {
			String temp[] = str.split(":");
			mapFields.put(temp[0].trim(), temp[1].trim());
		}
		return mapFields;
	}
}