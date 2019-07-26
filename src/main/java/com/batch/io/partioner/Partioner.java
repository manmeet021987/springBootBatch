package com.batch.io.partioner;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;

/* This class is a Partitioner class to read the given file and provide each line to single thread in ExecutionContext */
public class Partioner implements Partitioner {

	@Override
	public Map<String, ExecutionContext> partition(int gridSize) {
		Map<String, ExecutionContext> map = new HashMap<>();

		List<String> trunks = new ArrayList<>();
		try (BufferedReader br = new BufferedReader(new FileReader("files/MyFile.txt"))) {
			String line;


			while ((line = br.readLine()) != null) {
				trunks.add(line);
					}

		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		// add to map(executioncontext) to start processing
		for (int i=0; i<trunks.size(); i++) {
			ExecutionContext context = new ExecutionContext();
			context.put("lines", trunks.get(i));
			map.put("partition"+i, context);
		}

		return map;



	}


}
