package generator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

/**
 * Random data generator
 * 
 * @author radu
 */
public class Generator {
	public static void generateOneFile(int nrRows, String destinationFile) {
		StringBuilder rowData = new StringBuilder();
		Random rnd = new Random();
		
		long startTime = System.nanoTime();
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File(destinationFile)));

			for (int i = 1; i <= nrRows; i++) {
				rowData.setLength(0);
				rowData.append("client_").append(rnd.nextInt(100) + 1).append(",")		//client id
						.append(rnd.nextInt(99) + 1).append(",")						//quantity
						.append("product_").append(rnd.nextInt(1000)).append(",")		//product
						.append("store_").append(rnd.nextInt(100));						//store

				bw.write(rowData.toString());
				if (i < nrRows) {
					bw.write("\n");
				}
			}
			bw.close();
		} catch (IOException ex) {
			System.err.println(ex.getMessage());
		}

		long endTime = System.nanoTime();
		System.out.printf("Finished generating file in %4.4f ms\n", ((endTime - startTime) / 1_000_000.0));
	}
}