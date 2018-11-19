# nyseAvgStockCompression
Mapreduce Program to demostrate the compression technique

			i. Problem Statement : Displaying the paths & child paths present in the passed arguments.
				1) Input : 2012-13 csv on nyse data
				2) Code Approach
					a. Configure the driver class with below details
						i. conf.setBoolean("mapreduce.compress.map.output", true);     // Used to compress the map output.
						ii.  conf.setClass("mapreuce.map.output.compress.codec", SnappyCodec.class, CompressionCodec.class); // Setting the compression codec for map output.
						iii. FileOutputFormat.setCompressOutput(job, true);       // Used to compress the reduceroutput.
						iv. FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);     // Setting the compression codec for reducer output.
