import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Top3StocksByVolumeMapper extends Mapper<LongWritable, Text, CustomLongPair, Text> {

	NYSEParser parser = new NYSEParser();
	private Set<String> stockTickerSet = new HashSet<String>();
	private static CustomLongPair mapOutputKey = new CustomLongPair();
	
	protected void setup(Context context){
		
		String stockTicker = context.getConfiguration().get("filter.by.stockTicker");
		
		if(stockTicker != null){
			String[] tickerArray = stockTicker.split(",");
			
			for(String ticker : tickerArray){
				stockTickerSet.add(ticker);
			}
		} 
	}
	
	public void map(LongWritable LineOffset, Text record, Context context) throws IOException, InterruptedException{
		
		parser.parse(record.toString());
		
		if(stockTickerSet.isEmpty() || stockTickerSet.contains(parser.getStockTicker())){
			
			mapOutputKey.setFirst(new LongWritable(parser.getTradeDateNumeric()));
			mapOutputKey.setSecond(new LongWritable(parser.getVolume()));
		}		
		context.write(mapOutputKey, record);		
	}
}
