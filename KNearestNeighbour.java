package exp4_1;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import exp4_1.Distance;
import exp4_1.ListWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
/**
 * @author KING
 *
 */
public class KNearestNeighbour {
	public static class KNNMap extends Mapper<LongWritable,
	Text,LongWritable,ListWritable<DoubleWritable>>{
		private int k;
		private ArrayList<Instance> trainSet;
		@Override
		protected void setup(Context context) throws IOException,InterruptedException{
			k = context.getConfiguration().getInt("k", 1);
			trainSet = new ArrayList<Instance>();
			Path[] trainFile = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			//add all the tranning instances into attributes
			BufferedReader br = null;
			String line;
			for(int i = 0;i < trainFile.length;i++){
				br = new BufferedReader(new FileReader(trainFile[0].toString()));
				while((line = br.readLine()) != null){
		            Instance trainInstance = new Instance(line);
					trainSet.add(trainInstance);////读入缓存文件中的所有训练样本集合存放在本地数据结构中
				}
			}
		} 
		/**
		 * 找到距离最近的K个，
		 * 输出 <textIndex,lableList>
		 */
		@Override
		public void map(LongWritable textIndex, Text textLine, Context context)
				throws IOException, InterruptedException {
			//distance 存储所有临近的distance值
			//. trainLable 存储所有相应的lable
			ArrayList<Double> distance = new ArrayList<Double>(k);
			ArrayList<DoubleWritable> trainLable = new ArrayList<DoubleWritable>(k);
			for(int i = 0;i < k;i++){//初始化
				distance.add(Double.MAX_VALUE);
				trainLable.add(new DoubleWritable(-1.0));
			}
			ListWritable<DoubleWritable> lables = new ListWritable<DoubleWritable>(DoubleWritable.class);//自定义的数据结构，包括一个list一个对象	
			Instance testInstance = new Instance(textLine.toString()); //测试行
			for(int i = 0;i < trainSet.size();i++){
				try {
					//训练行与测试行
					double dis = Distance.EuclideanDistance(trainSet.get(i).getAtrributeValue(), testInstance.getAtrributeValue());
					int index = indexOfMax(distance);
					if(dis < distance.get(index)){//如果dis在范围内，dis小于里面距离最大的，说明它更近一些，更合适
						distance.remove(index);
					    trainLable.remove(index);
					    distance.add(dis);
					    trainLable.add(new DoubleWritable(trainSet.get(i).getLable()));
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}		
			}			
			lables.setList(trainLable);	//得到的K个训练数据的属性值
		    context.write(textIndex, lables);//将偏移量作为key
		}
		/**
		 * 返回一个数组中最大值的下标
		 */
		public int indexOfMax(ArrayList<Double> array){
			int index = -1;
			Double min = Double.MIN_VALUE; 
			for (int i = 0;i < array.size();i++){
				if(array.get(i) > min){
					min = array.get(i);
					index = i;
				}
			}
			return index;
		}
	}
	public static class KNNReduce extends Reducer<LongWritable,ListWritable<DoubleWritable>,NullWritable,DoubleWritable>{
		@Override
		public void reduce(LongWritable index, Iterable<ListWritable<DoubleWritable>> kLables, Context context)
				throws IOException, InterruptedException{
			/**
			 * 选出分类样本中频率最高的分类号作为样本分类号，
			 * reduce阶段输出的分类号就对应了带分类样本在文件中的位置，因为是按照偏移量作为Key直。
			 */
			DoubleWritable predictedLable = new DoubleWritable();
			for(ListWritable<DoubleWritable> val: kLables){
				try {
					//一个键只可能有一个list
					predictedLable = valueOfMostFrequent(val);
					break;
				} catch (Exception e) {
					//  
					e.printStackTrace();
				}
			}
			context.write(NullWritable.get(), predictedLable);
		}
		public DoubleWritable valueOfMostFrequent(ListWritable<DoubleWritable> list) throws Exception{
			if(list.isEmpty())
				throw new Exception("list is empty!");
			else{
				HashMap<DoubleWritable,Integer> tmp = new HashMap<DoubleWritable,Integer>();
				for(int i = 0 ;i < list.size();i++){
					//list里面放的是属性
					if(tmp.containsKey(list.get(i))){//如果已经有了，
						Integer frequence = tmp.get(list.get(i)) + 1;//更新数量
						tmp.remove(list.get(i));
						tmp.put(list.get(i), frequence);//写进去
					}else{//否则，新插入 <属性，1>
						tmp.put(list.get(i), new Integer(1));
					}
				}
				//找到频率最大的属性值
				DoubleWritable value = new DoubleWritable();
				Integer frequence = new Integer(Integer.MIN_VALUE);
				Iterator<Entry<DoubleWritable, Integer>> iter = tmp.entrySet().iterator();
				while (iter.hasNext()) {
				    Map.Entry<DoubleWritable,Integer> entry = (Map.Entry<DoubleWritable,Integer>) iter.next();
				    if(entry.getValue() > frequence){
				    	frequence = entry.getValue();
				    	value = entry.getKey();
				    }
				}
				return value;
			}
		}
	}
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException{
		//运行在eclipse端需要以下参数
    //	args = new String[] {"hdfs://localhost:9000/user/hadoop/testSet","hdfs://localhost:9000/user/testSetOut","hdfs://localhost:9000/user/hadoop/trainSet/train.txt","4"}; 
    	//获取配置信息
        Configuration configuration = new Configuration();
        //configuration.set("fs.defaultFS",  "hdfs://localhost:9000");
        //设置命令行参数
//        String[] otherArgs = new GenericOptionsParser(configuration , args).getRemainingArgs();
//        if(otherArgs.length != 4) {
//        	System.err.println("Usage:wordcount <in> <out>");
//        	System.exit(2);       	
//        }
		Job kNNJob = Job.getInstance(configuration, "kNNJob"); 
	//	kNNJob.setJarByClass(KNearestNeighbour.class);
		//kNNJob.setJar("knn.jar");
		// 判断output文件夹是否存在，如果存在则删除
				Path path = new Path(args[1]);// 取第1个表示输出目录参数（第0个参数是输入目录）
				FileSystem fileSystem = path.getFileSystem(configuration);// 根据path找到这个文件
				if (fileSystem.exists(path)) {
					fileSystem.delete(path, true);// true的意思是，就算output有东西，也一带删除
				}
		DistributedCache.addCacheFile(new URI(args[2]), kNNJob.getConfiguration());
		kNNJob.getConfiguration().setInt("k", Integer.parseInt(args[3]));
		kNNJob.setMapperClass(KNNMap.class);
		kNNJob.setMapOutputKeyClass(LongWritable.class);
		kNNJob.setMapOutputValueClass(ListWritable.class);
		kNNJob.setReducerClass(KNNReduce.class);
		kNNJob.setOutputKeyClass(NullWritable.class);
		kNNJob.setOutputValueClass(DoubleWritable.class);
		kNNJob.setInputFormatClass(TextInputFormat.class);
		kNNJob.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(kNNJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(kNNJob, new Path(args[1]));
		kNNJob.waitForCompletion(true);
		System.out.println("finished!");
	}
}