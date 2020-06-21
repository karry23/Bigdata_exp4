package exp4;
import java.io.File;  
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import javafx.util.Pair;

import java.io.BufferedReader;  
import java.io.BufferedWriter;  
import java.io.FileInputStream;  
import java.io.FileWriter;  

public class knn {
	static int n=3, k=5;//n是特征数量， k是范围
	static List<kNode> train = new LinkedList<>();
	static List<kNode> test = new LinkedList<>();
	
	public static void readFile(List<kNode> aim, String file) {
		try { // 防止文件建立或读取失败，用catch捕捉错误并打印，也可以throw    
            File filename = new File(file); // 要读取以上路径的txt文件  
            InputStreamReader reader = new InputStreamReader(new FileInputStream(filename)); // 建立一个输入流对象reader  
            BufferedReader br = new BufferedReader(reader); // 建立一个对象，它把文件内容转成计算机能读懂的语言  
            String line = "";  
            String[] data;
            double[] d = new double[n];
            line = br.readLine();  
            while (line != null) {  
            	int i=0;
                line = br.readLine(); // 一次读入一行数据  
               // System.out.println("line"+line);
                if(line == null)
                	break;
                data = line.split(" ");//
                for(i=0; i<n; i++)
                	d[i] = Double.parseDouble(data[i]);//转换成double数组
                if(data.length==4)
                	aim.add(new kNode(d, n, Integer.parseInt(data[i])));//数组、3、类别        
                else
                	aim.add(new kNode(d, n, 0));//数组、3、类别
            }  
		}catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	
	public static void writeFile(List<kNode> test, String file) {
		try {
			File writename = new File(file); //输出路径  
            writename.createNewFile(); // 创建新文件  
            BufferedWriter out = new BufferedWriter(new FileWriter(writename));  
            StringBuffer string;
            for(int i=0; i<test.size(); i++)
            {
            	string = new StringBuffer();
            	for(double d : test.get(i).getData())
            		string.append(d+" ");
            	string.append(test.get(i).getType());
            	out.write(string.toString()+"\t\n");
            }
            out.flush(); // 把缓存区内容压入文件  
            out.close(); // 最后记得关闭文件 
		}catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	
	public static double distance(double[] d, double[] data) {
		double count = 0;
		for(int i=0; i<n; i++)
			count += (data[i]-d[i])*(data[i]-d[i]); 
		return Math.sqrt(count);		
	}
	
	public static int KNN(kNode testNode) {
		List<Pair<Double, Integer>> store_dis = new LinkedList<>();//距离和type
		int[] res = new int[100];
		for(int i=0; i<100; i++)
			res[i]=0;
		
		for(int i=0; i<train.size(); i++)//计算每一个训练点到目标点的距离
		{
			double dis = distance(train.get(i).getData(), testNode.getData()); 
			int type = train.get(i).getType(); 
			store_dis.add(new Pair<Double, Integer>(dis, type));
		}
		//HashMap<Integer, Integer> s = new HashMap<>();
		for(int i=store_dis.size()-1; i>=0; i--) {//冒泡K次，找距离最小的K个
			int max=i;
			for(int j=0; j<i; j++) {
				if(store_dis.get(j).getKey() > store_dis.get(max).getKey())
					max = j;
			}
			Pair<Double, Integer> temp = store_dis.get(max);
			store_dis.set(max, store_dis.get(i));
			store_dis.set(i, temp);//换
		} 
		for(int i=0; i<k; i++)
			res[store_dis.get(i).getValue()]++;
		int max = 0;
		for(int i=0; i<100; i++)
		{
			if(res[i]>res[max]) {
				max = i;
			}
		}
		return max; 
	}
	
	 //为每一个test数据计算type
	public static void testKnn(List<kNode> train) {
		for(int i=0; i<test.size(); i++) {
			int type = KNN(test.get(i));
			test.get(i).setType(type);
		}
	}

	public static void main(String[] args) { 
				
	    //readFile(train, ":\\Workpace\\KNN\\src\\knn\\train.txt");
//	    readFile(test, "D:\\Workpace\\KNN\\src\\knn\\test.txt");
		readFile(test, "~/exp4/test.txt");
		//readFile(train, "hdfs://localhost:9000/exp4/input/train.txt");
        //readFile(test, "D:\\Workpace\\KNN\\src\\knn\\test.txt");
		readFile(test, "hdfs://localhost:9000/exp4/input/test.txt");
	    testKnn(train);
	    //writeFile(test, "D:\\Workpace\\KNN\\src\\knn\\outPut.txt");
	    writeFile(test, "hdfs://localhost:9000/exp4/output");
	    
	}

}
