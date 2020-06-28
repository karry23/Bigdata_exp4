package exp44;

/**
 * 客户端实现KMeans工具类的调用
 */

public class Client {
    public static void main(String[] args){
        //String path = "file:///usr/local/hadoop/exp4/DataSet.txt";
    	String path = "/usr/local/hadoop/exp4/DataSet.txt";
        KMeans kMeans = new KMeans(path, 3);
        kMeans.doKMeans();
    }
}
