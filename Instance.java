package exp4_1;
public class Instance {
	private double[] attributeValue;
    private double lable;
    /**
     * a line of form a1 a2 ...an lable
     */
    public Instance(String line){
    	String[] value = line.split(" ");
    	attributeValue = new double[value.length - 1];//
    	for(int i = 0;i < attributeValue.length;i++){//把数据存进来
    		attributeValue[i] = Double.parseDouble(value[i]);
    	}
    	lable = Double.parseDouble(value[value.length - 1]);//最后一个是所属标签
    }
    public double[] getAtrributeValue(){
    	return attributeValue;
    }
    public double getLable(){
    	return lable;
    }
}
