package exp4;
public class kNode {
	private double[] data;
	private int n;//data的长度
	private int type;//所属类别 
	
	public kNode(double[] data, int n, int type) {
		this.data = new double[n];
		for(int i=0; i<n; i++)
			this.data[i] = data[i];
		this.n = n;
		this.type = type;
	}
	public kNode(int n, double[] d) {
		this.n = n;
		this.data = new double[n];
		for(int i=0; i<d.length; i++)
			data[i] = d[i];
	}
	public double[] getData() {
		return this.data;
	}
	public void setType(int type) {
		this.type = type;
	}
	public int getType() {
		return this.type; 		
	}
	
}
