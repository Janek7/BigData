
public class MorrisCounter {
	
	private static final int X = 3;
	private static final int COUNT_LIMIT = 100000;

	public static void main(String[] args) {
		
		double mc = 0;
		int c = 0;
		double pow = Math.pow(2, X);
		
		for (int i = 0; i < COUNT_LIMIT; i++) {
			c++;
			if (Math.random() * pow <= 1) {
				mc++;
			}
			System.out.println(i + " " + Math.random() * pow + " " + mc);
		}
		mc = mc * pow;
		
		System.out.println("Normal Counter: " + c);
		System.out.println("Morris Counter: " + mc);

	}

}
