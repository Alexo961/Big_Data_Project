package secondjob;

import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class FinalForEach
implements VoidFunction<Tuple2<Tuple2<Integer, String>, String>> {

	Integer year = null;
	String sector = null;

	@Override
	public void call(Tuple2<Tuple2<Integer, String>, String> t) throws Exception {
		if (year == null && sector == null) {
			year = t._1._1();
			sector = t._1._2();
			System.out.println(year); 
			System.out.println("\t" + sector + ":");
		}
		else {

			if (!(t._1._1().equals(year))) {
				year = t._1._1();
				sector = t._1._2();
				System.out.println();
				System.out.println(year);
				System.out.println("\t" + sector + ":");
			}

			else {
				if (!(t._1._2().equals(sector))) {
					sector = t._1._2();
					System.out.println();
					System.out.println("\t" + sector + ":");
				}
			}
		}
		System.out.println(t._2());
	}

}
