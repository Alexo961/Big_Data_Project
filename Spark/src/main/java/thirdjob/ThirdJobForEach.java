package thirdjob;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class ThirdJobForEach implements VoidFunction<Tuple2<String, Iterable<String>>> {

	@Override
	public void call(Tuple2<String, Iterable<String>> t) throws Exception {
		List<String> yearVars = Arrays.asList(t._1().split(","));
		StringBuilder sb = new StringBuilder();
		for (String s : yearVars) {
			String year = s.split("_")[0];
			String variation = s.split("_")[1];
			sb.append(year + " " + variation + "%" + ", ");
		}
		sb.delete(sb.length() - 2, sb.length() - 1);
		sb.append(": ");

		int i = 0;
		StringBuilder sb2 = new StringBuilder();
		sb2.append("{");
		for (String s : t._2) {
			sb2.append(s).append(", ");
			i++;
		}
		sb.delete(sb.length() - 2, sb.length() - 1);
		sb2.append("}");

		if(i > 1) {
		System.out.println(sb.toString());
		System.out.println(sb2.toString());
		}

	}

}
