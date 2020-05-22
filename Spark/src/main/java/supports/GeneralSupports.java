package supports;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class GeneralSupports {

	public static List<String> iterableToList(Iterable<String> it){
		List<String> list = new ArrayList<>();
		for (String s : it) {
			if (s != null && !(s.equals("")))
				list.add(s);
		}
		Collections.sort(list);
		return list;
	}
}
