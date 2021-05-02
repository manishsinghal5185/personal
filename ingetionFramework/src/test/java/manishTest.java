import org.junit.Test;
import py4j.StringUtil;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class manishTest {
    @Test
    public void test(){
        List<String> intLIst = Arrays.asList("id","name");

        String key=intLIst.stream().map(n->String.valueOf(n)).collect(Collectors.joining(","));
        System.out.println(key);
    }
}
