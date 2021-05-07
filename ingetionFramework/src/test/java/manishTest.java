import com.capgemini.commons.dataTarget;
import com.capgemini.commons.dsFileRequest;
import com.capgemini.commons.ingestionRequest;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.ivy.util.StringUtils;
import org.junit.Test;
import py4j.StringUtil;
import com.capgemini.ingestionServiceRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class manishTest {
    @Test
    public void test() throws ParseException, IOException {
        String[] args = "-f|\"C:\\Users\\msingha1\\OneDrive - Capgemini\\Documents\\I&D Practice\\template.json\"|-p|\"bussDate=202105020,name=manish\""
                .split("\\|");
        CommandLine commandLine = ingestionServiceRunner.commandLineParse(args);
        String paramStr = commandLine.getOptionValue('p');
        String envStr = commandLine.getOptionValue('e');
        String configFileName = commandLine.getOptionValue('f');
        System.out.println(configFileName);
        ObjectMapper m = new ObjectMapper();
        JsonNode configNode = ingestionServiceRunner.readFromFiles(configFileName);
        ingestionRequest IR = new ingestionRequest();
        ingestionServiceRunner.buildIngestionRequest(IR, configNode, ingestionServiceRunner.parseParameters(paramStr));
        System.out.println("ingestionRequestObject is:" + m.writerWithDefaultPrettyPrinter().writeValueAsString(IR));
//        dsFileRequest dsFile = IR.getDsFiles().get(0);
//        if (dsFile.getDsSchemaFileName() == null || dsFile.getDsSchemaFileName().trim().isEmpty()) {
//            System.out.println("null schema");
//        }else
//            System.out.println("not null schema");
        String[] partCols=IR.getDataTargets().get(1).getPartition();
              if (partCols==null) {
                  partCols=new String[]{""};
                  System.out.println(Arrays.asList(partCols).contains("mann"));
              }
    }
    @Test
    public void sample(){
        String partCol="id,name";
        String[] partCols=partCol.split("\\,");
        System.out.println(StringUtils.join(partCols,","));
    }
}
