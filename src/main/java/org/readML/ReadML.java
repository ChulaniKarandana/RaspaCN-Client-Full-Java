package org.readML;

import org.dmg.pmml.DataType;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.PMML;
import org.jpmml.evaluator.*;
import org.jpmml.model.JAXBUtil;
import org.jpmml.model.filters.ImportFilter;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

import javax.xml.transform.Source;
import javax.xml.transform.sax.SAXSource;
import java.io.*;
import java.util.*;

import java.util.logging.Logger;

public class ReadML {


    private static final Logger log = Logger.getLogger(String.valueOf(ReadML.class));
    private Evaluator evaluator;
    public static BufferedWriter bw;
    private Map<FieldName, org.dmg.pmml.DataType> outputFields = new LinkedHashMap<FieldName, DataType>();
    ArrayList<Integer> noOfWorkers = new ArrayList<Integer>();
    ArrayList<Integer> noOfPartialSiddhiApps = new ArrayList<Integer>();
    //read the number of  partial siddhi apps from configuration files
    static int no_of_partial_siddhi_apps = 2;
    static int no_of_nodes = 2;
    static String worker1 = "wso2sp-worker-1";
    static String worker2 = "wso2sp-worker-2";
    static String worker3 = "wso2sp-worker-3";
    static String worker4 = "wso2sp-worker-4";
    static String worker5 = "wso2sp-worker-5";
    static String worker6 = "wso2sp-worker-6";
    static String worker7 = "wso2sp-worker-7";
    static String worker8 = "wso2sp-worker-8";
    static String worker9 = "wso2sp-worker-9";
    static String worker10 = "wso2sp-worker-10";
    static String worker11 = "wso2sp-worker-11";

    public final static PMML loadModel(final String file) throws Exception {

        PMML pmml = null;
        File inputFilePath = new File( file );
        try {
            InputStream in = new FileInputStream(inputFilePath);
            try {
                XMLReader reader = XMLReaderFactory.createXMLReader();
                ImportFilter filter = new ImportFilter(reader);

                Source source = new SAXSource(filter, new InputSource(in));

                pmml = JAXBUtil.unmarshalPMML(source);

            } finally {
                in.close();
            }
        } catch( Exception e) {
            throw new RuntimeException(e);
        }
        return pmml;
    }
    public ArrayList<Double> predictThroughputPmml() throws Exception {

        PMML pmml = loadModel("/home/user123/Applications/Mavericks_Kubernetes_Client/RaspaCN/ClientApplication-pmml/Worker_predictions.pmml");

        ModelEvaluatorFactory modelEvaluatorFactory = ModelEvaluatorFactory.newInstance();
        ModelEvaluator<?> modelEvaluator = modelEvaluatorFactory.newModelEvaluator(pmml);
        evaluator = (Evaluator) modelEvaluator;

        List<InputField> inputFields = evaluator.getActiveFields();

        if (evaluator.getOutputFields().size() == 0) {
            List<TargetField> targetFields = evaluator.getTargetFields();
            for (TargetField targetField : targetFields) {
                outputFields.put(targetField.getName(), targetField.getDataType());
            }
        } else {
            List<OutputField> outputFields = evaluator.getOutputFields();
            for (OutputField outputField : outputFields) {
                this.outputFields.put(outputField.getName(), outputField.getDataType());
            }
        }
        String csvFile = "/home/user123/Applications/Mavericks_Kubernetes_Client/RaspaCN/ClientApplication-pmml/Sample_new.csv";
        BufferedReader br = null;
        String line = "";
        String COMMA_DELIMITER = ",";
        noOfPartialSiddhiApps.clear();
        noOfWorkers.clear();

        ArrayList<Double> output = new ArrayList<Double>();
        try {
            br = new BufferedReader(new FileReader(csvFile));
            Map<FieldName, FieldValue> pmmlArguments = new LinkedHashMap<FieldName, FieldValue>();

            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] tokens = line.split( COMMA_DELIMITER );
                noOfWorkers.add(Integer.valueOf(tokens[0]));
                noOfPartialSiddhiApps.add(Integer.valueOf(tokens[1]));

                for(int i = 0; i < inputFields.size(); i++){
                    FieldValue pmmlValue = inputFields.get(i).prepare(Integer.parseInt(tokens[i]));
                    pmmlArguments.put(inputFields.get(i).getFieldName(), pmmlValue);
                }
                Map<FieldName, ?> result = evaluator.evaluate(pmmlArguments);
                for (FieldName fieldName : outputFields.keySet()) {
                    if (result.containsKey(fieldName)) {
                        Object value = result.get(fieldName);
                        output.add((Double) EvaluatorUtil.decode(value));
                    }
                }

            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return output;
    }

    public void getNoOfWorkers(ArrayList<Double> outputResult) throws IOException, InterruptedException {
        int count = 0;
        double maximumThroughput = outputResult.get(0);
        for (int i=0; i<outputResult.size(); i++){
            if(maximumThroughput < outputResult.get(i)){
                maximumThroughput = outputResult.get(i);
                count = i;
            }
        }
        System.out.println("Maximum Throughput : " + maximumThroughput);
        System.out.println("No of Workers : " + noOfWorkers.get(count));
        System.out.println("No of Partial Siddhi Apps : " + noOfPartialSiddhiApps.get(count));

        // run the deploy.sh files

        ProcessBuilder pb = new ProcessBuilder("bash", "/home/user123/Applications/Mavericks_Kubernetes_Client/pattern-distributed/scripts/auto_deploy.sh",
                String.valueOf(noOfWorkers.get(count)));
        Process p = pb.start();
        BufferedReader reader = new BufferedReader(new InputStreamReader(p.getErrorStream()));
        String line = null;
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }

        // sleep for some time till the deployment process completes(1 min)

        Thread.sleep(1000);

        // while true loop for continuously check whether their are empty workers

        while (true) {
            // call the undeploy script
            System.out.println("Calling the undeploy file .................");
            ProcessBuilder pb_undeploy = null;
            System.out.println("Workers:::::::::::"+String.valueOf(noOfWorkers.get(count)));
            System.out.println(String.valueOf(noOfWorkers.get(count)).getClass());
            if (String.valueOf(noOfWorkers.get(count)) == "1") {
                pb_undeploy = new ProcessBuilder("bash", "/home/user123/Applications/Mavericks_Kubernetes_Client/pattern-distributed/scripts/observe-undeploy.sh",
                        Integer.toString(no_of_nodes), Integer.toString(noOfPartialSiddhiApps.get(count)), worker1);

            }
            else if (String.valueOf(noOfWorkers.get(count)) == "2") {
                pb_undeploy = new ProcessBuilder("bash", "/home/user123/Applications/Mavericks_Kubernetes_Client/pattern-distributed/scripts/observe-undeploy.sh",
                        Integer.toString(no_of_nodes), Integer.toString(noOfPartialSiddhiApps.get(count)), worker1, worker4);

            }
            else if (String.valueOf(noOfWorkers.get(count)) == "3") {
                pb_undeploy = new ProcessBuilder("bash", "/home/user123/Applications/Mavericks_Kubernetes_Client/pattern-distributed/scripts/observe-undeploy.sh",
                        Integer.toString(no_of_nodes), Integer.toString(noOfPartialSiddhiApps.get(count)), worker1, worker2, worker4);

            }
            else if (String.valueOf(noOfWorkers.get(count)) == "4") {
                pb_undeploy = new ProcessBuilder("bash", "/home/user123/Applications/Mavericks_Kubernetes_Client/pattern-distributed/scripts/observe-undeploy.sh",
                        Integer.toString(no_of_nodes), Integer.toString(noOfPartialSiddhiApps.get(count)), worker1, worker2, worker4, worker5);

            }
            else if (String.valueOf(noOfWorkers.get(count)).equals("5")) {
                pb_undeploy = new ProcessBuilder("bash", "/home/user123/Applications/Mavericks_Kubernetes_Client/pattern-distributed/scripts/observe-undeploy.sh",
                        Integer.toString(no_of_nodes), Integer.toString(noOfPartialSiddhiApps.get(count)), worker1, worker2, worker3, worker4, worker5);

            }
            else if (String.valueOf(noOfWorkers.get(count)) == "6") {
                pb_undeploy = new ProcessBuilder("bash", "/home/user123/Applications/Mavericks_Kubernetes_Client/pattern-distributed/scripts/observe-undeploy.sh",
                        Integer.toString(no_of_nodes), Integer.toString(noOfPartialSiddhiApps.get(count)), worker1, worker2, worker3, worker4, worker5, worker6);

            }
            else if (String.valueOf(noOfWorkers.get(count)) == "7") {
                pb_undeploy = new ProcessBuilder("bash", "/home/user123/Applications/Mavericks_Kubernetes_Client/pattern-distributed/scripts/observe-undeploy.sh",
                        Integer.toString(no_of_nodes), Integer.toString(noOfPartialSiddhiApps.get(count)), worker1, worker2, worker3, worker4, worker5, worker6, worker7);

            }
            else if (String.valueOf(noOfWorkers.get(count)) == "8") {
                pb_undeploy = new ProcessBuilder("bash", "/home/user123/Applications/Mavericks_Kubernetes_Client/pattern-distributed/scripts/observe-undeploy.sh",
                        Integer.toString(no_of_nodes), Integer.toString(noOfPartialSiddhiApps.get(count)), worker1, worker2, worker3, worker4, worker5, worker6,worker7,worker10);

            }
            else if (String.valueOf(noOfWorkers.get(count)) == "9") {
                pb_undeploy = new ProcessBuilder("bash", "/home/user123/Applications/Mavericks_Kubernetes_Client/pattern-distributed/scripts/observe-undeploy.sh",
                        Integer.toString(no_of_nodes), Integer.toString(noOfPartialSiddhiApps.get(count)), worker1, worker2, worker3, worker4, worker5, worker6,worker7, worker8, worker10);

            }
            //pb_undeploy.start();
            Process p_undeploy = pb_undeploy.start();
            BufferedReader reader_undeploy = new BufferedReader(new InputStreamReader(p_undeploy.getInputStream()));
            String line_undeploy = null;
            while ((line_undeploy = reader_undeploy.readLine()) != null) {
                System.out.println(line_undeploy);
            }

            Thread.sleep(300000);
        }
    }
    
    public static void main(String a[]) throws Exception {
        try {
            File file = new File("/home/user123/Applications/Mavericks_Kubernetes_Client/RaspaCN/ClientApplication-pmml/ThroughputPredictions.csv");

            bw = null;
            FileWriter fw = new FileWriter(file,false);
            if (!file.exists()) {
                file.getParentFile().mkdirs();
                file.createNewFile();
            }
            fw = new FileWriter(file.getAbsoluteFile(), true);
            bw = new BufferedWriter(fw);
            bw.write("Throughput");

            bw.write("\n");
            bw.flush();
            bw.close();

        } catch (Exception e) {
            System.out.println("Error when writing to the file");
        }
        ReadML readML=new ReadML();
        readML.getNoOfWorkers(readML.predictThroughputPmml());
    }

}
