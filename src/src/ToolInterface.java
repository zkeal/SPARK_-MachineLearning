package src;

import Jama.Matrix;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import java.util.ArrayList;
import java.util.HashMap;

public interface ToolInterface {
    //cast traning data to form-data
    ArrayList<Double> getparameters(double SampleId, Object object);

    //check whether the kernel is support
    String getKernel(String kernel);

    //get init matrix
    Matrix getinit_matrix(int demension);

    //get Demension of data set
    int getDemension(HashMap<String, ArrayList> dateset);

    //get Matrix of label
    Matrix getLabel(HashMap<String, ArrayList> data_set) throws HiveException;

    //get Matrix of tran data
    Matrix getTrain_data(int dimension, HashMap<String, ArrayList> data_set) throws HiveException;

    //SMO method
    Equations SMO(Matrix aerph, Matrix label, Matrix traindata, double tolerance, int maxcounter) throws HiveException;

    //build result
    String build_result(Matrix aerph, Matrix label, Matrix traindata);
}
