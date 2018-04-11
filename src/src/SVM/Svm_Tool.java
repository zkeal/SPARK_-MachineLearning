package src.SVM;

import Jama.Matrix;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import java.util.*;

public class Svm_Tool implements ToolInterface {
    @Override
    public ArrayList<Double> getparameters(double flag,Object object)
    {

        ArrayList<Double> result = new ArrayList<Double>();
        result.add(flag);
        String Parameters = object.toString();
        if(Parameters.isEmpty() || Parameters.equals(""))
        {
            return null;
        }
        else
        {
            String P_list[] = Parameters.split(",");
            for(String temp:P_list)
            {
                String[] key_value = temp.split(":");
                if(key_value.length!=2)
                {
                    continue;
                }
                double value= Double.parseDouble(key_value[1]);
                if(0.0==value)
                {
                    value=value+0.001;//laplace calibration
                }
                result.add(value);
            }
        }
        return result;
    }

    @Override
    public String getKernel(String kernel) {
        if(!kernel.isEmpty())
        {
            for(Object value:Kernel.values())
            {
                if(value.toString().equals(kernel))
                {
                    return kernel;
                }
            }
        }
        return Kernel.Linear.toString();
    }

    public enum Kernel{
        Linear,Gaussian,Laplace,Sigmoid
    }

    @Override
    public Matrix getinit_matrix(int demension)
    {
        if(demension>0)
        {
            double[][] init_double = new double[demension][1];
            for(int i=0;i<demension;i++)
            {
                init_double[i][0] = 0;
            }
            return new Matrix(init_double);
        }
        return null;
    }

    @Override
    public int getDemension(HashMap<String,ArrayList> dateset)
    {
        return dateset.size();
    }

    @Override
    public Equations SMO(Matrix aerph, Matrix label, Matrix traindata, double tolerance, int maxcounter) throws HiveException
    {
        double C=tolerance;
        double B=0;
        int Dimension = traindata.getColumnDimension();
        int row = traindata.getRowDimension();
        try {
            int iter=0;
            while (iter < maxcounter)
            {
                for(int i=0;i<row;i++)
                {
                    double Ei = getEi(aerph,label,traindata,i,Dimension,B);
                    if((label.get(i,0)*Ei<-0.001 && aerph.get(i,0)<C)||(label.get(i,0)*Ei>0.001 && aerph.get(i,0)>0))
                    {
                        // random choice
                        int j = getRandom_index(i,row);
                        //int j=get_heuristic(aerph,label,traindata,Dimension,B,traindata.getRowDimension(),Ei);
                        double Ej = getEi(aerph,label,traindata,j,Dimension,B);
                        double Lbottom = 0;
                        double Hup =0;
                        if(label.get(i,0)!=label.get(j,0))
                        {
                            Lbottom = aerph.get(j,0)-aerph.get(i,0);
                            Lbottom = Lbottom>0?Lbottom:0;
                            Hup = aerph.get(j,0)-aerph.get(i,0)+C;
                            Hup = Hup<C?Hup:C;
                        }else {
                            Lbottom = aerph.get(j,0)+aerph.get(i,0)-C;
                            Lbottom=Lbottom>0?Lbottom:0;
                            Hup = aerph.get(j,0)+aerph.get(i,0);
                            Hup = Hup<C?Hup:C;
                        }
                        if(Lbottom==Hup)
                        {
                            break;
                        }else {
                            Matrix data_i = traindata.getMatrix(i,i,0,Dimension-1);
                            Matrix data_j = traindata.getMatrix(j,j,0,Dimension-1);

                            double eta = get_inner_product(data_i.times(2),data_j)-get_inner_product(data_i,data_i)-get_inner_product(data_j,data_j);
                            double aerph_old_i = aerph.get(i,0);
                            double aerph_old_j = aerph.get(j,0);

                            double aerph_new_j = aerph_old_j - label.get(j,0)*(Ei-Ej)/eta;
                            aerph_new_j=aerph_new_j > Hup?Hup:aerph_new_j;
                            aerph_new_j=aerph_new_j < Lbottom?Hup:aerph_new_j;
                            if(Math.abs(aerph_new_j-aerph_old_j)<0.001)
                            {
                                continue;
                            }
                            double aerph_new_i = aerph_old_i+label.get(i,0)*label.get(j,0)*(aerph_old_j-aerph_new_j);
                            double B1 = B-Ei-label.get(i,0)*(aerph_new_i-aerph_old_i)*data_i.times(data_i.transpose()).get(0,0)-label.get(j,0)*(aerph_new_j-aerph_old_j)*data_i.times(data_j.transpose()).get(0,0);
                            double B2 = B-Ej-label.get(i,0)*(aerph_new_i-aerph_old_i)*data_i.times(data_j.transpose()).get(0,0)-label.get(j,0)*(aerph_new_j-aerph_old_j)*data_j.times(data_j.transpose()).get(0,0);
                            if(aerph_new_i>0 && aerph_new_i<C)
                            {
                                B=B1;
                            }else if(aerph_new_j>0 && aerph_new_j< C)
                            {
                                B=B2;
                            }else {
                                B=B1+B2;
                            }
                            aerph.set(i,0,aerph_new_i);
                            aerph.set(j,0,aerph_new_j);
                        }
                    }
                }
                iter++;
            }
            Equations result = new Equations(aerph,label,traindata,B);
            return result;
        }catch (Exception e)
        {
            e.printStackTrace();
            throw new HiveException("Error in SMO: "+e.getMessage());
        }
    }


    @Override
    public Matrix getLabel(HashMap<String,ArrayList> data_set) throws HiveException
    {
        int label_size = data_set.size();
        if(0>=label_size)
        {
            throw new HiveException("error at size of label");
        }else {
            double[][] label = new double[label_size][1];
            Iterator iter = data_set.entrySet().iterator();
            int counter = 0;
            while (iter.hasNext())
            {
                Map.Entry entry = (Map.Entry)iter.next();
                ArrayList<Double> value = (ArrayList<Double>) entry.getValue();
                double flag = value.get(0);
                label[counter][0]=flag;
                counter++;
            }
            return new Matrix(label);
        }
    }

    @Override
    public Matrix getTrain_data(int Dimension,HashMap<String,ArrayList> data_set) throws HiveException
    {
        if(data_set.size()<=0)
        {
            throw new HiveException("error at size of training data");
        }

        double[][] matrix = new double[data_set.size()][Dimension];
        Iterator iter = data_set.entrySet().iterator();
        int lines_num = 0;
        while (iter.hasNext())
        {
            Map.Entry entry = (Map.Entry)iter.next();
            ArrayList<Double> temp_list = (ArrayList<Double>)entry.getValue();
            for(int i=1;i<temp_list.size();i++)
            {
                matrix[lines_num][i-1]=temp_list.get(i);
            }
            lines_num++;
        }
        return new Matrix(matrix);
    }

    @Override
    public int get_Arraysize(HashMap<String, ArrayList> data_set){
        Iterator it = data_set.entrySet().iterator();
        if(it.hasNext())
        {
            Map.Entry entry = (Map.Entry)it.next();
            return  ((ArrayList)entry.getValue()).size();
        }
        return 0;
    }

    public static double get_inner_product(Matrix A,Matrix B)
    {
        Matrix result = A.times(B.transpose());
        double inner_product = 0;
        for(int i=0;i<result.getRowDimension();i++)
        {
            for(int j=0;j<result.getColumnDimension();j++)
            {
                inner_product = inner_product+result.get(i,j);
            }
        }
        return inner_product;
    }

    public static int getRandom_index(int exceot_index,int bond)
    {
        Random index = new Random();
        int result = (int)(index.nextDouble());
        while (result == exceot_index)
        {
            result = (int)(index.nextDouble()*bond);
        }
        return result;
    }


    public static double getEi(Matrix aerph,Matrix label,Matrix traindata,int index, int Dimension,double B)
    {
        try {
            Matrix Wt = aerph.arrayTimes(label).transpose();
            Matrix X = traindata.times(traindata.getMatrix(index,index,0,Dimension-1).transpose());
            double coefficient =Wt.times(X).get(0,0);
            return coefficient+B-label.get(index,0);
        }catch (Exception e)
        {
            e.printStackTrace();
        }
        return 0;
    }
}
