package src;

import net.sf.json.JSONObject;
import org.apache.hadoop.hive.ql.exec.UDF;

public class PredictSvm extends UDF{
    public String evaluate(String args,String Coefficient)
    {
        if(!args.isEmpty() && !Coefficient.isEmpty())
        {
            double[] values = get_parameter(args);
            JSONObject json = JSONObject.fromObject(Coefficient);
            double B = json.getDouble("b");
            double[] aerph = get_value(json.getString("result_aerph"),",");
            double[] label = get_value(json.getString("result_label"),",");
            double[][] train_data = get_value(json.getString("result_traindata"));

            double temp =0;
            for(int i=0;i<aerph.length;i++)
            {
                temp=get_product(values,train_data[i])*aerph[i]*label[i]+temp;
            }
            return String.valueOf(temp+B);
        }
        return String.valueOf(0.0);
    }

    public double get_product(double[] matrix1,double[] matrix2)
    {
        if(matrix1.length==matrix2.length)
        {
            double inner_product = 0;
            for(int i=0;i<matrix1.length;i++)
            {
                inner_product=matrix1[i]*matrix2[i]+inner_product;
            }
            return inner_product;
        }else {
            return 0.0;
        }
    }


    public static double[] get_value(String args,String reg)
    {
        String P_list[] = args.split(reg);
        double[] result = new double[P_list.length];
        int i=0;
        for(String temp:P_list)
        {
            result[i]=(Double.parseDouble(P_list[i]));
            i++;
        }
        return result;
    }

    public double[][] get_value(String args)
    {
        String P_list[] = args.split("#");
        double[][] result = new double[P_list.length][];
        int i=0;
        for(String temp:P_list)
        {
            if (!temp.isEmpty())
            {
                result[i]=get_value(temp,",");
                i++;
            }

        }
        return result;
    }

    public static double[] get_parameter(String parameters)
    {
        String P_list[] = parameters.split(",");
        double[] result = new double[P_list.length];
        int i=0;
        for(String temp:P_list)
        {
            String[] key_value = temp.split(":");
            if(key_value.length!=2)
            {
                continue;
            }
            result[i]=(Double.parseDouble(key_value[1]));
            i++;
        }
        return result;
    }

    public static void main(String[] args)
    {
        String test="{\"b\":-111.29790328283006,\"result_aerph\":\"0.2,0.1704522559505279,0.0017687326930711685,0.006747985526357702,0.0011325604882561138,0.2,0.2,0.005219987161670672,0.004082006313533965,0.0034473008386169575,0.0011988700005446627,0.2,0.00595030102742088,0.2\",\"result_label\":\"1.0,-1.0,-1.0,-1.0,-1.0,1.0,1.0,-1.0,-1.0,-1.0,-1.0,-1.0,-1.0,-1.0\",\"result_traindata\":\"5.926133768341708,5.926133768341708,5.926133768341708,5.926133768341708,5.926133768341708#2.17368622747032,2.17368622747032,2.17368622747032,2.17368622747032,2.17368622747032#2.3820940441287437,2.3820940441287437,2.3820940441287437,2.3820940441287437,2.3820940441287437#2.9217560077569615,2.9217560077569615,2.9217560077569615,2.9217560077569615,2.9217560077569615#2.9948483964317427,2.9948483964317427,2.9948483964317427,2.9948483964317427,2.9948483964317427#5.374889171380197,5.374889171380197,5.374889171380197,5.374889171380197,5.374889171380197#7.403307433446136,7.403307433446136,7.403307433446136,7.403307433446136,7.403307433446136#0.9940782094619007,0.9940782094619007,0.9940782094619007,0.9940782094619007,0.9940782094619007#1.8360616806664711,1.8360616806664711,1.8360616806664711,1.8360616806664711,1.8360616806664711#2.2996407741050175,2.2996407741050175,2.2996407741050175,2.2996407741050175,2.2996407741050175#2.4323187277642173,2.4323187277642173,2.4323187277642173,2.4323187277642173,2.4323187277642173#-1.4173265279622664,-1.4173265279622664,-1.4173265279622664,-1.4173265279622664,-1.4173265279622664#2.906286500761733,2.906286500761733,2.906286500761733,2.906286500761733,2.906286500761733#0.43680156056676367,0.43680156056676367,0.43680156056676367,0.43680156056676367,0.43680156056676367#\"}";
        PredictSvm simple_test = new PredictSvm();
        String simple = "threat_file:5,cactus:5,aegis_crack:5,waf:6,special:2";
        System.out.print(simple_test.evaluate(simple,test));
    }
}
