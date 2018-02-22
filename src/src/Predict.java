package src;

import net.sf.json.JSONObject;
import org.apache.hadoop.hive.ql.exec.UDF;

public class Predict extends UDF{
    public double evaluate(String args,String Coefficient)
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
            return temp+B;
        }
        return 0.0;
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
        String P_list[] = args.split(";");
        double[][] result = new double[P_list.length][];
        int i=0;
        for(String temp:P_list)
        {
            result[i]=get_value(temp,",");
            i++;
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
        String test="{\"b\":-3.4408511272001627,\"result_aerph\":\"0.1,0.018612475085614588,5.425186267673937E-4,0.019335792839417748,0.061509213448200276\",\"result_label\":\"1.0,-1.0,-1.0,-1.0,-1.0\",\"result_traindata\":\"4.366384527637392,4.366384527637392,4.366384527637392,4.366384527637392,4.366384527637392;2.448150184011463,2.448150184011463,2.448150184011463,2.448150184011463,2.448150184011463;1.942067694556055,1.942067694556055,1.942067694556055,1.942067694556055,1.942067694556055;2.198642626737815,2.198642626737815,2.198642626737815,2.198642626737815,2.198642626737815;2.617154226808659,2.617154226808659,2.617154226808659,2.617154226808659,2.617154226808659;\"}";
        Predict simple_test = new Predict();
        String simple = "threat_file:5,cactus:5,aegis_crack:5,waf:6,special:2";
        System.out.print(simple_test.evaluate(simple,test));
    }
}
