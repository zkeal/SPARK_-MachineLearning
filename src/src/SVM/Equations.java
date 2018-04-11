package src.SVM;

import Jama.Matrix;
import net.sf.json.JSONObject;
import org.apache.hadoop.hive.ql.metadata.HiveException;


public class Equations {
    String result_aerph;
    String result_label;
    String result_traindata;
    double B;

    public double getB() {
        return B;
    }

    public void setB(double b) {
        B = b;
    }

    public String getResult_aerph() {
        return result_aerph;
    }

    public void setResult_aerph(String result_aerph) {
        result_aerph = result_aerph;
    }

    public String getResult_label() {
        return result_label;
    }

    public void setResult_label(String result_label) {
        result_label = result_label;
    }

    public String getResult_traindata() {
        return result_traindata;
    }

    public void setResult_traindata(String result_traindata) {
        result_traindata = result_traindata;
    }

    public Equations(Matrix aerph,Matrix label,Matrix train_data,double B) throws HiveException {
        StringBuilder Saerph = new StringBuilder();
        StringBuilder Slabel = new StringBuilder();
        StringBuilder Strain_data = new StringBuilder();
        this.B = B;
        for(int i=0;i<aerph.getRowDimension();i++)
        {
            if(aerph.get(i,0)!=0)
            {
                Saerph.append(aerph.get(i,0));
                Slabel.append(label.get(i,0));
                for(int j=0;j < train_data.getColumnDimension();j++)
                {
                    Strain_data.append(train_data.get(i,j));
                    Strain_data.append(",");
                }
                Strain_data.append("#");
                Saerph.append(",");
                Slabel.append(",");
            }
        }
        try {
            if(Saerph.toString().isEmpty() || Saerph.equals(""))
            {
                result_aerph="";
                result_label="";
                result_traindata="";
            }else {
                result_aerph = Saerph.toString().substring(0, Saerph.length() -1);
                result_label = Slabel.toString().substring(0, Slabel.length() -1);
                result_traindata = Strain_data.toString().replaceAll(",#","#");
            }
        }catch (Exception e)
        {
            throw new HiveException("Saerph:"+Saerph.toString()+",Slalbel:"+Slabel.toString());
        }


    }
    //to get a formal string to predict elsewhere
    @Override
    public String toString()
    {
        return JSONObject.fromObject(this).toString();
    }
}
