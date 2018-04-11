package src.Apriori;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class algorithm {
    public ArrayList<String> type_sumer;
    public algorithm()
    {
        type_sumer = new ArrayList<String>();
    }

    public String get_Maxfrequent(HashMap<String,ArrayList<String>> data_set, Double min, int size)
    {

        Map<String,Integer> result_record = new HashMap<String, Integer>();
        ArrayList<ArrayList<String>> para = new ArrayList<ArrayList<String>>();
        for(Map.Entry<String,ArrayList<String>> temp:data_set.entrySet())
        {
            ArrayList<String> temp_value=temp.getValue();
            for(String index:temp_value)
            {
                if(result_record.containsKey(index))
                {
                    Integer old_value=result_record.get(index);
                    result_record.put(index,old_value+1);
                }
                else
                {
                    result_record.put(index,1);
                }
            }
        }
        for(Map.Entry<String,Integer> temp:result_record.entrySet())
        {
            double d_value = (double) temp.getValue().intValue();
            if(min<=(d_value/size))
            {
                ArrayList<String> node_1 = new ArrayList<String>();
                node_1.add(temp.getKey());
                para.add(node_1);
                type_sumer.add(temp.getKey());
            }
        }
        return get_Maxfrequent(data_set,para,min,size);
    }

    public String get_Maxfrequent(HashMap<String,ArrayList<String>> data_set,ArrayList<ArrayList<String>> parameter,Double min,int size)
    {
        Map<Integer,Integer> accumlated_result = new HashMap<Integer, Integer>();
        ArrayList<ArrayList<String>> para = new ArrayList<ArrayList<String>>();
        ArrayList<ArrayList<String>> new_list=get_newStoreList(parameter);
        for(Map.Entry<String,ArrayList<String>> temp:data_set.entrySet())
        {
            for(ArrayList<String> store_temp:new_list)
            {
                ArrayList<String> temp_value=temp.getValue();
                if(find_value(store_temp,temp_value))
                {
                    Integer inert_index= new_list.indexOf(store_temp);
                    if(accumlated_result.containsKey(inert_index))
                    {
                        Integer old_value=accumlated_result.get(inert_index);
                        accumlated_result.put(inert_index,old_value+1);
                    }
                    else {
                        accumlated_result.put(inert_index,1);
                    }
                }
            }
        }
        for(Map.Entry<Integer,Integer> temp_record:accumlated_result.entrySet())
        {
            double d_value = (double) temp_record.getValue().intValue();
            if(min<=(d_value/size))
            {
                ArrayList<String> iter = Lists.newArrayList(new_list.get(temp_record.getKey()));
                para.add(iter);
            }
        }
        if(null==para || 0==para.size())
        {
            StringBuilder SB = new StringBuilder();
            for(ArrayList<String> temp:parameter)
            {
                SB.append(temp.toString());
                SB.append(".");
            }
            return SB.toString();
        }
        else
            return get_Maxfrequent(data_set,para,min,size);

    }

    public ArrayList<ArrayList<String>> get_newStoreList(ArrayList<ArrayList<String>> origin_list)
    {
        ArrayList<ArrayList<String>> new_scanlist = new ArrayList<ArrayList<String>>();
        for(ArrayList<String> temp:origin_list)
        {
            for (String temp_i:type_sumer)
            {
                ArrayList<String> new_node = Lists.newArrayList(temp);
                String node_end = new_node.get(new_node.size()-1);
                if(!temp.contains(temp_i) && type_sumer.indexOf(temp_i)>type_sumer.indexOf(node_end))
                {
                    new_node.add(temp_i);
                    new_scanlist.add(new_node);
                }
            }
        }
        return new_scanlist;
    }

    public boolean find_value(ArrayList<String> short_list,ArrayList<String> long_list)
    {
        int i=0;
        for(String temp:short_list)
        {
            for(String temp_1:long_list)
            {
                if(temp.equals(temp_1))
                {
                    i++;
                }
            }
        }
        if (i==short_list.size())
        {
            return true;
        }
        else{
            return false;
        }
    }

}
