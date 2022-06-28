package org.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

public class ReduceMovieset extends Reducer<Text , Text,Text , Text>{
    public void reduce(Text key , Iterable<Text> values , Context context) throws IOException, InterruptedException {

        int count = 0;
        int countNetflix = 0;
        for (Text val : values){

            String[] company = val.toString().split("/");
            for(String temppp : company){
                if(temppp.contains("\"")){
                    String[] yo = temppp.split("\"");
                    for (String haa : yo){
                        if(haa.trim().equalsIgnoreCase("Netflix")){
                            countNetflix++;
                        }
                    }
                }else{
                    if(temppp.trim().equalsIgnoreCase("Netflix")){
                        countNetflix++;
                    }
                }
            }


            count++;
        }
        int last = Character.getNumericValue( key.charAt(1));
        int first = Character.getNumericValue( key.charAt(0));
        Text tahun = new Text();
        for (int i = 0 ; i < 10 ; i++){
            for (int t = 0; t < 10 ; t++){
                if(i== first && t == last){
                    tahun.set("20"+first+last);
                }
            }
        }
        HashMap<String , String> value = new HashMap<String ,String>();
        value.put("TOTAL FILM " , String.valueOf(count));
        value.put("NETFLIX " , String.valueOf(countNetflix));
        context.write(tahun, new Text(value.toString()) );
    }
}
