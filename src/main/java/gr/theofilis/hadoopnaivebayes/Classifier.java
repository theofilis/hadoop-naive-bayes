package gr.theofilis.hadoopnaivebayes;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author konstantinosskianis
 */
public class Classifier {

    protected HashMap<String, Double> map = new HashMap<>();

    private void computeProbabilities(String filename, Map<String, Double> prob) {
        Scanner in = null;

        try {
            in = new Scanner(new File(filename));
            int countle=0; 
            int countgt = 0;
            while (in.hasNextLine()) {
                String[] words = in.nextLine().split(",");
                double prob_LE50K = 1.0;
                double prob_GT50K = 1.0;

                /*
                 for (int i = 0; i < words.length; i++) {
                 for (String key : prob.keySet()) {
                 if(key.contains(words[i])){
                 if (key.contains("_<=50K")) 
                 System.out.println(words[i]+" yolo "+key);
                 prob_LE50K = prob_LE50K*prob.get(key);
                            
                 }else if(key.contains(words[i])){
                 if(key.contains("_>50K"))
                 prob_GT50K = prob_GT50K*prob.get(key);
                 }


                 }
                 }*/
                int first = Integer.parseInt(words[0].trim());
                double pr_LE50K = 1.0;
                double pr_GT50K = 1.0;
                if (first < 35) {
                    pr_LE50K = prob.get("age_0.0_<=50K");
                    pr_GT50K = prob.get("age_0.0_>50K");
                } else if (first < 53) {
                    pr_LE50K = prob.get("age_1.0_<=50K");
                    pr_GT50K = prob.get("age_1.0_>50K");
                } else if (first < 71) {
                    pr_LE50K = prob.get("age_2.0_<=50K");
                    pr_GT50K = prob.get("age_2.0_>50K");
                } else if (first <= 90) {
                    pr_LE50K = prob.get("age_3.0_<=50K");
                    pr_GT50K = prob.get("age_3.0_>50K");
                }
                
                double sec_prob_le50k = 1.0;
                double sec_prob_gt50k =1.0;
                if(!words[1].trim().equals("?")){
                   sec_prob_le50k = prob.get("workclass_" + words[1].trim() + "_<=50K");
                   if(prob.containsKey("workclass_" + words[1].trim() + "_>50K")){
                        sec_prob_gt50k = prob.get("workclass_" + words[1].trim() + "_>50K"); 
                   }
                }

                int third = Integer.parseInt(words[2].trim());
                double third_pr_LE50K = 1.0;
                double third_pr_GT50K = 1.0;
                if (third < (12285+368105)) {
                    third_pr_LE50K = prob.get("fnlwgt_0.0_<=50K");
                    third_pr_GT50K = prob.get("fnlwgt_0.0_>50K");
                } else if (third < (12285+368105+368105)) {
                    third_pr_LE50K = prob.get("fnlwgt_1.0_<=50K");
                    third_pr_GT50K = prob.get("fnlwgt_1.0_>50K");
                } else if (third < (12285+368105+368105+368105)) {
                    third_pr_LE50K = prob.get("fnlwgt_2.0_<=50K");
                    third_pr_GT50K = prob.get("fnlwgt_2.0_>50K");
                } else if (third <= (12285+368105+368105+368105+368105)) {
                    third_pr_LE50K = prob.get("fnlwgt_3.0_<=50K");
                    //third_pr_GT50K = prob.get("fnlwgt_3.0_>50K");
                }
                
                double forth_prob_le50k = 1.0;
                double forth_prob_gt50k = 1.0;
                if(!words[3].trim().equals("?")){  
                    forth_prob_le50k = prob.get("education_" + words[3].trim() + "_<=50K");
                    if(prob.containsKey("education_" + words[3].trim() + "_>50K")){
                        forth_prob_gt50k = prob.get("education_" + words[3].trim() + "_>50K");
                    }
                }

                int fifth = Integer.parseInt(words[4].trim());
                double fifth_pr_LE50K = 1.0;
                double fifth_pr_GT50K = 1.0;
                if (fifth < 4) {
                    fifth_pr_LE50K = prob.get("education-num_0.0_<=50K");
                    fifth_pr_GT50K = prob.get("education-num_0.0_>50K");
                } else if (fifth < 8) {
                    fifth_pr_LE50K = prob.get("education-num_1.0_<=50K");
                    fifth_pr_GT50K = prob.get("education-num_1.0_>50K");
                } else if (fifth < 12) {
                    fifth_pr_LE50K = prob.get("education-num_2.0_<=50K");
                    fifth_pr_GT50K = prob.get("education-num_2.0_>50K");
                } else if (fifth <= 16) {
                    fifth_pr_LE50K = prob.get("education-num_3.0_<=50K");
                    fifth_pr_GT50K = prob.get("education-num_3.0_>50K");
                }

                double sixth_prob_le50k = prob.get("marital-status_" + words[5].trim() + "_<=50K");
                double sixth_prob_gt50k = prob.get("marital-status_" + words[5].trim() + "_>50K");

                double seventh_prob_le50k = prob.get("occupation_" + words[6].trim() + "_<=50K");
                double seventh_prob_gt50k = prob.get("occupation_" + words[6].trim() + "_>50K");

                double eigth_prob_le50k = prob.get("relationship_" + words[7].trim() + "_<=50K");
                double eigth_prob_gt50k = prob.get("relationship_" + words[7].trim() + "_>50K");

                double ninth_prob_le50k = prob.get("race_" + words[8].trim() + "_<=50K");
                double ninth_prob_gt50k = prob.get("race_" + words[8].trim() + "_>50K");

                double tenth_prob_le50k = prob.get("sex_" + words[9].trim() + "_<=50K");
                double tenth_prob_gt50k = prob.get("sex_" + words[9].trim() + "_>50K");

                int eleven = Integer.parseInt(words[10].trim());
                double eleven_pr_LE50K = 1.0;
                double eleven_pr_GT50K = 1.0;
                if (eleven < 33333) {
                    eleven_pr_LE50K = prob.get("capital-gain_0.0_<=50K");
                    eleven_pr_GT50K = prob.get("capital-gain_0.0_>50K");
                } else if (eleven < 66666) {
                    eleven_pr_LE50K = prob.get("capital-gain_1.0_<=50K");
                    //eleven_pr_GT50K = prob.get("capital-gain_1.0_>50K");
                } else if (eleven <= 99999) {
                    //eleven_pr_LE50K = prob.get("capital-gain_3.0_<=50K");
                    eleven_pr_GT50K = prob.get("capital-gain_3.0_>50K");
                }

                int twelve = Integer.parseInt(words[11].trim());
                double twelve_pr_LE50K = 1.0;
                double twelve_pr_GT50K = 1.0;
                if (twelve < 30) {
                    twelve_pr_LE50K = prob.get("capital-loss_0.0_<=50K");
                    twelve_pr_GT50K = prob.get("capital-loss_0.0_>50K");
                } else if (twelve < 50) {
                    twelve_pr_LE50K = prob.get("capital-loss_1.0_<=50K");
                    twelve_pr_GT50K = prob.get("capital-loss_1.0_>50K");
                } else if (twelve < 90) {
                    twelve_pr_LE50K = prob.get("capital-loss_2.0_<=50K");
                    twelve_pr_GT50K = prob.get("capital-loss_2.0_>50K");
                } else if (twelve < 100) {
                    twelve_pr_LE50K = prob.get("capital-loss_3.0_<=50K");
                    twelve_pr_GT50K = prob.get("capital-loss_3.0_>50K");
                }

                int thirteen = Integer.parseInt(words[12].trim());
                double thirteen_pr_LE50K = 1.0;
                double thirteen_pr_GT50K = 1.0;
                if (thirteen < 511) {
                    thirteen_pr_LE50K = prob.get("hours-per-week_0.0_<=50K");
                    thirteen_pr_GT50K = prob.get("hours-per-week_0.0_>50K");
                } else if (thirteen < 1021) {
                    thirteen_pr_LE50K = prob.get("hours-per-week_1.0_<=50K");
                    thirteen_pr_GT50K = prob.get("hours-per-week_1.0_>50K");
                } else if (thirteen < 1531) {
                    thirteen_pr_LE50K = prob.get("hours-per-week_2.0_<=50K");
                    thirteen_pr_GT50K = prob.get("hours-per-week_2.0_>50K");
                } else if (thirteen <= 2042) {
                    thirteen_pr_LE50K = prob.get("hours-per-week_3.0_<=50K");
                    thirteen_pr_GT50K = prob.get("hours-per-week_3.0_>50K");
                }

                
                double fourteen_prob_le50k=1.0;
                double fourteen_prob_gt50k=1.0;
                fourteen_prob_le50k = prob.get("native-country_" + words[13].trim() + "_<=50K");
                if(prob.containsKey("native-country_" + words[13].trim() + "_>50K")){
                    fourteen_prob_gt50k = prob.get("native-country_" + words[13].trim() + "_>50K");
                }

                prob_LE50K = pr_LE50K * sec_prob_le50k * third_pr_LE50K * forth_prob_le50k * fifth_pr_LE50K * sixth_prob_le50k * seventh_prob_le50k * eigth_prob_le50k * ninth_prob_le50k * tenth_prob_le50k * eleven_pr_LE50K * twelve_pr_LE50K * thirteen_pr_LE50K * fourteen_prob_le50k * 0.759;
                prob_GT50K = pr_GT50K * sec_prob_gt50k * third_pr_GT50K * forth_prob_gt50k * fifth_pr_GT50K * sixth_prob_gt50k * seventh_prob_gt50k * eigth_prob_gt50k * ninth_prob_gt50k * tenth_prob_gt50k * eleven_pr_GT50K * twelve_pr_GT50K * thirteen_pr_GT50K * fourteen_prob_gt50k * 0.241;
                //prob_GT50K = prob_GT50K * 0.241;
                System.out.println(prob_LE50K + " " + prob_GT50K);
                if(prob_LE50K>prob_GT50K){
                    ++countle;
                }else{
                    ++countgt;
                }
                
            }
            System.out.println(countle+" "+countgt);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public static void main(String[] args) {

        Map<String, Double> prob = Evaluate.readPropabilities("part-r-00000");

        for (String key : prob.keySet()) {

            System.out.println(key + " = " + prob.get(key));

        }

        Classifier c = new Classifier();
        //c.createNewFile();
        c.computeProbabilities("adult.data", prob);
    }
}
