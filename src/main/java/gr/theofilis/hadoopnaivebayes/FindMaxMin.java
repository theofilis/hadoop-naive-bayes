package gr.theofilis.hadoopnaivebayes;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author konstantinosskianis
 */
public class FindMaxMin {

    public static void main(String args[]) {
        Scanner in = null;

        try {
            in = new Scanner(new File("adult.data"));
            int count = 0;
            int max_1st = Integer.MIN_VALUE;
            int min_1st = Integer.MAX_VALUE;

            int max_3 = Integer.MIN_VALUE;
            int min_3 = Integer.MAX_VALUE;

            int max_5 = Integer.MIN_VALUE;
            int min_5 = Integer.MAX_VALUE;

            int max_11 = Integer.MIN_VALUE;
            int min_11 = Integer.MAX_VALUE;

            int max_12 = Integer.MIN_VALUE;
            int min_12 = Integer.MAX_VALUE;

            int max_13 = Integer.MIN_VALUE;
            int min_13 = Integer.MAX_VALUE;

            while (in.hasNextLine()) {
                String[] words = in.nextLine().split(",");

                int value_1 = Integer.parseInt(words[0].trim());
                if (value_1 > max_1st) {
                    max_1st = value_1;
                }
                if (value_1 < min_1st) {
                    min_1st = value_1;
                }

                int value_3 = Integer.parseInt(words[2].trim());
                if (value_3 > max_3) {
                    max_3 = value_3;
                }
                if (value_3 < min_3) {
                    min_3 = value_3;
                }

                int value_5 = Integer.parseInt(words[4].trim());
                if (value_5 > max_5) {
                    max_5 = value_5;
                }
                if (value_5 < min_5) {
                    min_5 = value_5;
                }

                int value_11 = Integer.parseInt(words[10].trim());
                if (value_11 > max_11) {
                    max_11 = value_11;
                }
                if (value_11 < min_11) {
                    min_11 = value_11;
                }

                int value_12 = Integer.parseInt(words[11].trim());
                if (value_12 > max_12) {
                    max_12 = value_12;
                }
                if (value_12 < min_12) {
                    min_12 = value_12;
                }

                int value_13 = Integer.parseInt(words[12].trim());
                if (value_13 > max_13) {
                    max_13 = value_12;
                }
                if (value_13 < min_13) {
                    min_13 = value_13;
                }


            }

            System.out.println("for feature 1 Max is: " + max_1st + " and min: " + min_1st);
            System.out.println("for feature 3 Max is: " + max_3 + " and min: " + min_3);
            System.out.println("for feature 5 Max is: " + max_5 + " and min: " + min_5);
            System.out.println("for feature 11 Max is: " + max_11 + " and min: " + min_11);
            System.out.println("for feature 12 Max is: " + max_12 + " and min: " + min_12);
            System.out.println("for feature 13 Max is: " + max_13 + " and min: " + min_13);


        } catch (FileNotFoundException | NumberFormatException e) {
            e.printStackTrace();
        }
    }
}
