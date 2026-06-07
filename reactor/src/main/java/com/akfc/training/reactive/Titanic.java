package com.akfc.training.reactive;

import reactor.core.publisher.Flux;

import java.util.Locale;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Goal: load the /titanic.csv resource (fields separated by ';') into a Flux of Customer
 * and run a few reactive queries on it.
 */
public class Titanic {

    private Flux<Customer> customers;

    public Titanic() {
        customers = loadData();
    }

    private Flux<Customer> loadData() {
        //TODO: read the CSV file reactively, making sure the reader is always closed
        //step 1: use Flux.using(resourceSupplier, sourceSupplier, resourceCleanup):
        //        - resource: a BufferedReader on Titanic.class.getResourceAsStream("/titanic.csv")
        //        - source:   Flux.fromStream(reader.lines())
        //        - cleanup:  close the reader (beware of the checked IOException)
        //step 2: map each line to a Customer:
        //        - parse the line with a Scanner using ";" as delimiter and Locale.US
        //        - fields, in order: pClass (int), survived (int, 0 = false), name (String),
        //          sex ("male" -> Sex.MAN, otherwise Sex.WOMAN), age (double)
        //        - careful: the age may be missing — use hasNextDouble() and default to -1
        return null;
    }

    public Flux<Customer> getCustomers() {
        return customers;
    }

    public static void main(String[] args) {
        Titanic t = new Titanic();
        //TODO: query the data
        //step 1: display the first 5 customers (take)
        //step 2: compute and display the average age of the men:
        //        - filter on sex == Sex.MAN and a valid age (> 0)
        //        - collect the elements, extract the ages and average them
        //          (hint: collectList then java streams, or look at MathFlux from reactor-extra)
        //bonus:  compute the average age per sex, and the survival rate per passenger class
    }

    record Customer(int pClass, boolean survived, String name, Sex sex, double age) {

        public String[] fullName() {
            Pattern p = Pattern.compile("(\\w+), (\\w+)\\. (.*)");
            Matcher m = p.matcher(name);
            if (m.find()) {
                return new String[] { m.group(2), m.group(1), m.group(3) };
            }
            return null;
        }
        @Override
        public String toString() {
            return String.format("%d\t%b\t%s\t%s\t%.2f", pClass, survived, name, sex.name(), age);
        }
    }

    enum Sex {
        MAN, WOMAN;
    }

}
