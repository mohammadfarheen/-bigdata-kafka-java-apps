/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.kafkapart2;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Scanner;

/**
 *
 * @author Anvesh Rokanlawar
 */
public class Producer {
    private static Scanner in;
    static int shift=4;
    
    public static void main(String[] argv)throws Exception {
        if (argv.length != 1) {
            System.err.println("Please specify 1 parameters ");
            System.exit(-1);
        }
        String topicName = argv[0];
        in = new Scanner(System.in);
        System.out.println("Enter message(type exit to quit)");

        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArraySerializer");
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        org.apache.kafka.clients.producer.Producer producer = new KafkaProducer(configProperties);
        System.out.println("Enter the text for encryption");
        String line = in.nextLine();
        while(!line.equals("exit")) {
            ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topicName,encryption(line, shift));
            producer.send(rec);
            line = in.nextLine();
            
        }
        in.close();
        producer.close();
    }

    private static String encryption(String line, int shift) {
       StringBuffer sb= new StringBuffer(); 
  
        for (int i=0; i<line.length(); i++) 
        { 
            if (Character.isUpperCase(line.charAt(i))) 
            { 
                char ch = (char)(((int)line.charAt(i) + 
                                  shift - 65) % 26 + 65); 
                sb.append(ch); 
            } 
            else
            { 
                char ch = (char)(((int)line.charAt(i) + 
                                  shift - 97) % 26 + 97); 
                sb.append(ch); 
            } 
        } 
        return sb.toString(); 
    }
}