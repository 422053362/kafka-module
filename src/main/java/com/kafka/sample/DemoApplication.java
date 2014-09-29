package com.kafka.sample;

import org.spring.integeration.kafka.producer.IKafKaProducer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

/**
 * Created by peng on 2014/6/11.
 */
public class DemoApplication {

    public static void main(String[] args) throws InterruptedException {
        ApplicationContext context = new ClassPathXmlApplicationContext("application-consumer.xml");
        context.getBean("demoConsumer");
        System.out.println("开始等待");

        Thread.sleep(5000);

        ApplicationContext context1 = new ClassPathXmlApplicationContext("application-producer.xml");
        IKafKaProducer producer = (IKafKaProducer) context1.getBean("demoProducer");
        System.out.println("开始发送key-value");
        int i = 100;
        while(i>0) {
            producer.sendAsync("textKey", "testValue");
            i--;
            Thread.sleep(1000);
        }
        System.out.println("结束发送key-value");
    }

}
