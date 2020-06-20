package com.tanknavy.hive;

/**
 * Author: Alex Cheng 6/18/2020 1:06 PM
 */
public class Demo {
    @Deprecated
    public void run(){
        System.out.println("this is father method");
    }

    public static void main(String[] args) {
        SubDemo subDemo = new SubDemo();
        subDemo.run();
    }
}

class SubDemo extends Demo{
    @Override
    public void run(){
        System.out.println("this is child method");
    }
}