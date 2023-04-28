package org.janusgraph.zk;

/**
 * @author: mahao
 * @date: 2021/7/14
 */
public class ParentTest {

    public static void main(String[] args) {
        Son son = new Son();
        Person person = (son).num();
    }
}

class Person {

    public Person num() {
        System.out.println("parent num()");
        return null;
    }

}

class Son extends Person {

}