package com.yaoqi.hbase.component.assertion;

import org.junit.Test;

/**
 * @Author YaoQi
 * @Date 2018/8/13 17:17
 * @Modified
 * @Description
 */
public class AssertTest {

    @Test
    public void hasLengthBatch() {

    }

    @Test
    public void notNullBatch() {
        Object a = null;
        String[] b = {"a", "b"};
        Assert.notNullBatch(a, b);
    }

    @Test
    public void testStr() {
        String a = "aa";
        String b = "aa";
        System.out.println(b.contains(a));
    }

    public static void main(String[] args) {
        for (;;){
            System.out.println("fuck");
        }
    }

}