package cn.spark;

import java.util.ArrayList;
import java.util.Arrays;

public class LambdaTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ArrayList<String> list = new ArrayList<>(Arrays.asList("I", "love", "you", "too"));
		list.replaceAll(str -> {
		    if(str.length()>3)
		        return str.toUpperCase();
		    return str;
		});
		
		list.forEach(str->System.out.println(str));

	}
	
	

}
