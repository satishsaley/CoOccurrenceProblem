package com.satishproj2.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Pair implements WritableComparable<Pair> {

	public Text firstWord;
	public Text secondWord;

	public Pair() {
		this.firstWord = null;
		this.secondWord = null;
	}
	
	public Pair(Text one, Text two) {
		this.firstWord = one;
		this.secondWord = two;
	}

	public Pair(String one,String two)
	{
		this.firstWord=new Text(one);
		this.secondWord=new Text(two); 
	}
	@Override
	public void readFields(DataInput arg0) throws IOException {
		
	//	firstWord.readFields(arg0);
		//secondWord.readFields(arg0);
		firstWord=new Text(arg0.readUTF());
		secondWord=new Text(arg0.readUTF());
		
	}

//	@Override
//	public int hashCode() {
//
//		return firstWord.hashCode();//^secondWord.hashCode();
//	}

	@Override
	public boolean equals(Object obj) {

		if (obj instanceof Pair) {
			Pair p = (Pair) obj;
			return (firstWord.equals(p.firstWord) && secondWord
					.equals(p.secondWord));
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return "(" +firstWord.toString() + "," + secondWord.toString() +")";
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		//out.writeInt(counter);
        //out.writeLong(timestamp);
		
        arg0.writeUTF(firstWord.toString());
        arg0.writeUTF(secondWord.toString());
        //firstWord.write(arg0);
		//secondWord.write(arg0);
	}

	@Override
	public int compareTo(Pair p) {
		
		Text star=new Text("*");
		if(firstWord.compareTo(p.firstWord)==0)
		{
			if(secondWord.compareTo(p.secondWord) ==0)
				return 0;
			else if(secondWord.toString().equals("*"))
				return -1;
			else if(p.secondWord.toString().equals("*"))
				return 1;
			else return secondWord.compareTo(p.secondWord);
		}
		else
		{
			return firstWord.compareTo(p.firstWord);
		}
		
		
		
//		Text star=new Text("*");
//		if(star.compareTo(p.secondWord) ==0)
//		{
//			return firstWord.compareTo(p.firstWord);
//		}
//		
//		
//		int difference = firstWord.compareTo(p.firstWord);
//
//		if (difference == 0) {// if firstWord is same then check second word
//			difference = secondWord.compareTo(p.secondWord);
//		}

	}

}
