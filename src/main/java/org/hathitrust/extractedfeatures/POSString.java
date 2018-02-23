package org.hathitrust.extractedfeatures;

public class POSString {

	String _str;
	String[] _pos_tags;
	
	public POSString(String str, String[] pos_tags)
	{
		_str = str;
		_pos_tags = pos_tags;
	}
	
	public String getString()
	{
		return _str;
	}
	
	public String[] getPOSTags()
	{
		return _pos_tags;
	}
	
}
