package textAnalytica;

public class ttee {
	public static void main(String[] args) {
		String str = "The root problem is that, once you've made the first replacement, you can not work again with the same initially given string. I think the correct way of doing it would be using different copies and moving from one to the other the content while it is being replaced May be, better than this, the solution could be just do an extra replacement after each done replacement to erase the already replaced patterns.";
		System.out.println(str);
		str = str.toLowerCase().replaceAll(stopWordRegex(), "");
		str = str.replaceAll("[^a-zA-Z0-9 .]", "");
		str = str.replaceAll("\\s+", " ");
		str = str.trim();
		System.out.println(str);
	}
	
	public static String stopWordRegex() {
		String stopWords = "a,able,about,across,after,all,almost,also,am,among,an,and,any,are,as,at,be,because,been,but,by,can,cannot,could,dear,did,do,does,either,else,ever,every,for,from,get,got,had,has,have,he,her,hers,him,his,how,however,i,if,in,into,is,it,its,just,least,let,like,likely,may,me,might,most,must,my,neither,no,nor,not,of,off,often,on,only,or,other,our,own,rather,said,say,says,she,should,since,so,some,than,that,the,their,them,then,there,these,they,this,tis,to,too,twas,us,wants,was,we,were,what,when,where,which,while,who,whom,why,will,with,would,yet,you,your,'s,'ve";
		return "\\b( ?" + stopWords.replace(",", "|")+ ")\\b";
	}
}
