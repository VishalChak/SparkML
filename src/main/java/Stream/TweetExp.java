package Stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import twitter4j.Status;
import twitter4j.auth.Authorization;
import twitter4j.auth.AuthorizationFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationContext;

public class TweetExp {
	public static void main(String[] args) throws InterruptedException {
		
		System.setProperty("twitter4j.oauth.consumerKey","ejjRs1TOnKU9AtG9zWfxeD4Q3");
	    System.setProperty("twitter4j.oauth.consumerSecret","8www8r0Psw3tVelF1fSFOgOzWwiZxSQKEFsXqGmI2f92LdsbxB");
	    System.setProperty("twitter4j.oauth.accessToken","1571251572-dJcN2h7dwNxebBG9g8jFBJlIPSyrUyq10QsWgcg");
	    System.setProperty("twitter4j.oauth.accessTokenSecret","gBG1vs40qCAsQjrSzLc6vKnrJcMzD4Bx7IoJXQBHY8g2o");
		
		Configuration twitterConf = ConfigurationContext.getInstance();
		Authorization twitterAuth = AuthorizationFactory.getInstance(twitterConf);
		SparkConf sparkConf = new SparkConf().setAppName("Tweets Android").setMaster("local[2]");
		
		JavaStreamingContext sc = new JavaStreamingContext(sparkConf, new Duration(5000));
		String[] filters = { "#LiveStreaming","#India","#INDIA","#MODI","#MOVIE"  };
		JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(sc, twitterAuth, filters);
		
		JavaDStream<String> statuses = stream.map(new Function<Status, String>() {

			public String call(Status arg0) throws Exception {
				System.out.println(arg0);
				return arg0.getText();
			}
			
		});
		
		statuses.print();
		
		sc.start();
		sc.awaitTermination();
	}
}
