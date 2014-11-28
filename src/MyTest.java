

import java.util.Map;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class MyTest {
	private Jedis redis;
		
	public void fillSampleData() {
		redis = new Jedis("127.0.0.1");
		redis.connect();
	    //redis.del("input");
	    	
	    	  for (int i=1;i<100;i++){
	    		String event ="{\"@timestamp\":\"2014-12-01T01:01:01Z\",\"src_ip\":\"10.0.0.1\",\"dst_ip\":\"%dst_ip%\",\"type\":\"fw\",\"timestamp\":\"timestamp\"}";
	  			String dst_ip="192.168.0."+i;
	  			event = event.replace("%dst_ip%", dst_ip);	  			
	  			try{
	  				redis.rpush("input", event);
	  			}
	  			catch (Exception ex){
	  				System.out.println(ex.toString());
	  			}
	  		}	    	  
	  }
}
