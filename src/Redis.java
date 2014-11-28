
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import com.espertech.esper.client.EPRuntime;

import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class Redis {
  private String listnameIn="input";
  private String listnameOut="alerts";
  //private ConnectionFactory factory[];    
  private static JedisFactory jedisFactory;
  private EPRuntime runtime;
  static Logger logger = Logger.getLogger(Redis.class.getName());
  
  public Redis(EPRuntime runtime) {
    this.runtime = runtime;    
    jedisFactory = new JedisFactory();
  }

  public void take(){
	  Jedis  jedisTake = jedisFactory.getJedisPool().getResource();
	  try {
		  List<String> events = jedisTake.blpop(0,listnameIn);
		  if (events!=null){
			  String event = events.get(1);			  
			  try{
				  JSONObject eventJson = new JSONObject(event);
				  String type = eventJson.getString("type");
				  Map<String, Object> eventMap = new HashMap<String, Object>();
				  Iterator<String> keys = eventJson.keys();
				    while(keys.hasNext()){
				        String key = keys.next();
				        String value = eventJson.getString(key);
				        eventMap.put(key, value);
				    }
				  
				  runtime.sendEvent(eventMap,type);				  
				  
			  
			  }
			  catch (Exception ex){
				  logger.error(ex.toString());
				  ex.printStackTrace();
			  }
		  }
		  
	  }
	  catch (JedisConnectionException jex) {
		  logger.error("Exception during taking fromo Redis "+jex.toString());
		  if(jedisTake !=null){
			  jedisFactory.getJedisPool().returnBrokenResource(jedisTake);
		  }
      } 
	  catch (Exception ex){
		  ex.printStackTrace();
	  }
	  finally {
		  if(jedisTake !=null){
            jedisFactory.getJedisPool().returnResource(jedisTake);
		  }		  
       }

  }

  public void publish(String [] results) {
	  Jedis jedisPublish = jedisFactory.getJedisPool().getResource();
	  try {
		  Pipeline pipe = jedisPublish.pipelined();		  
		  String alert="";
		  try {
			  for(int i=0;i<results.length;i++) {	
				  alert = results[i];
				  //JSONObject eventJson = new JSONObject(tempObject);						  
				  //jedisPublish.rpush(listnameOut, eventJson.toString());
				  jedisPublish.rpush(listnameOut, alert);
			 }
			  pipe.sync();
		 } 
				  catch (JedisConnectionException jex) {
					  if(jedisPublish !=null){
			            jedisFactory.getJedisPool().returnBrokenResource(jedisPublish);
					  }
				  }
				  catch (Exception ex){
					  ex.printStackTrace();					  
				  }
				  finally {
					  if(jedisPublish !=null){
			            jedisFactory.getJedisPool().returnResource(jedisPublish);
			            
					  }					  
			        }
				  
	  }
	  catch(Exception e) {
    	logger.error("Failed to rpush event");
    	e.printStackTrace();
    	
	  }
  	}
}
class JedisFactory {
    private static JedisPool jedisPool;

    public JedisFactory() {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(128);
        jedisPool = new JedisPool(poolConfig,"127.0.0.1",6379);                 
    }

    public JedisPool getJedisPool() {
		return jedisPool;
	}

	
}