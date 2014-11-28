import com.espertech.esper.client.*;
import com.espertech.esper.client.util.JSONEventRenderer;


public class RedisUpdateListener implements UpdateListener {
	private Redis redis;
	private JSONEventRenderer jsonRenderer;
	private EPRuntime runtime;
	public RedisUpdateListener(Redis redis,EPRuntime runtime){
		this.redis = redis;
		this.runtime = runtime;
	}
    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        if (newEvents != null) {
        	String [] output = new String[newEvents.length];
        	for(int i=0; i<newEvents.length; i++){
        		EventType eventType = newEvents[i].getEventType();
            	jsonRenderer = runtime.getEventRenderer().getJSONRenderer(eventType);
            	output[i] = jsonRenderer.render(newEvents[i]);
            }
        	redis.publish(output);          	
        }
     }        
}