import com.espertech.esper.client.*;


public class MyUpdateListener implements UpdateListener {
    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        if (newEvents != null) {
        	String eventType = newEvents[0].getEventType().toString();
          	Object event = newEvents[0].getEventType();
           	System.out.println("Event received "+eventType+" "
                       + newEvents[0].getUnderlying());
        }
     }        
}