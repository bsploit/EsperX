import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.BasicConfigurator;

import redis.clients.jedis.Jedis;

import com.espertech.esper.client.*;



public class EsperX {
	private static UpdateListener myListener = new MyUpdateListener();
	public static void main(String[] args) {
		//Включаем log4j
		BasicConfigurator.configure();
		//определяем тип события
		Map<String, Object> logonEventDef = new HashMap<String, Object>();
	    logonEventDef.put("src_ip", String.class);
	    logonEventDef.put("login", int.class);
	    logonEventDef.put("result", String.class);
	    
	    //определяем тип события
	    String[] firewallPropsNames = 
	        {"src_ip", "src_port","dst_ip","dst_port","action"};
	    Object[] firewallpropsTypes = 
	        {String.class,int.class,String.class,int.class,String.class};
	    
	    //Создаем конфигурацию
	    Configuration engineConfig = new Configuration();
	    engineConfig.addEventType("antivirus", Antivirus.class.getName());
	    engineConfig.addEventType("logonEvent",logonEventDef);
	    engineConfig.addEventType("firewall",firewallPropsNames,firewallpropsTypes);
	    
	    //конфигурируем внешний источник (mysql)
/*	    ConfigurationDBRef mysql = new ConfigurationDBRef();	
	    mysql.setDriverManagerConnection("com.mysql.jdbc.Driver", 
	        "jdbc:mysql://localhost/testDB", "user", "password");
	    //mysql.setExpiryTimeCache(60, 120);	    
	    engineConfig.addDatabaseReference("mysql", mysql);
	    engineConfig.getEngineDefaults().getLogging().setEnableJDBC(true);
	*/    
	    //подключаем свою функцию, для использования внутри EPL
	    engineConfig.addPlugInSingleRowFunction("ipToInt", "MyEsperUtils", "ipToInt");
	    //конфигурируем движок
	    EPServiceProvider engine = EPServiceProviderManager.getDefaultProvider(engineConfig);
	    EPAdministrator admin = engine.getEPAdministrator();
	    //Пример правила для обнаружения брутфорса паролей
	    EPStatement rule = admin.createEPL("select * from logonEvent(result='fail').win:time(1 min) group by src_ip having count(*)>30");
	    
	    //определяем правило для отладки
	    String scanRule ="select src_ip,dst_ip,dst_port from firewall.win:time(30 sec) as fw"				
				+" group by fw.src_ip "
				+" having count(distinct fw.dst_ip) > 10" 
				+" output first every 1 hour";
	    
	    /*scanRule ="select src_ip,dst_ip,* from firewall as fw,"
				+ " sql:mysql ['select description from ipplan where ${ipToInt(src_ip)} between startaddr and endaddr'] as src_net,"
	    		+ " sql:mysql ['select description from ipplan where ${ipToInt(dst_ip)} between startaddr and endaddr'] as dst_net"
	    		+ " where src_net.description = 'wifi' and dst_net.description='database' and action='permit'" 
				+" output first every 1 hour";
				*/
	    EPStatement rulefw = admin.createEPL(scanRule);
	    
	    //подключаем листенер, для получения результатов
	    rule.addListener(myListener);
	    rulefw.addListener(myListener);
	    
	    EPRuntime runtime = engine.getEPRuntime();
	    //пример отправки события
	    runtime.sendEvent(new Antivirus("user-pc","c:\\windows\\virus.exe","Trojan"));
	    //пример отправки события		
		Map<String, Object> logonEvent = new HashMap<String, Object>();
	    logonEvent.put("src_ip", "10.0.0.1");
	    logonEvent.put("login", "root");
	    logonEvent.put("result", "fail");
		runtime.sendEvent(logonEvent,"logonEvent");

	    /*Object [] firewallEvent={"10.0.0.1",32000,"10.0.0.2",22,"permit"};
	    runtime.sendEvent(firewallEvent,"firewall");
	    */
		//запускаем тест для обнаружения сканирования
	    runScanTest(runtime);
	    //тест для проверки интеграции с редисом
	    //runRedisTest(engine);
	    
	    
	   
	}
	private static void runScanTest(EPRuntime runtime){
		
		for (int i=0; i<20;i++){
			Object [] fwEvent={"192.168.1.1",32000,"192.168.99."+i,22,"permit"};
			String[] firewallPropsNames = 
		        {"src_ip", "src_port","dst_ip","dst_port","action"};
			runtime.sendEvent(fwEvent,"firewall");
		}
		for (int i=0; i<20;i++){
			Object [] fwEvent={"10.0.0.1",32000,"10.0.0."+i,22,"permit"};
			String[] firewallPropsNames = 
		        {"src_ip", "src_port","dst_ip","dst_port","action"};
			runtime.sendEvent(fwEvent,"firewall");
		}
	}
	public static void fillSampleData() {
		//наполняем редис тестовыми данными
			Jedis redis = new Jedis("127.0.0.1");
			redis.connect();
			redis.del("input");
			for (int i=1;i<100;i++){
				String event ="{\"@timestamp\":\"2014-12-01T01:01:01Z\",\"src_ip\":\"10.0.0.1\",\"dst_ip\":\"%dst_ip%\",\"type\":\"fw\",\"timestamp\":\"timestamp\"}";
				String dst_ip="192.168.0."+i;
				event = event.replace("%dst_ip%", dst_ip);	  			
	  			redis.rpush("input", event);
			}
			redis.close();
	}
	private static void runRedisTest(EPServiceProvider engine){
			Redis redis = new Redis(engine.getEPRuntime());		
			Map<String, Object> fwEventDef = new HashMap<String, Object>();
			fwEventDef.put("@timestamp", String.class);
			fwEventDef.put("src_ip", String.class);
			fwEventDef.put("dst_ip", String.class);
			fwEventDef.put("dst_port", String.class);			
			engine.getEPAdministrator().getConfiguration().addEventType("fw",fwEventDef);
		
			EPStatement rule = engine.getEPAdministrator().createEPL("select * from fw.win:time(1 min) group by src_ip having count(distinct dst_ip)>20 output first every 1 min");
			RedisUpdateListener rListener = new RedisUpdateListener(redis, engine.getEPRuntime());
			rule.addListener(rListener);
			
			fillSampleData();				   
			
			while(true) {
				redis.take();
			}	        
	}

}
