import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.joda.time.DateTime;

import java.util.Map;

/**
 * 创建数据 
 */
public class CreateSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private String[] sentences = null; //用来存放数据  


    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        sentences = new String[]{"Hahahaha! I am coming ~~"}; // "Hahaha...这个字符串即为要源源不断发送的信息"  
    }

    @Override
    public void nextTuple() {
        /*storm会循环的调用这个方法*/
        /*线程进行休眠,10s发送一次数据,在这10s内,让其余工作进行*/
        Utils.sleep(10000);
        //获得数据源  
        System.out.println(new DateTime().toString("HH:mm:ss") + "--------------CreateSpout 开始发送数据----------");
        this.collector.emit(new Values(sentences)); //发送出去  
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
//发送时设置接收方的Tuple实例中的key  
        outputFieldsDeclarer.declare(new Fields("sentence"));
    }

}