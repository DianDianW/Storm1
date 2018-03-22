import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.joda.time.DateTime;

import java.util.Map;

/**
 * 按照空格切分字符串,然后推出去 
 */
public class SplitBolt extends BaseRichBolt {

    private OutputCollector outputCollector;
    private int countTime = 0;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;
    }

    @Override
    public void execute(Tuple tuple) {  
        /*这是官方文档中 tuple.getString(int i)的解释: 
          Returns the String at position i in the tuple. If that field is not a String, you will get a runtime error. 
                public String getString ( int i); 
         */
        String sentence = tuple.getString(0);
        for (String word : sentence.split(" ")) {
            System.out.println(new DateTime().toString("HH:mm:ss") + "--------------------SplitBolt 开始运行--------------------\n" + "> > > >  第"+count() +"次发送数据,这次发送的是:" + word);
            outputCollector.emit(new Values(word));
        }
    }
    //这是为了得到当前一共发送了多少个单词了,加深理解  
    private int count() {
        return ++countTime;
    }

    /*在发射的时候,将接收方的tuple中的 key 设置为"word"*/
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}