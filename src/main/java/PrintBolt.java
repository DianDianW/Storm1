import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.joda.time.DateTime;

import java.util.Map;

public class PrintBolt extends BaseRichBolt {
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    }

    @Override
    public void execute(Tuple input) {
        System.out.println(new DateTime().toString("HH:mm:ss") + "--------------------final bolt 开始运行--------------------");
        /*----------???????????????????????---------------------------*/
        Map<String, Integer> counts = (Map<String, Integer>) input.getValue(0);
        /*最后一个阶段,将最后的结果打印出来*/
        System.out.println(justForm(20-8)+"key"+justForm(20-8)+"      "+"value");
        for (Map.Entry<String, Integer> kv : counts.entrySet()) {
            /*这里的justForm()函数是为了保证格式一致*/
            System.out.println(kv.getKey() + justForm(kv.getKey().length()) + " 频数 : " + kv.getValue());
        }
    }

    //保证格式一致的私有方法  
    private String justForm(int length) {
        for (int i = 0; i < 20 - length; i++) {
            System.out.print(" ");
        }
        return "";
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}  