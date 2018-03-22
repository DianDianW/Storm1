import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.TupleHelpers;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

public class CountBolt extends BaseRichBolt {

    private Map<String, Integer> counts = new HashMap<>();
    private OutputCollector outputCollector;


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        /*加入Tick时间窗口, 统计*/
        /*------------------?????????????????????????---------------------------*/
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 10);
        return conf;
    }

    @Override
    public void execute(Tuple tuple) {
        /*时间窗口定义为10s内的统计数据,统计完毕后,发射到下一阶段的bolt进行处理*/
        //发射完成后return结束,开始新一轮的事件窗口计数操作
        if (TupleHelpers.isTickTuple(tuple)) {/*来判断是否应该发射当前窗口数据*/
            System.out.println((new DateTime().toString("HH:mm:ss")) + "--------------------sumWordBolt 开始运行--------------------\n发送的数据内容是" + counts);
            outputCollector.emit(new Values(counts));
            return;
        }

        /*如果没有到发送时间,就继续统计wordcount*/
        String word = tuple.getStringByField("word");
        Integer count = counts.get(word);
        if (count == null) {
            count = 0;
        }
        count++;
        counts.put(word, count);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word_map"));
    }
}