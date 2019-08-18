import com.bigtable.Constant.Constant;
import com.bigtable.utils.HBaseFilterUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class ScanTest01 {
    /**
     * 18706287692,15133295266,2019-05-25 15:00:31,0683
     * 2019-04 2019 -07
     */
    public static void main(String[] args) throws IOException {
        Filter filter1 = HBaseFilterUtil.eqFilter("f1", "call1", Bytes.toBytes("18706287692"));
        Filter filter2 = HBaseFilterUtil.eqFilter("f1", "call1", Bytes.toBytes("18706287692"));

        Filter filter3 = HBaseFilterUtil.orFilter(filter1, filter2);
        // 时间大于等于
        Filter filter4 = HBaseFilterUtil.gteqFilter("f1", "buildTime", Bytes.toBytes("2019-04"));

        Filter filter5 = HBaseFilterUtil.lteqFilter("f1", "buildTime", Bytes.toBytes("2019-07"));
        // 与
        Filter filter6 = HBaseFilterUtil.andFilter(filter4, filter5);
        Filter filter7 = HBaseFilterUtil.andFilter(filter3, filter6);

        Scan scan = new Scan();
        scan.setFilter(filter7);
        Connection connection = ConnectionFactory.createConnection(Constant.CONF);
        Table table = connection.getTable(TableName.valueOf("ns_telecom:calllog"));
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("RK:" + Bytes.toString(CellUtil.cloneRow(cell))
                        + "CN:" + Bytes.toString(CellUtil.cloneQualifier(cell))
                        + "VALUE:" + Bytes.toString(CellUtil.cloneValue(cell))
                );
            }
        }
    }
}
