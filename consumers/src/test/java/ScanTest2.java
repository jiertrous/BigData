
import com.bigtable.Constant.Constant;
import com.bigtable.utils.HBaseScanUtiil;
import com.bigtable.utils.PropertityUtil;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


import java.io.IOException;
import java.text.ParseException;
import java.util.List;

public class ScanTest2 {

    //18706287692,15133295266,2019-05-25 15:00:31,0683
    public static void main(String[] args) throws ParseException, IOException {
        // 获取每一组的startRow和stopRow
        List<String[]> startStop = HBaseScanUtiil.getStartStop("18706287692",
                "2018-04", "2019-06");

        Connection connection = ConnectionFactory.createConnection(Constant.CONF);
        Table table = connection.getTable(TableName.valueOf(PropertityUtil.getPropertity().getProperty
                ("hbase.table.name")));
        while (HBaseScanUtiil.hasNext()) {
            //System.out.println(HBaseScanUtiil.next()[0]);
            String[] next = HBaseScanUtiil.next();
            System.out.println(next[0] + " | " + next[1]);
            Scan scan = new Scan(Bytes.toBytes(next[0]), Bytes.toBytes(next[1]));
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
//                for (Cell cell : result.rawCells()) {
//                    // 打印RowKey
//                    System.out.println(Bytes.toString(CellUtil.cloneRow(cell)));
//                }
                System.out.println(Bytes.toString(result.getRow()));

            }
        }
    }
}
