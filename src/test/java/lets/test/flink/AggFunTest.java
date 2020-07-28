package lets.test.flink;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.planner.functions.aggfunctions.MaxWithRetractAggFunction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.apache.flink.table.api.Expressions.*;

public class AggFunTest {

    EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
    TableEnvironment tableEnv = TableEnvironment.create(settings);

    @ParameterizedTest
    @ValueSource(strings = {"", "TEMPORARY", "TEMPORARY SYSTEM"})
    public void testWithCreateFunction(String functionType) {
        initInput();

        String functionClass = "org.apache.flink.table.planner.functions.aggfunctions.MaxWithRetractAggFunction$DoubleMaxWithRetractAggFunction";

        String createFunQuery = String.format("CREATE %s FUNCTION a AS '%s'", functionType, functionClass);
        tableEnv.executeSql(createFunQuery);
        tableEnv.createTemporaryView("B", tableEnv.from("A")
                .groupBy($("symbol"))
                .select($("symbol"), call("a", $("price")))
        );

        Table res = tableEnv.from("B");

        res.execute().print();
    }

    @Test
    public void testWithRegisterFunction() {
        initInput();

        tableEnv.createTemporarySystemFunction("max_value", MaxWithRetractAggFunction.DoubleMaxWithRetractAggFunction.class);

        Table res = tableEnv.sqlQuery("select max_value(price) as max_price from A group by symbol");

        res.execute().print();
    }

    private void initInput() {
        Table table = tableEnv.fromValues(DataTypes.ROW(
                DataTypes.FIELD("price", DataTypes.DOUBLE().notNull()),
                DataTypes.FIELD("symbol", DataTypes.STRING().notNull())
                ),
                row(1.0, "S"), row(2.0, "S"));
        tableEnv.createTemporaryView("A", table);
    }

}
