package org.yellowleaf.flink.dataviewer;

import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.state.api.SavepointReader;
import org.apache.flink.streaming.api.datastream.DataStream;;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.yellowleaf.flink.dataviewer.ReaderFunction;
import org.apache.flink.statefun.flink.core.StatefulFunctionsJobConstants;

public class App
{
    public static void main(String[] args)
    {
        try {
            String organization = args[0];
            String savepointPath = args[1]; // E.g., file:///tmp/savepoints/savepoint-ba36a6-201fd659f759
            System.out.println("Reading savepoint from " + savepointPath);

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            SavepointReader savepoint = SavepointReader.read(env,
                                                             savepointPath,
                                                             new EmbeddedRocksDBStateBackend());
            DataStream keyedState = savepoint.readKeyedState(StatefulFunctionsJobConstants.FUNCTION_OPERATOR_UID,
                                                             new ReaderFunction(organization));
            keyedState.print();

            env.execute("Flink Data Viewer");
        } catch (java.lang.Exception e) {
            e.printStackTrace();
        }
    }
}
