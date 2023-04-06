package org.yellowleaf.flink.dataviewer;

import java.util.Base64;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.statefun.flink.core.types.remote.RemoteValueTypeInfo;
import org.apache.flink.statefun.sdk.state.RemotePersistedValue;
import org.apache.flink.statefun.sdk.TypeName;
import org.apache.flink.util.Collector;

public class ReaderFunction extends KeyedStateReaderFunction<byte[], String>
{
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(ReaderFunction.class);

    ValueState<byte[]> state;
    String organization;

    public ReaderFunction(String organization)
    {
        this.organization = organization;
    }

    @Override
    public void open(Configuration parameters)
    {
        System.out.println("ReaderFunction.open() with parameters: " + parameters);

        TypeName type = TypeName.parseFrom(organization + ".events.desk/histories");
        TypeInformation<byte[]> typeInfo = new RemoteValueTypeInfo(type);
        System.out.println("typeInfo: " + typeInfo);
        ValueStateDescriptor<byte[]> stateDescriptor
            = new ValueStateDescriptor<>(organization + ".desk.history", typeInfo);

        state = getRuntimeContext().getState(stateDescriptor);
    }

    @Override
    public void readKey(
        byte[] key,
        Context ctx,
        Collector<String> out) throws Exception
    {
        System.out.println("ReaderFunction.readKey() with key: " + key);
        out.collect(Base64.getEncoder().encodeToString(key));
    }
}
