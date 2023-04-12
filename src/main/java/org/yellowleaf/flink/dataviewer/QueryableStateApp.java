package org.yellowleaf.flink.dataviewer;

import java.util.Base64;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.queryablestate.client.QueryableStateClient;
import org.apache.flink.statefun.flink.core.types.remote.RemoteValueTypeInfo;
import org.apache.flink.statefun.sdk.TypeName;

public class QueryableStateApp
{
    public static void main(String[] args)
    {
        try {
            // kubectl cp target/flink-data-viewer-1.0-SNAPSHOT-jar-with-dependencies.jar "$(kubectl get pod -o name | grep worker | head -1 | cut -d/ -f2):/tmp"
            // kubectl describe svc statefun-queryable-state | grep Endpoints
            // kubectl exec -it $(kubectl get pod -o name | grep worker | head -1) -- java -cp /tmp/flink-data-viewer-1.0-SNAPSHOT-jar-with-dependencies.jar org.yellowleaf.flink.dataviewer.QueryableStateApp officespacesoftware.com $(curl -s http://localhost:8081/v1/jobs | jq -r '.jobs[].id') 10.40.6.140 gabriel-huddle-dicksonlabs/186/155489

            String organization = args[0];
            String jobId = args[1];
            String address = args[2];
            String key = args[3];

            TypeName type = TypeName.parseFrom(organization + ".events.desk/histories");

            TypeInformation<byte[]> typeInfo = new RemoteValueTypeInfo(type);
            ValueStateDescriptor<byte[]> stateDescriptor
                = new ValueStateDescriptor<>(organization + ".desk.history", typeInfo);

            QueryableStateClient client = new QueryableStateClient(address, 9069);

            CompletableFuture<ValueState<byte[]>> responseFuture =
                client.getKvState(
                    JobID.fromHexString(jobId),
                    "gmd",
                    key,
                    BasicTypeInfo.STRING_TYPE_INFO,
                    stateDescriptor);

            System.out.println("Waiting for a response...");
            ValueState<byte[]> response = responseFuture.get();

            System.out.println("Base64 value (https://protogen.marcgravell.com/decode): "
                               + Base64.getEncoder().encodeToString(response.value()));
        } catch (Exception e) {
            System.out.println("Exception: " + e);
        }
    }
}
