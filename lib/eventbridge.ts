import { PutEventsCommand, PutEventsCommandInput } from "@aws-sdk/client-eventbridge";
import { StreamToBatchCommand } from "../index.js";
import { eventBridgeClient } from "@microws/aws";

export function StreamToEventBridge<T>(
  format: (batch: Array<T>) => PutEventsCommandInput,
  options?: {
    concurrency: number;
    batchSize: number;
  },
) {
  return StreamToBatchCommand({
    concurrency: options?.concurrency || 4,
    batchSize: Math.min(options?.batchSize || 10, 10),
    command: async (batch: Array<T>) => {
      if (batch.length > 0) {
        try {
          let result = await eventBridgeClient.send(new PutEventsCommand(format(batch)));
          console.log(result);
        } catch (e) {
          throw new Error(e.message);
        }
        //@todo, need to actually do something with the results. Check for anything that failed, etc..
      }
    },
  });
}
