import {
  BatchWriteCommand,
  BatchWriteCommandInput,
  QueryCommand,
  QueryCommandInput,
  TransactWriteCommand,
  TransactWriteCommandInput,
} from "@aws-sdk/lib-dynamodb";

import { TransactionCanceledException } from "@aws-sdk/client-dynamodb";
import { StreamReadableInfiniteLoop, StreamToBatchCommand } from "../index.js";
import { dynamodb } from "@microws/aws";
import { ReadableStream, WritableStream } from "node:stream/web";

export function StreamFromDynamoDB<T>(
  params: {
    /**
          Keys that make up the lastEvaluatedKey
          - ex. ["PK","SK"]
          - ex. ["PK", "SK", "GSI1PK", "GSI1SK"]
       */
    keys?: Array<string>;
    watch?: boolean;
    watchDelay?: number;
    /**
     * How many to store in memory that can't be processed
     */
    bufferLimit?: number;
    onCaughtUp?: (value?: unknown) => void;
  },
  query: QueryCommandInput,
) {
  const { keys, watch, watchDelay, bufferLimit, onCaughtUp } = {
    keys: ["PK", "SK"],
    watch: false,
    watchDelay: 1_000,
    bufferLimit: 5000,
    ...params,
  };
  let caughtup = false;
  return StreamReadableInfiniteLoop<
    | T
    | {
        _microws: true;
        caughtup: boolean;
        LastEvaluatedKey: Record<string, any>;
      }
  >(
    {
      highWaterMark: bufferLimit,
      onCaughtUp,
    },
    async (configuration) => {
      try {
        let { Items, LastEvaluatedKey } = await dynamodb.send(
          new QueryCommand({
            ...query,
            ExclusiveStartKey: configuration,
          }),
        );

        let nextKey = LastEvaluatedKey;
        if (!nextKey) {
          let last = Items.at(-1);
          nextKey = last
            ? keys.reduce((acc, key) => {
                acc[key] = last[key];
                return acc;
              }, {})
            : configuration;
        }
        if (!caughtup && !LastEvaluatedKey) {
          caughtup = true;
          Items.push({
            _microws: true,
            caughtup: true,
            LastEvaluatedKey: nextKey,
          });
        }
        return {
          entries: Items,
          configuration: nextKey,
          nextDelay: LastEvaluatedKey ? 0 : watchDelay,
          finished: !watch && !LastEvaluatedKey,
        };
      } catch (e) {
        throw new Error(e.message);
      }
    },
    undefined,
  );
}
export function StreamToDynamoDBBatchWrite<T>(
  format: (batch: Array<T>) => BatchWriteCommandInput,
  options?: {
    concurrency: number;
    batchSize: number;
  },
) {
  return StreamToBatchCommand({
    concurrency: options?.concurrency || 4,
    batchSize: Math.min(options?.batchSize || 25, 25),
    command: async (batch: Array<T>) => {
      if (batch.length > 0) {
        try {
          let result = await dynamodb.send(new BatchWriteCommand(format(batch)));
          console.log(result);
        } catch (e) {
          throw new Error(e.message);
        }
        //@todo, need to actually do something with the results. Check for anything that failed, etc..
      }
    },
  });
}
export function StreamToDynamoDTransactionWrite<T>(
  format: (batch: Array<T>) => TransactWriteCommandInput["TransactItems"],
  options?: {
    concurrency?: number;
    batchSize?: number;
    pruneConditionExpressionFailures?: boolean;
  },
): WritableStream<T> {
  let stream = StreamToBatchCommand<T>({
    concurrency: options?.concurrency || 10,
    batchSize: Math.min(options?.batchSize || 100, 100),
    command: async (batch: Array<T>) => {
      if (batch.length > 0) {
        let formattedBatch: TransactWriteCommandInput["TransactItems"];

        try {
          formattedBatch = format(batch);
          let written = formattedBatch.length;
          let pruned = 0;

          if (options.pruneConditionExpressionFailures) {
            for (let i = 0; i < 4; i++) {
              try {
                await dynamodb.send(
                  new TransactWriteCommand({
                    TransactItems: formattedBatch,
                  }),
                );
                written = formattedBatch.length;
                break;
              } catch (e) {
                if (e instanceof TransactionCanceledException) {
                  formattedBatch = e.CancellationReasons.map((reason, i) => {
                    if (reason.Code === "None") {
                      return formattedBatch[i];
                    }
                    return null;
                  }).filter(Boolean);
                  pruned = written - formattedBatch.length;
                  written = formattedBatch.length;
                } else {
                  throw e;
                }
              }
            }
          } else {
            await dynamodb.send(
              new TransactWriteCommand({
                TransactItems: formattedBatch,
              }),
            );
          }
          return { written, pruned };
        } catch (e) {
          if (e instanceof TransactionCanceledException) {
            throw new Error(
              JSON.stringify(
                e.CancellationReasons.map((reason, i) => {
                  if (reason.Code !== "None") {
                    return { ...reason, request: formattedBatch[i] };
                  }
                  return null;
                }).filter(Boolean),
                null,
                2,
              ),
            );
          } else {
            console.log(e);
            throw new Error(e.message);
          }
        }
        //@todo, need to actually do something with the results. Check for anything that failed, etc..
      } else {
        return {
          written: 0,
        };
      }
    },
  });
  let test = new ReadableStream({}).pipeThrough;
  let pruned = 0;
  let written = 0;

  let stuff = stream.readable
    .pipeThrough(
      StreamToBatchCommand({
        concurrency: 1,
        batchSize: options.concurrency,
        command: async (batch) => {
          return batch.reduce(
            (acc, row) => {
              acc.written += row.written;
              acc.pruned += row.pruned;
              return acc;
            },
            {
              written: 0,
              pruned: 0,
            },
          );
        },
      }),
    )
    .pipeTo(
      new WritableStream({
        write(event) {
          written += event.written;
          pruned += event.pruned || 0;
          console.log(`Wrote ${event.written} Pruned ${event.pruned || 0} - Total Written ${written} Pruned ${pruned}`);
        },
        close() {
          console.log(`Total: Wrote ${written} Pruned ${pruned}`);
        },
      }),
    );
  let writer = stream.writable.getWriter();
  return new WritableStream({
    async write(chunk) {
      await writer.write(chunk);
    },
    async close() {
      writer.close();
      await stuff;
    },
  });
}
