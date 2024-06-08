import { createReadStream } from "fs";
import {
  ReadableStream,
  ReadableStreamController,
  TransformStream,
  TransformStreamDefaultController,
  WritableStream,
} from "node:stream/web";
import { parse, Options as ParseOptions } from "csv-parse";
import { Readable, pipeline } from "stream";
export * from "./aws.js";

export function StreamReadableInfiniteLoop<T>(
  { highWaterMark, onCaughtUp }: { highWaterMark: number; onCaughtUp: (value?: unknown) => void },
  func: (configuration: any) => Promise<{
    nextDelay: number;
    entries: Array<any>;
    configuration: any;
    finished: boolean;
  }>,
  initialConfiguration: any,
) {
  let configuration = initialConfiguration;
  let timeout = null;
  let pause = false;

  async function loadItems(controller: ReadableStreamController<T>) {
    if (timeout) clearTimeout(timeout);
    if (!pause) {
      let result = await func(configuration);
      configuration = result.configuration;
      for (let i = 0; i < result.entries.length; i++) {
        controller.enqueue(result.entries[i]);
      }
      if (controller.desiredSize < 0) {
        pause = true;
      }
      if (result.finished) {
        controller.close();
      } else {
        timeout = setTimeout(() => loadItems(controller), result.nextDelay);
      }
      return result;
    }
  }

  let stream = new ReadableStream<T>(
    {
      async start(controller) {
        await loadItems(controller);
      },
      async pull(controller) {
        pause = false;
        await loadItems(controller);
      },
    },
    {
      highWaterMark,
    },
  );
  return stream;
}
export function StreamCount(params?: { name?: string; size?: number }) {
  let count = 0;
  let { name, size } = { name: "Count", size: 1_000, ...params };

  return new TransformStream({
    async transform(chunk) {
      if (++count % size == 0) {
        console.log(`${name}: ${count}`);
      }
    },
    flush() {
      console.log(`${name}: ${count}  Final`);
    },
  });
}
export function StreamObjectsToJsonLines<T>(options?: any) {
  return new TransformStream<T>({
    transform(chunk, controller) {
      controller.enqueue(JSON.stringify(chunk) + "\n");
    },
  });
}
export function SplitNewLines() {
  return new TransformStream({
    async start(controller) {
      this.buffer = "";
    },
    transform(chunk, controller) {
      let previous = 0;
      for (let i = 0; i < chunk.length; i++) {
        if (chunk[i] == "\n") {
          try {
            controller.enqueue(this.buffer + chunk.slice(previous, i));
          } catch (e) {
            console.log(e);
          }
          this.buffer = "";
          previous = i + 1;
        }
      }
      if (previous <= chunk.length) {
        this.buffer += chunk.slice(previous);
      }
    },
    flush(controller) {
      if (this.buffer.length) {
        try {
          controller.enqueue(this.buffer);
        } catch (e) {
          console.log(e);
        }
      }
    },
  });
}
export function StreamJSONLinesToObjects<T>() {
  const decoder = new TextDecoder();

  return new TransformStream<string | Uint8Array, T>({
    async start(controller) {
      this.buffer = "";
    },
    transform(chunk, controller) {
      let previous = 0;
      if (typeof chunk !== "string") {
        chunk = decoder.decode(chunk);
      }
      for (let i = 0; i < chunk.length; i++) {
        if (chunk[i] == "\n") {
          try {
            controller.enqueue(JSON.parse(this.buffer + chunk.slice(previous, i)));
          } catch (e) {
            console.log(e);
          }
          this.buffer = "";
          previous = i + 1;
        }
      }
      if (previous <= chunk.length) {
        this.buffer += chunk.slice(previous);
      }
    },
    flush(controller) {
      if (this.buffer.length) {
        try {
          controller.enqueue(JSON.parse(this.buffer));
        } catch (e) {
          console.log(e);
        }
      }
    },
  });
}
export function StreamFromCSVFile<T>(filename: string, options: ParseOptions): ReadableStream<T> {
  return Readable.toWeb(
    createReadStream(filename).pipe(
      parse({
        ...options,
        bom: true,
      }),
    ),
  );
}
export function StreamLog<T>(stringify?: boolean) {
  return new TransformStream<T, T>({
    transform(chunk, controller) {
      if (stringify) {
        console.log(JSON.stringify(chunk, null, 2));
      } else {
        console.log(chunk);
      }
      controller.enqueue(chunk);
    },
  });
}

export function StreamToBatchCommand<
  T,
  S = {
    written: number;
    pruned?: number;
  },
>({
  concurrency = 1,
  batchSize = 50,
  command,
}: {
  concurrency: number;
  batchSize: number;
  command: (batch: Array<T>) => Promise<S> | Promise<void>;
}): TransformStream<T, S> {
  let buffer: Array<T> = [];
  let processingCommands = new Set();
  let resolve: any = false;
  let reject: any = false;

  async function queue(controller: TransformStreamDefaultController<S>) {
    const batch = buffer.splice(0, batchSize);
    let promise = command(batch);
    processingCommands.add(promise);
    promise
      .then((result) => {
        processingCommands.delete(promise);
        if (processingCommands.size < concurrency && resolve !== false) {
          resolve();
          resolve = false;
        }
        controller.enqueue(result);
      })
      .catch((e) => {
        processingCommands.delete(promise);
        if (processingCommands.size < concurrency && resolve !== false) {
          reject(e);
          resolve = false;
        }
      });
    if (processingCommands.size >= concurrency && !resolve) {
      // console.log("Waiting ", processingCommands.size);
      await new Promise((res, rej) => {
        resolve = res;
        reject = rej;
      });
    } else {
      // console.log("NOT Waiting ", processingCommands.size);
    }
  }

  return new TransformStream<T, S>({
    async transform(chunk, controller) {
      buffer.push(chunk);
      if (buffer.length >= batchSize) {
        await queue(controller);
      }
    },
    async flush(controller) {
      if (buffer.length >= 0) {
        await queue(controller);
      }
      await Promise.all(processingCommands.values());

      //now let's just finish everything up
      concurrency = 1;
      while (buffer.length > 0) {
        await queue(controller);
      }
    },
  });
}
export function StreamToDevNull() {
  return new WritableStream();
}
