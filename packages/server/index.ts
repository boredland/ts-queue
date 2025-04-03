import { Queue, Worker } from "bullmq";
import IORedis from "ioredis";
import { RedisMemoryServer } from "redis-memory-server";
import { Elysia, t } from "elysia";
import { hasher } from 'node-object-hash';
import { timedPromise } from "./utils";

const redisServer = new RedisMemoryServer();

await redisServer.ensureInstance();

console.log("Redis server started on port", await redisServer.getPort());

const connection = new IORedis({
    maxRetriesPerRequest: null,
    host: await redisServer.getHost(),
    port: await redisServer.getPort(),
});

type Payload = {
    body: string;
    destination: string;
    contentType: string;
    errorCallback?: string;
    timeout: number;
};

const { hash } = hasher({ sort: true, coerce: true });

const app = new Elysia()
    .state("queues", {} as Record<string, Queue<Payload>>)
    .state("workers", {} as Record<string, Worker<Payload>>)
    .post(
        "/enqueue",
        ({ body, store }) => {
            store.queues[body.queueName] =
                store.queues[body.queueName] ??
                new Queue<Payload>(body.queueName, {
                    connection,
                });
            store.workers[body.queueName] =
                store.workers[body.queueName] ??
                new Worker<Payload>(
                    body.queueName,
                    async (job) => {
                        try {
                            await timedPromise((async () => {
                                await new Promise((resolve) => setTimeout(resolve, job.data.timeout));
                                const response = await fetch(job.data.destination, {
                                    method: "POST",
                                    headers: {
                                        "Content-Type": job.data.contentType,
                                    },
                                    body: job.data.body,
                                });
                                return response.text();
                            })(), job.data.timeout)
                        } catch (error) {
                            console.error("Error processing job:", error);
                            if (job.data.errorCallback) {
                                await fetch(job.data.errorCallback, {
                                    method: "POST",
                                    headers: {
                                        "Content-Type": "application/json",
                                    },
                                    body: JSON.stringify({
                                        error: error instanceof Error ? error.message : "Unknown error",
                                        jobId: job.id,
                                        queueName: job.queueName,
                                        destination: job.data.destination,
                                        body: job.data.body,
                                        contentType: job.data.contentType,
                                        timeout: job.data.timeout,
                                        delay: job.opts.delay,
                                        deduplicationId: job.opts.deduplication?.id,
                                    }),
                                });
                            }
                        }
                    },
                    {
                        connection,
                        concurrency: body.parallelism,
                    },
                );

            // biome-ignore lint/style/noNonNullAssertion: we ensured the worker exists
            store.workers[body.queueName]!.concurrency = body.parallelism;

            const deduplicationId = body.deduplicationId ?? body.contentBasedDeduplication ? hash(body.body) : undefined

            store.queues[body.queueName]?.add(
                `${body.queueName}::${body.destination}`,
                {
                    body: body.body,
                    destination: body.destination,
                    contentType: body["Content-Type"],
                    errorCallback: body.errorCallback,
                    timeout: body.timeout * 1000,
                },
                {
                    delay: body.delay * 1000,
                    attempts: body.retries,
                    backoff: {
                        type: "exponential",
                    },
                    deduplication: deduplicationId ? { id: deduplicationId } : undefined,
                },
            );
        },
        {
            body: t.Object({
                // handler options
                queueName: t.String(),
                destination: t.String(),
                body: t.String(),
                errorCallback: t.Optional(t.String()),
                "Content-Type": t.String({
                    default: "application/json",
                }),
                timeout: t.Number({
                    default: 30,
                    description: "Processing timeout in seconds",
                }),

                // worker options
                parallelism: t.Number({
                    default: 1,
                }),
                delay: t.Number({
                    default: 0,
                    description: "Delay until the job is processed in seconds",
                }),
                retries: t.Number({
                    default: 0,
                }),
                deduplicationId: t.Optional(t.String()),
                contentBasedDeduplication: t.Boolean({
                    default: false,
                }),
            }),
        },
    )
    .listen(3000);
