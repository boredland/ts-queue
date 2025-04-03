export class TimeoutError extends Error {
    constructor(ms: number) {
        super(`Timeout after ${ms}ms`);
        this.name = "TimeoutError";
    }
}

export const timedPromise = <T>(promise: Promise<T>, timeout: number) => {
    return new Promise<T>((resolve, reject) => {
        const timeoutId = setTimeout(() => {
            reject(new TimeoutError(timeout));
        }, timeout);

        promise.then(resolve, reject).finally(() => {
            clearTimeout(timeoutId);
        });
    });
};