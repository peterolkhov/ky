import {getEventListeners} from 'node:events';
import {setImmediate as nextTurn} from 'node:timers/promises';
import test from 'ava';
import ky from '../source/index.js';

for (const source of ['beforeRequest', 'afterResponse', 'fetch'] as const) {
	for (const method of ['text', 'json', 'arrayBuffer', 'blob', 'bytes', 'formData'] as const) {
		for (const timeout of [false, 10_000, 'total'] as const) {
			test(`abort settles a deferred ${source} ${method} body with ${timeout} timeout`, async t => {
				const controller = new AbortController();
				const reason = new Error('cancelled');
				const hookErrors: Error[] = [];
				let streamController!: ReadableStreamDefaultController<Uint8Array>;
				let started!: () => void;
				const reading = new Promise<void>(resolve => {
					started = resolve;
				});
				const response = new Response(new ReadableStream<Uint8Array>({
					start(bodyController) {
						streamController = bodyController;
					},
				}), {headers: {'content-type': method === 'formData' ? 'application/x-www-form-urlencoded' : 'application/json'}});
				const bodyMethod = method === 'json' ? 'text' : method;
				const read = response[bodyMethod].bind(response);
				Object.defineProperty(response, bodyMethod, {
					async value() {
						started();
						return read();
					},
				});

				const promise = ky('https://example.com', {
					signal: controller.signal,
					timeout: timeout === 'total' ? false : timeout,
					totalTimeout: timeout === 'total' ? 10_000 : false,
					fetch: async () => source === 'fetch' ? response : new Response('unused'),
					hooks: {
						beforeRequest: source === 'beforeRequest' ? [() => response] : [],
						afterResponse: source === 'afterResponse' ? [() => response] : [],
						beforeError: [({error}) => {
							hookErrors.push(error);
							return error;
						}],
					},
				})[method]();
				const outcome = promise.catch((error: unknown) => error);
				let settled = false;
				void outcome.then(() => {
					settled = true;
				});

				try {
					await reading;
					controller.abort(reason);
					// Drain the abort's promise jobs while the response body is still held open.
					await nextTurn();
					t.true(settled);
					t.is(await outcome, reason);
					t.deepEqual(hookErrors, [reason]);
				} finally {
					// A later body failure must be handled without processing the error a second time.
					streamController.error(new Error('late body failure'));
				}

				await nextTurn();
				t.deepEqual(hookErrors, [reason]);
				t.is(getEventListeners(controller.signal, 'abort').length, 0);
			});
		}
	}
}

for (const method of ['text', 'json'] as const) {
	test(`an abort before the ${method} shortcut does not start the body reader`, async t => {
		const controller = new AbortController();
		const reason = new Error('cancelled before reading');
		let reads = 0;
		const response = new Response('true');
		response.text = async () => {
			reads++;
			return 'true';
		};

		const request = ky('https://example.com', {
			signal: controller.signal,
			hooks: {beforeRequest: [() => response]},
		});
		await request;
		controller.abort(reason);

		await t.throwsAsync(request[method](), {is: reason});
		t.is(reads, 0);
		t.false(response.bodyUsed);
	});
}

for (const {label, reason, errorHookCalls} of [
	{label: 'default', reason: undefined, errorHookCalls: 1},
	{label: 'DOMException', reason: new DOMException('cancelled', 'AbortError'), errorHookCalls: 1},
	{label: 'string', reason: 'cancelled', errorHookCalls: 0},
	{label: 'object', reason: {cancelled: true}, errorHookCalls: 0},
]) {
	test(`a body reader that aborts synchronously rejects before finishing (${label})`, async t => {
		const controller = new AbortController();
		const hookErrors: Error[] = [];
		let finish!: () => void;
		const response = new Response('unused');
		response.text = async () => {
			controller.abort(reason);
			await new Promise<void>(resolve => {
				finish = resolve;
			});
			return 'unused';
		};

		const promise = ky('https://example.com', {
			signal: controller.signal,
			timeout: false,
			hooks: {
				beforeRequest: [() => response],
				beforeError: [({error}) => {
					hookErrors.push(error);
					return error;
				}],
			},
		}).text();
		let rejected = false;
		const outcome = promise.catch((error: unknown) => {
			rejected = true;
			return error;
		});

		try {
			await nextTurn();
			t.true(rejected);
			t.is(await outcome, controller.signal.reason);
			t.is(hookErrors.length, errorHookCalls);
			if (errorHookCalls > 0) {
				t.is(hookErrors[0], controller.signal.reason);
			}
		} finally {
			finish();
		}

		t.is(getEventListeners(controller.signal, 'abort').length, 0);
	});
}

for (const body of ['', 'complete']) {
	test(`a completed body releases the abort listener (${JSON.stringify(body)})`, async t => {
		const controller = new AbortController();
		const result = await ky('https://example.com', {
			signal: controller.signal,
			hooks: {beforeRequest: [() => new Response(body)]},
		}).text();

		t.is(result, body);
		t.is(getEventListeners(controller.signal, 'abort').length, 0);
	});
}

test.serial('an abort clears the body timeout before the body finishes', async t => {
	const controller = new AbortController();
	const reason = new Error('cancelled');
	let streamController!: ReadableStreamDefaultController;
	const response = new Response(new ReadableStream({
		start(bodyController) {
			streamController = bodyController;
		},
	}));
	const request = ky('https://example.com', {
		signal: controller.signal,
		hooks: {beforeRequest: [() => response]},
	});
	await request;

	const originalSetTimeout = globalThis.setTimeout;
	const originalClearTimeout = globalThis.clearTimeout;
	const timers = new Set<ReturnType<typeof setTimeout>>();
	globalThis.setTimeout = ((...arguments_) => {
		const timer = originalSetTimeout(...arguments_);
		timers.add(timer);
		return timer;
	}) as typeof globalThis.setTimeout;
	globalThis.clearTimeout = ((timer: ReturnType<typeof setTimeout>) => {
		timers.delete(timer);
		originalClearTimeout(timer);
	}) as typeof globalThis.clearTimeout;

	const outcome = request.text().catch((error: unknown) => error);
	try {
		await nextTurn();
		t.is(timers.size, 1);
		controller.abort(reason);
		await nextTurn();
		t.is(timers.size, 0);
		t.is(await outcome, reason);
	} finally {
		streamController.error(new Error('late failure'));
		globalThis.setTimeout = originalSetTimeout;
		globalThis.clearTimeout = originalClearTimeout;
	}
});

test('a body error releases the abort listener', async t => {
	const controller = new AbortController();
	const reason = new Error('body failed');
	const response = new Response(new ReadableStream({
		start(bodyController) {
			bodyController.error(reason);
		},
	}));

	await t.throwsAsync(ky('https://example.com', {
		signal: controller.signal,
		hooks: {beforeRequest: [() => response]},
	}).text(), {is: reason});
	t.is(getEventListeners(controller.signal, 'abort').length, 0);
});
