// Host APIs used by the embedded DOM and Google's interpreter. No I/O is exposed.
globalThis.global = globalThis;
if (typeof FinalizationRegistry === 'undefined') globalThis.FinalizationRegistry = class { register(){} unregister(){ return true; } };
if (typeof WeakRef === 'undefined') globalThis.WeakRef = class { constructor(value){this.value=value;} deref(){return this.value;} };
globalThis.performance = {now: __monotonicNow, timeOrigin: Date.now(), timing: {}, getEntries: () => [], getEntriesByType: () => []};
globalThis.console = {log(){}, info(){}, warn(){}, error(){}, debug(){}, trace(){}};
globalThis.__timers = [];
globalThis.__timerDelays = [];
globalThis.setTimeout = (fn, delay = 0, ...args) => { __timerDelays.push(delay); if (delay < 10000) __timers.push(() => fn(...args)); return __timers.length; };
globalThis.clearTimeout = globalThis.clearInterval = () => {};
globalThis.setInterval = () => 1;
globalThis.setImmediate = (fn,...args) => setTimeout(fn,0,...args);
globalThis.clearImmediate = clearTimeout;
