// Each extraction restores a fresh isolate from the immutable helper snapshot.
globalThis.__dom = new __JSDOM('<!DOCTYPE html><html lang="en"><head><title></title></head><body></body></html>', {
  url: 'https://www.youtube.com/', referrer: 'https://www.youtube.com/', userAgent: __userAgent
});
Object.assign(globalThis, {
  window: __dom.window, document: __dom.window.document, location: __dom.window.location,
  origin: __dom.window.origin, navigator: __dom.window.navigator
});
performance.timeOrigin = Date.now();
for (let turn = 0; turn < 10; turn++) {
  for (let i = 0, n = __timers.length; i < n; i++) __timers.shift()();
}
for (const name of ['Event','EventTarget','CustomEvent','MessageEvent','HTMLElement','Element','HTMLDocument','Document','Node','NodeList','AbortController','AbortSignal','Blob','File','getComputedStyle','requestAnimationFrame','matchMedia','localStorage','sessionStorage','screen'])
  if (typeof globalThis[name] === 'undefined' && window[name] !== undefined) globalThis[name] = window[name];
globalThis.addEventListener = window.addEventListener.bind(window);
globalThis.removeEventListener = window.removeEventListener.bind(window);
globalThis.dispatchEvent = window.dispatchEvent.bind(window);
window.eval = globalThis.eval;
window.Function = globalThis.Function;
// Internal extension functions retain their private references. Hide the raw
// core bootstrap/ops surface before evaluating upstream JavaScript.
delete globalThis.Deno;
delete globalThis.__bootstrap;
