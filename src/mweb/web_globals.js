// Install only the Web APIs needed by the vendored DOM and player helpers.
// No filesystem, subprocess, environment, fetch or module-loader extensions.
{
  const load = __bootstrap.core.loadExtScript;
  const encoding = load('ext:deno_web/08_text_encoding.js');
  const url = load('ext:deno_web/00_url.js');
  const base64 = load('ext:deno_web/05_base64.js');
  const clone = load('ext:deno_web/02_structured_clone.js');
  const timing = load('ext:deno_web/15_performance.js');
  Object.assign(globalThis, {
    TextEncoder: encoding.TextEncoder, TextDecoder: encoding.TextDecoder,
    URL: url.URL, URLSearchParams: url.URLSearchParams,
    atob: base64.atob, btoa: base64.btoa, structuredClone: clone.structuredClone,
    __monotonicNow: timing.performance.now.bind(timing.performance),
  });
}
