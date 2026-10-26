// The generic parser and dependency-aware player extractor are vendored alongside
// this file. This adapter executes only the extracted URL-transform dependencies.
globalThis.__preparePlayer = source => {
  const analyzer = new JsAnalyzer(source, {extractions: [
    {friendlyName: 'decipher', match: nsigMatcher},
    {friendlyName: 'timestamp', match: timestampMatcher, collectDependencies: false}
  ]});
  const result = new JsExtractor(analyzer).buildScript({
    disallowSideEffectInitializers: true,
    exportRawValues: true,
    rawValueOnly: ['timestamp']
  });
  if (!result.exported.includes('decipher')) throw new Error('Player transform not found');
  globalThis.__playerScript = result.output + `
    return function(n, sp, signature) {
      const transformed = exportedVars.decipher('https://media.googlevideo.com/videoplayback?n=' + encodeURIComponent(n), sp, signature);
      for (const name of Object.getOwnPropertyNames(Object.getPrototypeOf(transformed))) {
        if (!['constructor','clone','set','get'].includes(name) && typeof transformed[name]==='function') transformed[name]();
      }
      const sig=transformed.get(sp), nextN=transformed.get('n');
      return {signature:sig?decodeURIComponent(sig):null, n:nextN?decodeURIComponent(nextN):null};
    };
  `;
  globalThis.__decipher = new Function(__playerScript)();
  return Number(result.exportedRawValues.timestamp);
};
