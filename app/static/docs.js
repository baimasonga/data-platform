'use strict';
// A self-hosted reference keeps the strict CSP and works without CDN access.
async function loadReference() {
  const response = await fetch('/openapi.json');
  if (!response.ok) throw new Error('Unable to load the API specification.');
  const spec = await response.json();
  const endpoints = document.getElementById('endpoints');
  for (const [path, operations] of Object.entries(spec.paths)) {
    for (const [method, operation] of Object.entries(operations)) {
      if (!['get', 'post', 'put', 'patch', 'delete', 'head', 'options'].includes(method)) continue;
      const section = document.createElement('section');
      section.className = 'panel';
      const heading = document.createElement('h2');
      heading.textContent = `${method.toUpperCase()} ${path}`;
      const summary = document.createElement('p');
      summary.textContent = operation.summary || '';
      const details = document.createElement('pre');
      details.textContent = JSON.stringify({
        parameters: operation.parameters || [],
        requestBody: operation.requestBody,
        responses: operation.responses
      }, null, 2);
      section.append(heading, summary, details);
      endpoints.append(section);
    }
  }
  document.getElementById('schemas').textContent = JSON.stringify(spec.components?.schemas || {}, null, 2);
  document.getElementById('docs-status').textContent = 'API specification loaded. This reference does not execute requests.';
}
loadReference().catch(error => {
  document.getElementById('docs-status').textContent = error.message;
});
