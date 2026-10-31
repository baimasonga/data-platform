'use strict';
const $ = id => document.getElementById(id);
const state = {datasets: [], result: null, view: 'datasets', imports: [], importsRequest: 0, history: [], historyRequest: 0};
function element(tag, text, className) { const e = document.createElement(tag); if(text !== undefined) e.textContent = text; if(className) e.className = className; return e; }
function message(text, error=false) { $('message').hidden=false; $('message').className=error?'error':''; $('message').textContent=text; }
async function api(path, options={}) {
  const response = await fetch('/api' + path, options);
  const data = await response.json();
  if (!response.ok) {
    const detail = data.detail;
    const text = typeof detail === 'string' ? detail : Array.isArray(detail)
      ? detail.map(item => String(item.msg).replace(/^Value error, /, '')).join(' ')
      : 'The request could not be completed.';
    throw new Error(text);
  }
  return data;
}

function table(columns, rows) { const wrap=element('div',undefined,'table-wrap'), t=element('table'), head=element('thead'), tr=element('tr'); columns.forEach(c=>tr.append(element('th',c))); head.append(tr); t.append(head); const body=element('tbody'); rows.forEach(row=>{const r=element('tr'); row.forEach(v=>r.append(element('td',v===null?'NULL':String(v),v===null?'null':'')));body.append(r);}); t.append(body);wrap.append(t);return wrap; }
async function switchView(view) { state.view=view; document.querySelectorAll('main > section').forEach(e=>e.hidden=e.id!=='view-'+view); document.querySelectorAll('nav button').forEach(b=>b.classList.toggle('active',b.dataset.view===view)); $('breadcrumb').textContent=view.toUpperCase(); const titles={datasets:['Your data, understood.','Import, inspect and query data in one place.'],sql:['Ask your data.','Write SQL, inspect results and explore patterns.'],saved:['Keep your best questions.','Reusable SQL, ready when you need it.'],notebooks:['Explore in SQL cells.','Save notes and independent read-only SQL cells.'],dashboards:['Your evidence, together.','Refresh saved table and metric widgets against current data.'],metrics:['Define reusable measures.','Keep SQL, value columns, units and definitions together.'],connections:['Connect your sources.','Bring database snapshots and incremental rows into versioned datasets.'],workflows:['Prepare reusable data.','Build transformations with version history, quality gates and recorded inputs.'],history:['Every question, recorded.','Review SQL runs, diagnose errors and revisit your work.'],imports:['Follow every import.','Track successful uploads and understand what needs attention.'],activity:['A record of your work.','Recent imports and query activity in this installation.']}; $('page-title').textContent=titles[view][0];$('subtitle').textContent=titles[view][1]; try { if(['notebooks','dashboards','metrics'].includes(view))await loadAnalysis(({notebooks:'notebook',dashboards:'dashboard',metrics:'metric'})[view]);if(view==='connections')await loadConnectors();if(view==='workflows') {await loadWorkflowTargets();await loadPipelines();}if(view==='history') await loadHistory();if(view==='imports') await loadImports();if(view==='saved') await loadSaved();if(view==='activity')await loadActivity(); }catch(e){message(e.message,true);} }
document.querySelectorAll('nav button').forEach(b=>b.addEventListener('click',()=>switchView(b.dataset.view)));
async function refresh() { state.datasets=await api('/datasets');$('metric-datasets').textContent=state.datasets.length; const rows=state.datasets.reduce((s,d)=>s+d.row_count,0);$('metric-rows').textContent=rows.toLocaleString();const cells=state.datasets.reduce((s,d)=>s+d.row_count*d.column_count,0);const missing=state.datasets.reduce((s,d)=>s+d.profile.reduce((n,c)=>n+c.nulls,0),0);$('metric-quality').textContent=cells?((1-missing/cells)*100).toFixed(1)+'%':'—';renderDatasets(); }
function renderDatasets() {
  const list = $('dataset-list');
  list.replaceChildren();
  const filter = $('search').value.toLowerCase();
  const datasets = state.datasets.filter(d => d.name.toLowerCase().includes(filter));
  if (!datasets.length) {
    list.append(element('div', state.datasets.length ? 'No matching datasets.' : 'No datasets yet. Import a file to begin.', 'empty'));
    return;
  }
  datasets.forEach(d => {
    const card = element('article', undefined, 'dataset-card');
    const summary = element('div', undefined, 'dataset-summary');
    const text = element('div');
    text.append(element('div', d.name, 'dataset-name'), element('div',
      d.deletion_pending ? 'Deletion pending. Retry to finish removing this dataset.' :
      `${d.row_count.toLocaleString()} rows · ${d.column_count} columns · ${(d.size_bytes / 1024).toFixed(1)} KB`, 'dataset-meta'));
    summary.append(text);
    const button = element('button', d.deletion_pending ? 'Retry deletion' : 'Inspect dataset →');
    button.onclick = () => d.deletion_pending ? openDelete(d) : inspect(d.id);
    card.append(summary, button);
    list.append(card);
  });
}
$('search').addEventListener('input',renderDatasets);
$('import-button').addEventListener('click',()=>$('file').click());
$('file').addEventListener('change',async e=>{const file=e.target.files[0];if(!file)return;if(file.size>20*1024*1024){message('Maximum upload size is 20 MB.',true);e.target.value='';return;}const button=$('import-button');button.disabled=true;button.textContent='Importing…';message('Importing and profiling your dataset…');try{const data=new FormData();data.append('file',file);const result=await api('/datasets',{method:'POST',body:data});await refresh();await switchView('datasets');message(`Imported ${file.name}: ${result.row_count.toLocaleString()} records.`);await inspect(result.id);}catch(err){message(err.message,true);}finally{button.disabled=false;button.textContent='+ Import dataset';e.target.value='';if(state.view==='imports')loadImports().catch(err=>message(err.message,true));}});
async function inspect(id) {try{const d=await api('/datasets/'+id);const panel=$('dataset-detail');$('version-panel').hidden=true;panel.hidden=false;panel.replaceChildren();const box=element('div',undefined,'panel');const heading=element('div',undefined,'panel-heading');heading.append(element('h2',d.name));const close=element('button','Close');close.onclick=()=>panel.hidden=true;heading.append(close);box.append(heading);const actions=element('div',undefined,'detail-actions'),query=element('button','Query this dataset','primary');query.onclick=()=>{setQueryEngine('duckdb');$('sql').value=`SELECT *\nFROM "${d.table_name}"\nLIMIT 100;`;switchView('sql');};const rename=element('button','Rename dataset');rename.onclick=()=>openRename(d);const remove=element('button','Delete dataset','danger');remove.onclick=()=>openDelete(d);const exportButton=element('button','Export dataset CSV');exportButton.onclick=()=>exportDataset(d,exportButton);const replace=element('button','Upload new version');replace.onclick=()=>{versionTarget=d;$('version-file').click();};const versions=element('button','Versions & lineage');versions.onclick=()=>showVersions(d);const quality=element('button','Quality contract');quality.onclick=()=>openQuality(d);const lake=element('button','Publish to lakehouse');lake.onclick=()=>publishLakehouse(d,lake);const lakeHistory=element('button','Lakehouse history');lakeHistory.onclick=()=>showLakehouse(d);if(state.session?.role==='viewer')for(const button of [replace,quality,rename,remove,lake])button.disabled=true;actions.append(query,exportButton,replace,versions,quality,lake,lakeHistory,rename,remove);box.append(actions,element('p','SQL table: '+d.table_name),element('p',`CSV export includes all ${d.row_count.toLocaleString()} rows.`));if(d.source){box.append(element('p','Source SHA-256: '+d.source.sha256,'source-provenance'));const download=element('a','Download original source');download.href='/api/datasets/'+encodeURIComponent(d.id)+'/source';actions.append(download);}box.append(element('h3','Column quality'),table(['Column','Type','Missing','Distinct','Complete'],d.profile.map(c=>[c.name,c.type,c.nulls,c.distinct,c.completeness+'%'])));box.append(element('h3','Preview · first 50 records'),table(d.preview.columns,d.preview.rows));panel.append(box);}catch(e){message(e.message,true);} }
$('run-query').addEventListener('click',async()=>{const button=$('run-query');button.disabled=true;button.textContent='Running…';$('query-output').hidden=true;state.result=null;const statement=$('sql').value,engine=$('sql-engine').value;try{const result=await api('/query',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({sql:statement,engine})});if($('sql').value!==statement||$('sql-engine').value!==engine){message('The editor changed while the query ran. Find that run in Query history or run the current SQL.');return;}state.result=result;$('query-table').replaceChildren(table(result.columns,result.rows));$('query-info').textContent=`${result.rows.length.toLocaleString()} rows · ${result.elapsed_ms} ms${result.truncated?' · limited to 1,000 rows':''}`;$('query-output').hidden=false;$('chart').replaceChildren();['chart-category','chart-value'].forEach(id=>{$(id).replaceChildren();result.columns.forEach((name,index)=>{const option=element('option',name);option.value=index;$(id).append(option);});});const numeric=result.columns.findIndex((_,i)=>result.rows.some(r=>typeof r[i]==='number'));if(numeric>=0)$('chart-value').value=numeric;message('Query completed.');}catch(e){message(e.message,true);}finally{button.disabled=false;button.textContent='Run query ↗';}});
$('sql').addEventListener('keydown',e=>{if((e.ctrlKey||e.metaKey)&&e.key==='Enter'){e.preventDefault();$('run-query').click();}});
$('save-query').onclick=()=>{if(!$('sql').value.trim()){message('Enter a query first.',true);return;}$('save-dialog').showModal();};$('cancel-save').onclick=()=>$('save-dialog').close();
$('save-form').addEventListener('submit',async e=>{e.preventDefault();try{await api('/saved-queries',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({name:$('query-name').value,sql:$('sql').value,engine:$('sql-engine').value})});$('save-dialog').close();$('query-name').value='';message('Query saved.');}catch(err){message(err.message,true);}});
let savedRequest = 0;
async function loadSaved() {
  const request = ++savedRequest;
  const saved = await api('/saved-queries');
  if (request !== savedRequest) return;
  const list = $('saved-list');
  list.replaceChildren();
  if (!saved.length) list.append(element('div', 'No saved queries yet. Save a query from the SQL workbench.', 'empty'));
  for (const query of saved) {
    const card = element('article', undefined, 'dataset-card saved-card');
    const text = element('div');
    text.append(element('div', query.name, 'dataset-name'), element('code', query.sql.slice(0, 160)));
    const actions = element('div', undefined, 'saved-actions');
    const open = element('button', 'Open query →');
    open.onclick = () => {
      setQueryEngine(query.engine||'duckdb');
      $('sql').value = query.sql;
      state.result = null;
      $('query-output').hidden = true;
      switchView('sql');
    };
    const edit = element('button', 'Edit query');
    edit.onclick = () => openSavedDialog('edit', query);
    const remove = element('button', 'Delete query', 'danger');
    remove.onclick = () => openSavedDialog('delete', query);
    if(state.session?.role==='viewer'){edit.disabled=true;remove.disabled=true;}
    actions.append(open, edit, remove);
    card.append(text, actions);
    list.append(card);
  }
}
$('refresh-saved').onclick = () => loadSaved().catch(error => message(error.message, true));
let managedSavedQuery = null;
let savedMutationBusy = false;
function openSavedDialog(kind, query) {
  managedSavedQuery = {...query};
  $(kind + '-saved-error').textContent = '';
  if (kind === 'edit') {
    $('edit-saved-name').value = query.name;
    $('edit-saved-engine').value = query.engine||'duckdb';
    $('edit-saved-sql').value = query.sql;
  } else {
    $('delete-saved-description').textContent = `Permanently delete “${query.name}”?`;
  }
  $(kind + '-saved-dialog').showModal();
}
for (const kind of ['edit', 'delete']) {
  $('cancel-' + kind + '-saved').onclick = () => $(kind + '-saved-dialog').close();
  $(kind + '-saved-dialog').addEventListener('cancel', event => {
    if (savedMutationBusy) event.preventDefault();
  });
  $(kind + '-saved-form').addEventListener('submit', async event => {
    event.preventDefault();
    if (savedMutationBusy) return;
    savedMutationBusy = true;
    $('confirm-' + kind + '-saved').disabled = true;
    $('cancel-' + kind + '-saved').disabled = true;
    $(kind + '-saved-error').textContent = '';
    const query = managedSavedQuery;
    const snapshot = {name: query.name, sql: query.sql, engine:query.engine||'duckdb'};
    try {
      await api('/saved-queries/' + encodeURIComponent(query.id), {
        method: kind === 'edit' ? 'PUT' : 'DELETE',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(kind === 'edit' ? {
          name: $('edit-saved-name').value, sql: $('edit-saved-sql').value, engine:$('edit-saved-engine').value, expected: snapshot
        } : snapshot)
      });
      $(kind + '-saved-dialog').close();
      await loadSaved();
      message(kind === 'edit' ? 'Saved query updated.' : 'Saved query deleted.');
    } catch (error) {
      $(kind + '-saved-error').textContent = error.message;
      message(error.message, true);
    } finally {
      savedMutationBusy = false;
      $('confirm-' + kind + '-saved').disabled = false;
      $('cancel-' + kind + '-saved').disabled = false;
    }
  });
}
async function loadActivity(){const list=$('activity-list');list.replaceChildren();const events=await api('/activity');if(!events.length)list.append(element('div','No activity yet. Import your first dataset.','empty'));events.forEach(e=>{const row=element('div',undefined,'activity-row'),text=element('div');text.append(element('strong',e.action),element('p',e.detail));row.append(text,element('small',new Date(e.created_at).toLocaleString()));list.append(row);});}
$('draw-chart').onclick=()=>{if(!state.result)return;const category=Number($('chart-category').value),value=Number($('chart-value').value),rows=state.result.rows.slice(0,20);if(rows.some(r=>typeof r[value]!=='number'||r[value]<0)){message('Choose a numeric column containing non-negative values. Chart displays the first 20 result rows.',true);return;}const max=Math.max(...rows.map(r=>r[value]),1);$('chart').replaceChildren();rows.forEach(r=>{const line=element('div',undefined,'bar-row'),track=element('div',undefined,'bar-track'),bar=element('div',undefined,'bar-fill');const fraction=Math.round(r[value]/max*100); // CSP allows no inline styles: use a native SVG for precise widths.
const svg=document.createElementNS('http://www.w3.org/2000/svg','svg');svg.setAttribute('width','100%');svg.setAttribute('height','22');svg.setAttribute('viewBox','0 0 100 22');svg.setAttribute('preserveAspectRatio','none');const rect=document.createElementNS(svg.namespaceURI,'rect');rect.setAttribute('width',fraction);rect.setAttribute('height','22');rect.setAttribute('fill','#2e927b');svg.append(rect);track.append(svg);line.append(element('span',String(r[category]??'NULL'),'bar-label'),track,element('span',r[value].toLocaleString()));$('chart').append(line);});$('chart').append(element('small','First 20 result rows, in query order. Values are not aggregated.'));};
$('export').onclick=()=>{if(!state.result)return;const quote=v=>'"'+String(v??'').replaceAll('"','""')+'"';const safe=v=>typeof v==='string'&&/^[=+@\-\t\r]/.test(v)?"'"+v:v;const csv=[state.result.columns,...state.result.rows].map(r=>r.map(v=>quote(safe(v))).join(',')).join('\r\n');const url=URL.createObjectURL(new Blob(['\uFEFF'+csv],{type:'text/csv;charset=utf-8'}));const a=element('a');a.href=url;a.download='query-results.csv';a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);};
refresh().catch(e=>message(e.message,true));

async function loadImports() {
  const request = ++state.importsRequest;
  $('imports-status').textContent = 'Refreshing imports…';
  try {
    const attempts = await api('/imports');
    if (request !== state.importsRequest) return;
    state.imports = attempts;
    renderImports();
    $('imports-status').textContent = 'Import history is up to date.';
  } catch (error) {
    if (request === state.importsRequest) $('imports-status').textContent = 'Unable to refresh imports: ' + error.message;
  }
}
function renderImports() {
  const list = $('import-list');
  list.replaceChildren();
  const filter = $('import-filter').value;
  const attempts = state.imports.filter(item => filter === 'all' || item.status === filter);
  if (!attempts.length) {
    list.append(element('p', state.imports.length ? 'No imports match this status.' : 'No imports yet. Import a dataset to begin.', 'empty'));
    return;
  }
  for (const item of attempts) {
    const card = element('article', undefined, 'import-card');
    const heading = element('div', undefined, 'panel-heading');
    heading.append(element('h3', item.filename), element('span', item.status, 'import-status status-' + item.status));
    card.append(heading, element('p', 'Started ' + new Date(item.started_at).toLocaleString()));
    if (item.finished_at) card.append(element('p', 'Finished ' + new Date(item.finished_at).toLocaleString()));
    if (item.row_count !== null) card.append(element('p', item.row_count.toLocaleString() + ' rows imported'));
    if (item.error) card.append(element('p', item.error, 'import-error'));
    if (item.status === 'succeeded' && !item.dataset_available) card.append(element('p', 'Dataset is no longer available.'));
    if (item.dataset_available && item.status === 'succeeded') {
      const open = element('button', 'Open dataset');
      open.onclick = async () => { await switchView('datasets'); await inspect(item.dataset_id); };
      card.append(open);
    }
    if(item.retry_available&&item.status!=='running'){const remove=element('button','Remove retained retry file');remove.onclick=async()=>{if(!confirm('Remove the retained retry file? Published dataset versions and import history are kept.'))return;try{await api('/imports/'+item.id+'/retained-source',{method:'DELETE',headers:{'Content-Type':'application/json'},body:JSON.stringify({confirmation_name:item.filename})});await loadImports();message('Retained retry file removed.');}catch(error){message(error.message,true);}};card.append(remove);}
    if (item.retry_available && ['failed', 'interrupted'].includes(item.status)) {
      const retry = element('button', 'Retry retained import');
      retry.onclick = async () => {
        retry.disabled = true;
        try { await api('/imports/' + encodeURIComponent(item.id) + '/retry', {method:'POST'}); await refresh(); message('Import published.'); }
        catch(error) { message(error.message, true); }
        finally { await loadImports(); }
      };
      card.append(retry);
    }
    list.append(card);
  }
}
$('import-filter').addEventListener('change', renderImports);
$('refresh-imports').addEventListener('click', loadImports);
setInterval(() => { if (state.view === 'imports' && !document.hidden) loadImports(); }, 3000);

let managedDataset = null;
let datasetMutationBusy = false;
function openRename(dataset) {
  managedDataset = dataset;
  $('dataset-name').value = dataset.name;
  $('rename-error').textContent = '';
  $('rename-dialog').showModal();
  $('dataset-name').focus();
}
function openDelete(dataset) {
  managedDataset = dataset;
  $('delete-confirmation').value = '';
  $('delete-label').textContent = `Type “${dataset.name}” to confirm`;
  $('delete-error').textContent = '';
  $('confirm-delete').disabled = true;
  $('delete-dialog').showModal();
  $('delete-confirmation').focus();
}
$('cancel-rename').onclick = () => $('rename-dialog').close();
$('cancel-delete').onclick = () => $('delete-dialog').close();
for (const kind of ['rename', 'delete']) {
  $(kind + '-dialog').addEventListener('cancel', event => {
    if (datasetMutationBusy) event.preventDefault();
  });
}
$('delete-confirmation').addEventListener('input', () => {
  $('confirm-delete').disabled = datasetMutationBusy || $('delete-confirmation').value !== managedDataset?.name;
});
function mutationBusy(kind, busy) {
  datasetMutationBusy = busy;
  $('confirm-' + kind).disabled = busy;
  $('cancel-' + kind).disabled = busy;
}
$('rename-form').addEventListener('submit', async event => {
  event.preventDefault();
  if (datasetMutationBusy) return;
  const dataset = managedDataset;
  mutationBusy('rename', true);
  $('rename-error').textContent = '';
  try {
    const updated = await api('/datasets/' + encodeURIComponent(dataset.id), {
      method: 'PATCH', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({name: $('dataset-name').value})
    });
    $('rename-dialog').close();
    await refresh();
    await inspect(dataset.id);
    message(`Dataset renamed to ${updated.name}.`);
  } catch (error) {
    $('rename-error').textContent = error.message;
    message(error.message, true);
  } finally { mutationBusy('rename', false); }
});
$('delete-form').addEventListener('submit', async event => {
  event.preventDefault();
  if (datasetMutationBusy || $('delete-confirmation').value !== managedDataset?.name) return;
  const dataset = managedDataset;
  mutationBusy('delete', true);
  $('delete-error').textContent = '';
  try {
    await api('/datasets/' + encodeURIComponent(dataset.id), {
      method: 'DELETE', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({confirmation_name: $('delete-confirmation').value})
    });
    $('delete-dialog').close();
    $('dataset-detail').hidden = true;
    $('version-panel').hidden = true;
    state.result = null;
    $('query-output').hidden = true;
    await refresh();
    message(`Deleted ${dataset.name}.`);
  } catch (error) {
    $('delete-error').textContent = error.message;
    message(error.message, true);
    await refresh().catch(() => {});
  } finally {
    mutationBusy('delete', false);
    $('confirm-delete').disabled = $('delete-confirmation').value !== managedDataset?.name;
  }
});

async function loadHistory() {
  const request = ++state.historyRequest;
  $('history-status').textContent = 'Refreshing query history…';
  try {
    const runs = await api('/query-history');
    if (request !== state.historyRequest) return;
    state.history = runs;
    renderHistory();
    $('history-status').textContent = 'Query history is up to date.';
  } catch (error) {
    if (request === state.historyRequest) $('history-status').textContent = 'Unable to refresh query history: ' + error.message;
  }
}
function renderHistory() {
  const list = $('history-list');
  list.replaceChildren();
  const filter = $('history-filter').value;
  const search = $('history-search').value.toLowerCase();
  const runs = state.history.filter(run => (filter === 'all' || run.status === filter) && run.sql.toLowerCase().includes(search));
  if (!runs.length) {
    list.append(element('p', state.history.length ? 'No queries match these filters.' : 'No queries yet. Run SQL in the workbench to begin.', 'empty'));
    return;
  }
  for (const run of runs) {
    const card = element('article', undefined, 'history-card');
    const heading = element('div', undefined, 'panel-heading');
    heading.append(element('h3', new Date(run.started_at).toLocaleString()),
      element('span', run.status.replaceAll('_', ' '), 'import-status status-' + run.status));
    const sql = element('pre', run.sql, 'history-sql');
    card.append(heading, element('p','Engine: '+(run.engine||'duckdb')), sql);
    if (run.elapsed_ms !== null) card.append(element('p', `${run.elapsed_ms.toLocaleString()} ms total duration`));
    if (run.status === 'running') card.append(element('p', 'Running or waiting for another operation to finish.'));
    if (run.result_rows !== null) card.append(element('p', `${run.result_rows.toLocaleString()} rows returned${run.truncated ? ' · limited to 1,000 rows' : ''}`));
    if (run.error) card.append(element('p', run.error, 'import-error'));
    const open = element('button', 'Open in SQL editor');
    open.onclick = () => {
      setQueryEngine(run.engine||'duckdb');
      $('sql').value = run.sql;
      state.result = null;
      $('query-output').hidden = true;
      switchView('sql');
      $('sql').focus();
      message('Query loaded. Choose Run query to execute it.');
    };
    card.append(open);
    list.append(card);
  }
}
$('history-search').addEventListener('input', renderHistory);
$('history-filter').addEventListener('change', renderHistory);
$('refresh-history').onclick = loadHistory;
setInterval(() => { if (state.view === 'history' && !document.hidden) loadHistory(); }, 3000);

async function exportDataset(dataset, button) {
  button.disabled = true;
  button.textContent = 'Preparing CSV…';
  try {
    const response = await fetch('/api/datasets/' + encodeURIComponent(dataset.id) + '/export');
    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.detail || 'Unable to export this dataset.');
    }
    const blob = await response.blob();
    const disposition = response.headers.get('Content-Disposition') || '';
    const unicode = disposition.match(/filename\*=UTF-8''([^;]+)/i);
    const plain = disposition.match(/filename="([^"\r\n]+)"/i);
    const filename = unicode ? decodeURIComponent(unicode[1]) : plain ? plain[1] : 'dataset.csv';
    const url = URL.createObjectURL(blob);
    const link = element('a');
    link.href = url;
    link.download = filename;
    link.click();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
    message(`CSV prepared: ${Number(response.headers.get('X-Export-Row-Count')).toLocaleString()} rows. Download started.`);
  } catch (error) {
    message(error.message, true);
  } finally {
    button.disabled = false;
    button.textContent = 'Export dataset CSV';
  }
}

let versionTarget = null;
$('version-file').addEventListener('change', async event => {
  const file = event.target.files[0], dataset = versionTarget;
  if (!file || !dataset) return;
  if (file.size > 20 * 1024 * 1024) { message('Maximum upload size is 20 MB.', true); event.target.value=''; return; }
  message('Validating and publishing a new version…');
  try {
    const form = new FormData(); form.append('file', file);
    const result = await api('/datasets/' + encodeURIComponent(dataset.id) + '/versions', {method:'POST', body:form});
    await refresh(); await inspect(dataset.id); message(`Version ${result.version} published.`);
  } catch(error) { message(error.message, true); }
  finally { event.target.value=''; }
});
async function showVersions(dataset) {
  try {
    const versions = await api('/datasets/' + encodeURIComponent(dataset.id) + '/versions');
    const panel = $('version-panel'); panel.replaceChildren(element('h2', 'Versions & lineage')); panel.hidden=false;
    for (const version of versions) {
      const card = element('article', undefined, 'history-card');
      card.append(element('h3', `Version ${version.number} · ${version.kind}`), element('p', `${version.row_count.toLocaleString()} rows · ${new Date(version.created_at).toLocaleString()}`));
      if (version.sql) card.append(element('pre', version.sql, 'history-sql'));
      for (const input of version.inputs) card.append(element('p', input.connector_id ? `Source: ${input.name} · watermark ${input.watermark_after ?? 'snapshot'}` : `Input: ${input.name} · version ${input.version_id}`));
      const query=element('button','Query this version');
      query.onclick=()=>{setQueryEngine('duckdb'); $('sql').value=`SELECT * FROM "${version.table_name}" LIMIT 100;`; state.result=null; $('query-output').hidden=true; switchView('sql'); };
      card.append(query);
      if(version.original_name){const source=element('a',"Download this version's source");source.href='/api/datasets/'+encodeURIComponent(dataset.id)+'/versions/'+encodeURIComponent(version.id)+'/source';card.append(source);}
      if (version.number !== versions[0].number) {
        const restore=element('button','Restore as new version');
        restore.onclick=async()=>{
          if (!confirm(`Restore version ${version.number} as a new version? Current quality rules will be checked.`)) return;
          restore.disabled=true;
          try { const result=await api('/datasets/'+encodeURIComponent(dataset.id)+'/versions/'+encodeURIComponent(version.id)+'/restore',{method:'POST'});await refresh();await inspect(dataset.id);await showVersions(dataset);message(`Version ${result.version} published.`); }
          catch(error){message(error.message,true);restore.disabled=false;}
        };
        card.append(restore);
      }
      panel.append(card);
    }
  } catch(error) { message(error.message,true); }
}
async function loadWorkflowTargets() {
  await refresh();
  const select=$('transform-target');select.replaceChildren();const create=element('option','New dataset');create.value='';select.append(create);
  for (const dataset of state.datasets.filter(d=>!d.deletion_pending)) { const option=element('option',dataset.name);option.value=dataset.id;select.append(option); }
  if (!$('transform-sql').value) $('transform-sql').value=$('sql').value;
  const sources=await api('/connectors');$('pipeline-connector').replaceChildren();const sql=element('option','SQL transformation above');sql.value='';$('pipeline-connector').append(sql);for(const source of sources){const option=element('option',source.name+' · PostgreSQL');option.value=source.id;$('pipeline-connector').append(option);}
  $('pipeline-connector').dispatchEvent(new Event('change'));
}
$('transform-form').addEventListener('submit',async event=>{
  event.preventDefault();$('transform-submit').disabled=true;message('Executing and validating transformation…');
  try {
    const result=await api('/transformations',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({name:$('transform-name').value,sql:$('transform-sql').value,target_id:$('transform-target').value||null})});
    await refresh();await switchView('datasets');await inspect(result.id);message(`Transformation published as version ${result.version}.`);
  } catch(error){message(error.message,true);}finally{$('transform-submit').disabled=false;}
});
let qualityDataset=null;
function qualityRuleRow(rule={}) {
  const row=element('div',undefined,'quality-rule');const column=element('select');column.setAttribute('aria-label','Rule column');
  for (const item of qualityDataset.profile){const option=element('option',item.name);option.value=item.name;column.append(option);}if(rule.column_name)column.value=rule.column_name;
  const kind=element('select');kind.setAttribute('aria-label','Rule type');
  for(const [value,label] of [['not_null','No missing values'],['unique','Unique, non-null values'],['min','Minimum numeric value'],['max','Maximum numeric value']]){const option=element('option',label);option.value=value;kind.append(option);}kind.value=rule.rule||'not_null';
  const threshold=element('input');threshold.type='number';threshold.step='any';threshold.setAttribute('aria-label','Numeric bound');threshold.value=rule.threshold??'';
  const toggle=()=>{threshold.hidden=!['min','max'].includes(kind.value);threshold.required=!threshold.hidden;};kind.onchange=toggle;toggle();
  const remove=element('button','Remove rule');remove.type='button';remove.onclick=()=>row.remove();row.append(column,kind,threshold,remove);$('quality-rules').append(row);
}
async function openQuality(dataset){
  try{qualityDataset=dataset;const contract=await api('/datasets/'+encodeURIComponent(dataset.id)+'/quality');$('quality-rules').replaceChildren();for(const rule of contract.rules)qualityRuleRow(rule);$('quality-error').textContent='';$('quality-results').replaceChildren();$('quality-dialog').showModal();}
  catch(error){message(error.message,true);}
}
$('add-quality-rule').onclick=()=>qualityRuleRow();$('close-quality').onclick=()=>$('quality-dialog').close();
$('quality-form').addEventListener('submit',async event=>{
  event.preventDefault();const rules=Array.from($('quality-rules').children).map(row=>({column_name:row.children[0].value,rule:row.children[1].value,threshold:row.children[2].hidden?null:Number(row.children[2].value)}));
  try{await api('/datasets/'+encodeURIComponent(qualityDataset.id)+'/quality',{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify({rules})});$('quality-error').textContent='Contract saved. Use Check current data to run it.';}
  catch(error){$('quality-error').textContent=error.message;}
});
$('run-quality').onclick=async()=>{
  $('run-quality').disabled=true;
  try{const result=await api('/datasets/'+encodeURIComponent(qualityDataset.id)+'/quality/run',{method:'POST'});const panel=$('quality-results');panel.replaceChildren(element('p',result.passed?'All saved rules passed.':'Some saved rules failed.'));for(const rule of result.results)panel.append(element('p',`${rule.column_name} · ${rule.rule}: ${rule.passed?'Passed':'Failed'}`));}
  catch(error){$('quality-error').textContent=error.message;}finally{$('run-quality').disabled=false;}
};

let editingPipeline=null;
$('new-pipeline').onclick=()=>{editingPipeline=null;$('save-pipeline').textContent='Save as pipeline';$('new-pipeline').hidden=true;};
$('save-pipeline').onclick=async()=>{
  if (!$('transform-form').reportValidity()) return;
  $('save-pipeline').disabled=true;
  try{
    const body={name:$('transform-name').value,sql:$('pipeline-connector').value?'':$('transform-sql').value,target_id:$('pipeline-connector').value?null:($('transform-target').value||null),connector_id:$('pipeline-connector').value||null,interval_seconds:Number($('pipeline-interval').value),enabled:$('pipeline-enabled').checked};
    await api('/pipelines'+(editingPipeline?'/'+encodeURIComponent(editingPipeline):''),{method:editingPipeline?'PUT':'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    $('new-pipeline').click();await loadPipelines();message('Pipeline definition saved.');
  }catch(error){message(error.message,true);}finally{$('save-pipeline').disabled=false;}
};
let pipelineRequest=0;
async function loadPipelines(){
  const request=++pipelineRequest;
  try{
    const items=await api('/pipelines');if(request!==pipelineRequest)return;const list=$('pipeline-list');list.replaceChildren();
    if(!items.length)list.append(element('p','No pipelines yet. Save a transformation above to create one.','empty'));
    for(const pipeline of items){
      const card=element('article',undefined,'history-card');card.append(element('h3',pipeline.name),element('pre',pipeline.sql,'history-sql'),element('p',pipeline.enabled?`Scheduled every ${pipeline.interval_seconds} seconds. Next run: ${new Date(pipeline.next_run).toLocaleString()}`:'Manual runs only.'));
      const run=element('button','Run pipeline now');run.onclick=async()=>{run.disabled=true;try{await api('/pipelines/'+pipeline.id+'/run',{method:'POST'});await refresh();await loadPipelines();message('Pipeline published successfully.');}catch(error){message(error.message,true);await loadPipelines();}};
      const edit=element('button','Edit pipeline');edit.onclick=()=>{editingPipeline=pipeline.id;$('transform-name').value=pipeline.name;$('transform-sql').value=pipeline.sql;$('transform-target').value=pipeline.target_id||'';$('pipeline-connector').value=pipeline.connector_id||'';$('pipeline-connector').dispatchEvent(new Event('change'));$('pipeline-interval').value=pipeline.interval_seconds||3600;$('pipeline-enabled').checked=Boolean(pipeline.enabled);$('save-pipeline').textContent='Update pipeline';$('new-pipeline').hidden=false;$('transform-name').focus();};
      const history=element('button','Pipeline run history');history.onclick=async()=>{
        try{const runs=await api('/pipelines/'+pipeline.id+'/runs');let panel=card.querySelector('.pipeline-runs');if(!panel){panel=element('div',undefined,'pipeline-runs');card.append(panel);}panel.replaceChildren();if(!runs.length)panel.append(element('p','No runs yet.'));
          for(const item of runs){const row=element('article',undefined,'history-card');row.append(element('p',`${item.status} · ${new Date(item.started_at).toLocaleString()}`));if(item.error)row.append(element('p',item.error,'import-error'));if(['failed','interrupted'].includes(item.status)){const retry=element('button','Retry pipeline run');retry.onclick=async()=>{retry.disabled=true;try{await api('/pipelines/'+pipeline.id+'/runs/'+item.id+'/retry',{method:'POST'});await refresh();history.click();message('Retry published successfully.');}catch(error){message(error.message,true);retry.disabled=false;}};row.append(retry);}panel.append(row);}
        }catch(error){message(error.message,true);}
      };
      const remove=element('button','Delete pipeline','danger');remove.onclick=async()=>{if(!confirm('Delete this pipeline and its run history? Published datasets are kept.'))return;try{await api('/pipelines/'+pipeline.id,{method:'DELETE',headers:{'Content-Type':'application/json'},body:JSON.stringify({confirmation_name:pipeline.name})});await loadPipelines();message('Pipeline deleted.');}catch(error){message(error.message,true);}};card.append(run,edit,history,remove);
      if(pipeline.enabled){const pause=element('button','Pause schedule');pause.onclick=async()=>{pause.disabled=true;try{await api('/pipelines/'+pipeline.id,{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify({...pipeline,enabled:false})});await loadPipelines();message('Schedule paused.');}catch(error){message(error.message,true);pause.disabled=false;}};card.append(pause);}
      list.append(card);
    }
  }catch(error){message(error.message,true);}
}
$('refresh-pipelines').onclick=loadPipelines;

async function loadSession(){
  try{
    const session=await api('/session');state.session=session;
    if(session.mode!=='oidc')return;
    const controls=$('session-controls');controls.hidden=false;controls.replaceChildren(element('p',session.user.name),element('small','User ID: '+session.user.id));
    const select=element('select');select.setAttribute('aria-label','Current workspace');
    for(const workspace of session.workspaces){const option=element('option',workspace.name+' · '+workspace.role);option.value=workspace.id;select.append(option);}select.value=session.workspace_id;
    select.onchange=async()=>{try{await api('/workspaces/'+select.value+'/select',{method:'POST'});location.reload();}catch(error){message(error.message,true);}};
    const create=element('button','New workspace');create.onclick=()=>$('workspace-dialog').showModal();
    const logout=element('button','Sign out');logout.onclick=async()=>{await fetch('/auth/logout',{method:'POST'});location.href='/';};
    controls.append(select,create,logout);
    if(session.role==='owner'){const members=element('button','Manage members');members.onclick=()=>openMembers();controls.append(members);}
    if(session.role==='viewer'){$('import-button').disabled=true;$('save-query').disabled=true;document.querySelector('nav button[data-view=workflows]').disabled=true;document.querySelector('nav button[data-view=connections]').disabled=true;}
  }catch(error){message(error.message,true);}
}
$('cancel-workspace').onclick=()=>$('workspace-dialog').close();
$('workspace-form').addEventListener('submit',async event=>{event.preventDefault();try{const workspace=await api('/workspaces',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({name:$('workspace-name').value})});await api('/workspaces/'+workspace.id+'/select',{method:'POST'});location.reload();}catch(error){message(error.message,true);}});
async function openMembers(){
  try{const members=await api('/workspace/members');$('member-list').replaceChildren();for(const member of members){const row=element('div',undefined,'member-row');row.append(element('p',`${member.name} · ${member.role}`),element('small',member.sub));if(member.sub!==state.session.user.id){const remove=element('button','Remove member');remove.onclick=async()=>{try{await api('/workspace/members',{method:'DELETE',headers:{'Content-Type':'application/json'},body:JSON.stringify({user_id:member.sub})});await openMembers();}catch(error){$('member-error').textContent=error.message;}};row.append(remove);}$('member-list').append(row);}await loadTokens();if(!$('members-dialog').open)$('members-dialog').showModal();}catch(error){message(error.message,true);}
}
$('close-members').onclick=()=>$('members-dialog').close();
$('member-form').addEventListener('submit',async event=>{event.preventDefault();try{await api('/workspace/members',{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify({user_id:$('member-id').value,role:$('member-role').value})});$('member-error').textContent='Membership saved.';await openMembers();}catch(error){$('member-error').textContent=error.message;}});
const sessionReady=loadSession();

$('connector-form').addEventListener('submit',async event=>{
  event.preventDefault();$('create-connector').disabled=true;
  try{await api('/connectors',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({name:$('connector-name').value,host:$('connector-host').value,port:Number($('connector-port').value),database:$('connector-db').value,username:$('connector-user').value,password:$('connector-password').value,schema_name:$('connector-schema').value,table:$('connector-table').value,mode:$('connector-mode').value,watermark_column:$('connector-watermark').value||null})});$('connector-password').value='';await loadConnectors();message('Connection saved with encrypted credentials.');}
  catch(error){message(error.message,true);}finally{$('create-connector').disabled=false;}
});
async function loadConnectors(){
  try{const sources=await api('/connectors');const list=$('connector-list');list.replaceChildren();if(!sources.length)list.append(element('p','No source connections yet.','empty'));
    for(const source of sources){const card=element('article',undefined,'history-card');card.append(element('h3',source.name),element('p',`${source.database_name} · ${source.source_schema}.${source.source_table} · ${source.mode}`));if(source.watermark)card.append(element('p','Published watermark: '+source.watermark));
      const test=element('button','Test source');test.onclick=async()=>{test.disabled=true;try{const result=await api('/connectors/'+source.id+'/test',{method:'POST'});message(`Connected. ${result.tables.length} tables visible.`);}catch(error){message(error.message,true);}finally{test.disabled=false;}};
      const run=element('button','Import source');run.onclick=async()=>{run.disabled=true;message('Extracting and validating source data…');try{const result=await api('/connectors/'+source.id+'/import',{method:'POST'});await refresh();await loadConnectors();message(result.unchanged?'No new source rows.':`Source published: ${result.new_rows} new rows.`);}catch(error){message(error.message,true);}finally{run.disabled=false;}};
      const remove=element('button','Delete connection','danger');remove.onclick=async()=>{if(!confirm(`Delete connection “${source.name}”? Published datasets are kept.`))return;try{await api('/connectors/'+source.id,{method:'DELETE',headers:{'Content-Type':'application/json'},body:JSON.stringify({confirmation_name:source.name})});await loadConnectors();}catch(error){message(error.message,true);}};
      card.append(test,run,remove);list.append(card);
    }
    const copies=await api('/lakehouse');list.append(element('h2','Lakehouse copies'));
    if(!copies.length)list.append(element('p','No lakehouse copies yet. Publish one from a dataset.'));
    for(const copy of copies){const card=element('article',undefined,'history-card');card.append(element('h3',copy.dataset_name),element('p',copy.state),element('p',copy.table_name));if(copy.error)card.append(element('p',copy.error,'import-error'));if(copy.state!=='deleted'){const remove=element('button','Remove lakehouse copy','danger');remove.onclick=async()=>{if(!confirm('Permanently remove this published lakehouse copy and its snapshots? The local dataset is kept.'))return;remove.disabled=true;try{await api('/lakehouse/'+copy.version_id,{method:'DELETE',headers:{'Content-Type':'application/json'},body:JSON.stringify({confirmation_name:copy.table_name})});await loadConnectors();message('Lakehouse copy removed.');}catch(error){message(error.message,true);remove.disabled=false;}};card.append(remove);}list.append(card);}
  }catch(error){message(error.message,true);}
}
$('refresh-connectors').onclick=loadConnectors;
async function publishLakehouse(dataset,button){button.disabled=true;message('Publishing this version to Iceberg…');try{const result=await api('/datasets/'+dataset.id+'/lakehouse',{method:'POST'});message('Lakehouse version published: '+result.table);await showLakehouse(dataset);}catch(error){message(error.message,true);}finally{button.disabled=false;}}
async function showLakehouse(dataset){
  try{const exports=await api('/datasets/'+dataset.id+'/lakehouse');const panel=$('version-panel');panel.hidden=false;panel.replaceChildren(element('h2','Lakehouse history'));if(!exports.length)panel.append(element('p','No lakehouse versions published yet.'));
    for(const item of exports){const card=element('article',undefined,'history-card');card.append(element('h3',item.state),element('p',item.table_name));if(item.error)card.append(element('p',item.error,'import-error'));if(item.state==='published'){const query=element('button','Query lakehouse copy');query.onclick=()=>{setQueryEngine('trino');$('sql').value='SELECT * FROM '+item.table_name+' LIMIT 1000';switchView('sql');};card.append(query);const snapshots=element('button','Iceberg snapshots');snapshots.onclick=async()=>{try{const result=await api('/datasets/'+dataset.id+'/lakehouse/'+item.version_id+'/snapshots');card.append(table(result.columns,result.rows));snapshots.disabled=true;}catch(error){message(error.message,true);}};card.append(snapshots);}panel.append(card);}
  }catch(error){message(error.message,true);}
}

async function loadTokens(){const tokens=await api('/workspace/tokens');$('token-list').replaceChildren();for(const token of tokens){const row=element('div',undefined,'member-row');row.append(element('p',token.name+' · expires '+new Date(token.expires*1000).toLocaleString()));const revoke=element('button','Revoke token');revoke.onclick=async()=>{await api('/workspace/tokens',{method:'DELETE',headers:{'Content-Type':'application/json'},body:JSON.stringify({id:token.id})});await loadTokens();};row.append(revoke);$('token-list').append(row);}}
$('token-form').addEventListener('submit',async event=>{event.preventDefault();try{const result=await api('/workspace/tokens',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({name:$('token-name').value,lifetime_seconds:86400})});$('token-value').value=result.token;$('token-value').hidden=false;$('clear-token').hidden=false;await loadTokens();}catch(error){$('member-error').textContent=error.message;}});
$('clear-token').onclick=()=>{$('token-value').value='';$('token-value').hidden=true;$('clear-token').hidden=true;};
$('members-dialog').addEventListener('close',()=>$('clear-token').click());

const analysisStates={notebook:{},dashboard:{},metric:{}};
const analysisViews={notebook:'notebooks',dashboard:'dashboards',metric:'metrics'};
async function loadAnalysis(kind,ident){
  const state=analysisStates[kind];
  try{
    state.list=await api('/documents?kind='+kind);
    if(kind==='dashboard')state.metrics=await api('/documents?kind=metric');
    if(ident){state.current=await api('/documents/'+encodeURIComponent(ident));state.draft=structuredClone(state.current);state.dirty=false;state.cache={};}
    renderAnalysis(kind);
  }catch(error){message(error.message,true);}
}
function renderAnalysis(kind){
  const state=analysisStates[kind],root=$('analysis-'+kind);
  const viewer=globalThis.document.querySelector('nav button[data-view=workflows]').disabled;
  root.replaceChildren();const listing=element('div',undefined,'panel');const create=element('button','New '+kind);create.disabled=viewer;
  create.onclick=()=>{if(state.dirty&&!confirm('Discard unsaved changes?'))return;state.current=null;state.draft={kind,name:'',revision:0,...(kind==='notebook'?{cells:[]}:kind==='dashboard'?{widgets:[]}:{sql:'',value_column:'value',unit:'',description:''})};state.cache={};state.dirty=false;renderAnalysis(kind);};listing.append(create);
  for(const document of state.list||[]){const row=element('div',undefined,'analysis-list-row');const open=element('button',document.name);open.onclick=async()=>{if(state.dirty&&!confirm('Discard unsaved changes?'))return;history.replaceState(null,'','#'+analysisViews[kind]+'/'+document.id);await loadAnalysis(kind,document.id);};row.append(open,element('small',new Date(document.updated_at).toLocaleString()));listing.append(row);}if(kind==='dashboard'&&state.current){const chooser=element('details');chooser.append(element('summary','Choose or create a dashboard'),listing);root.append(chooser);}else root.append(listing);
  if(!state.draft)return;
  const draft=state.draft,editor=element('div',undefined,'panel analysis-editor');
  const title=element('h2',state.current?'Edit '+kind:'New '+kind);editor.append(title);
  const label=element('label','Name');const name=element('input');name.value=draft.name;name.maxLength=200;name.required=true;name.readOnly=viewer;name.setAttribute('aria-label',kind+' name');name.oninput=()=>{draft.name=name.value;dirty();};label.append(name);editor.append(label);
  const status=element('p',state.dirty?'Unsaved changes. Save before running.':'No queries run automatically.');status.setAttribute('role','status');editor.append(status);
  const runs=[];
  function dirty(){state.dirty=true;state.cache={};status.textContent='Unsaved changes. Save before running.';for(const button of runs)button.disabled=true;for(const panel of editor.querySelectorAll('.analysis-result'))panel.replaceChildren(element('p','Definition changed; save and refresh to see results.'));}
  function field(parent,caption,value,update,multiline=false){const wrapper=element('label',caption);const input=element(multiline?'textarea':'input');input.value=value;input.readOnly=viewer;input.setAttribute('aria-label',caption);input.oninput=()=>{update(input.value);dirty();};wrapper.append(input);parent.append(wrapper);return input;}
  const results=element('div',undefined,'analysis-results');
  function output(index,panel,error){
    const result=state.cache?.[index];panel.replaceChildren();
    if(error)panel.append(element('p',(result?'Refresh failed. Last successful data remains visible. ':'Refresh failed. ')+error,'import-error'));
    if(!result){panel.append(element('p','Not refreshed yet.'));return;}
    panel.append(element('p','Last refreshed '+new Date(result.refreshed_at).toLocaleString()));
    if(result.kind==='metric')panel.append(element('strong',result.value===null?'No value':String(result.value)+(result.unit?' '+result.unit:''),'metric-value'));
    else if(result.kind==='note')panel.append(element('pre',result.text,'analysis-note'));
    else{panel.append(element('p',`${result.rows.length} rows returned${result.truncated?' · capped at 1,000 rows':''}. Preview shows up to 50 rows and 12 columns.`));panel.append(table(result.columns.slice(0,12),result.rows.slice(0,50).map(row=>row.slice(0,12))));}
    if(result.sql){const details=element('details');details.append(element('summary','SQL used'),element('pre',result.sql,'history-sql'));panel.append(details);const open=element('button','Open SQL in workbench');open.onclick=()=>{setQueryEngine('duckdb');$('sql').value=result.sql;clearQueryResult();switchView('sql');};panel.append(open);}
  }
  async function run(index,panel,button){
    if(state.dirty||!state.current){status.textContent='Save this definition before running.';return;}
    const id=state.current.id,revision=state.current.revision;button.disabled=true;
    try{const result=await api('/documents/'+id+'/items/'+index+'/run',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({revision})});if(state.dirty||state.current?.id!==id||state.current.revision!==revision)return;state.cache??={};state.cache[index]=result;output(index,panel);}
    catch(error){output(index,panel,error.message);}finally{button.disabled=state.dirty||!state.current;}
  }
  if(kind==='metric'){
    field(editor,'Metric SQL',draft.sql,value=>draft.sql=value,true);field(editor,'Value column',draft.value_column,value=>draft.value_column=value);field(editor,'Unit',draft.unit,value=>draft.unit=value);field(editor,'Definition',draft.description,value=>draft.description=value,true);
    const panel=element('div',undefined,'analysis-result');const button=element('button','Run metric');button.disabled=!state.current||state.dirty;button.onclick=()=>run(0,panel,button);runs.push(button);results.append(button,panel);output(0,panel);
  }else{
    const items=kind==='notebook'?draft.cells:draft.widgets;
    items.forEach((item,index)=>{
      const card=element('article',undefined,'analysis-item');card.append(element('h3',kind==='notebook'?`Cell ${index+1} · ${item.kind}`:item.title));
      let settings=card;
      if(kind==='dashboard'){const configuration=element('details');configuration.open=!state.current||state.dirty;configuration.append(element('summary','Configure widget'));settings=element('div');configuration.append(settings);card.append(configuration);}
      if(kind==='notebook')field(card,item.kind==='note'?'Note text':'Cell SQL',item.text,value=>item.text=value,true);
      else{
        field(settings,'Widget title',item.title,value=>item.title=value);
        if(item.kind==='table')field(settings,'Widget SQL',item.sql,value=>item.sql=value,true);
        else{const label=element('label','Metric definition');const select=element('select');select.setAttribute('aria-label','Metric definition');select.disabled=viewer;const empty=element('option','Choose a metric');empty.value='';select.append(empty);for(const metric of state.metrics||[]){const option=element('option',metric.name);option.value=metric.id;select.append(option);}select.value=item.metric_id||'';select.onchange=()=>{item.metric_id=select.value;dirty();};label.append(select);settings.append(label);}
      }
      const remove=element('button',kind==='notebook'?'Remove cell':'Remove widget');remove.disabled=viewer;remove.onclick=()=>{items.splice(index,1);state.dirty=true;state.cache={};renderAnalysis(kind);};settings.append(remove);
      const panel=element('div',undefined,'analysis-result');const button=element('button',kind==='notebook'?'Run cell':'Refresh widget');button.disabled=!state.current||state.dirty;button.onclick=()=>run(index,panel,button);runs.push(button);card.append(button,panel);output(index,panel);results.append(card);
    });
    const add=(caption,item)=>{const button=element('button',caption);button.disabled=viewer||items.length>=(kind==='notebook'?20:8);button.onclick=()=>{items.push(item());state.dirty=true;state.cache={};renderAnalysis(kind);};editor.append(button);};
    if(kind==='notebook'){add('Add SQL cell',()=>({kind:'sql',text:''}));add('Add note cell',()=>({kind:'note',text:''}));}
    else{add('Add table widget',()=>({kind:'table',title:'Table',sql:''}));add('Add metric widget',()=>({kind:'metric',title:'Metric',metric_id:''}));}
  }
  if(kind==='dashboard'&&draft.widgets.length){const refreshAll=element('button','Refresh dashboard');refreshAll.disabled=!state.current||state.dirty;refreshAll.onclick=async()=>{refreshAll.disabled=true;try{for(let index=0;index<draft.widgets.length;index++){refreshAll.textContent=`Refreshing ${index+1} of ${draft.widgets.length}…`;await run(index,results.children[index].querySelector('.analysis-result'),runs[index]);}}finally{refreshAll.textContent='Refresh dashboard';refreshAll.disabled=!state.current||state.dirty;}};runs.push(refreshAll);if(state.current)root.append(element('h2',state.current.name),refreshAll);else editor.append(refreshAll);}
  const save=element('button','Save '+kind,'primary');save.disabled=viewer;save.onclick=async()=>{save.disabled=true;try{const result=await api('/documents'+(state.current?'/'+state.current.id:''),{method:state.current?'PUT':'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(draft)});state.current=result;state.draft=structuredClone(result);state.dirty=false;state.cache={};history.replaceState(null,'','#'+analysisViews[kind]+'/'+result.id);await loadAnalysis(kind);message('Saved '+kind+'.');}catch(error){message(error.message,true);save.disabled=false;}};editor.append(save);
  if(state.current){const reload=element('button','Reload saved definition');reload.onclick=()=>{if(state.dirty&&!confirm('Discard unsaved changes?'))return;loadAnalysis(kind,state.current.id);};const remove=element('button','Delete '+kind,'danger');remove.disabled=viewer;remove.onclick=async()=>{if(prompt('Type the definition name to permanently delete it:')!==state.current.name)return;try{await api('/documents/'+state.current.id,{method:'DELETE',headers:{'Content-Type':'application/json'},body:JSON.stringify({revision:state.current.revision,confirmation_name:state.current.name})});state.current=null;state.draft=null;state.dirty=false;state.cache={};history.replaceState(null,'',location.pathname);await loadAnalysis(kind);}catch(error){message(error.message,true);}};editor.append(reload,remove);}
  if(kind==='dashboard'&&state.current){root.append(results);const configuration=element('details');configuration.open=state.dirty||!draft.widgets.length;configuration.append(element('summary','Edit dashboard definition'),editor);root.append(configuration);}else root.append(editor,results);
}
async function restoreAnalysisHash(){const match=location.hash.match(/^#(notebooks|dashboards|metrics)\/([a-f0-9]{32})$/);if(match){const kind={notebooks:'notebook',dashboards:'dashboard',metrics:'metric'}[match[1]];await switchView(match[1]);await loadAnalysis(kind,match[2]);}}
window.addEventListener('hashchange',restoreAnalysisHash);
sessionReady.then(restoreAnalysisHash);

function clearQueryResult(){state.result=null;$('query-output').hidden=true;}

function setQueryEngine(engine){$('sql-engine').value=engine;$('engine-badge').textContent=engine==='trino'?'Trino · workspace copies only':'DuckDB · read only';clearQueryResult();}
$('sql-engine').onchange=()=>setQueryEngine($('sql-engine').value);
$('sql').addEventListener('input',clearQueryResult);

$('pipeline-connector').onchange=()=>{const source=Boolean($('pipeline-connector').value);$('transform-sql').required=!source;$('transform-submit').disabled=source;};
