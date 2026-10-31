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
async function switchView(view) { state.view=view; document.querySelectorAll('main > section').forEach(e=>e.hidden=e.id!=='view-'+view); document.querySelectorAll('nav button').forEach(b=>b.classList.toggle('active',b.dataset.view===view)); $('breadcrumb').textContent=view.toUpperCase(); const titles={datasets:['Your data, understood.','Import, inspect and query data in one place.'],sql:['Ask your data.','Write SQL, inspect results and explore patterns.'],saved:['Keep your best questions.','Reusable SQL, ready when you need it.'],history:['Every question, recorded.','Review SQL runs, diagnose errors and revisit your work.'],imports:['Follow every import.','Track successful uploads and understand what needs attention.'],activity:['A record of your work.','Recent imports and query activity in this installation.']}; $('page-title').textContent=titles[view][0];$('subtitle').textContent=titles[view][1]; try { if(view==='history') await loadHistory();if(view==='imports') await loadImports();if(view==='saved') await loadSaved();if(view==='activity')await loadActivity(); }catch(e){message(e.message,true);} }
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
async function inspect(id) {try{const d=await api('/datasets/'+id);const panel=$('dataset-detail');panel.hidden=false;panel.replaceChildren();const box=element('div',undefined,'panel');const heading=element('div',undefined,'panel-heading');heading.append(element('h2',d.name));const close=element('button','Close');close.onclick=()=>panel.hidden=true;heading.append(close);box.append(heading);const actions=element('div',undefined,'detail-actions'),query=element('button','Query this dataset','primary');query.onclick=()=>{$('sql').value=`SELECT *\nFROM "${d.table_name}"\nLIMIT 100;`;switchView('sql');};const rename=element('button','Rename dataset');rename.onclick=()=>openRename(d);const remove=element('button','Delete dataset','danger');remove.onclick=()=>openDelete(d);const exportButton=element('button','Export dataset CSV');exportButton.onclick=()=>exportDataset(d,exportButton);actions.append(query,exportButton,rename,remove);box.append(actions,element('p','SQL table: '+d.table_name),element('p',`CSV export includes all ${d.row_count.toLocaleString()} rows.`));if(d.source){box.append(element('p','Source SHA-256: '+d.source.sha256,'source-provenance'));const download=element('a','Download original source');download.href='/api/datasets/'+encodeURIComponent(d.id)+'/source';actions.append(download);}box.append(element('h3','Column quality'),table(['Column','Type','Missing','Distinct','Complete'],d.profile.map(c=>[c.name,c.type,c.nulls,c.distinct,c.completeness+'%'])));box.append(element('h3','Preview · first 50 records'),table(d.preview.columns,d.preview.rows));panel.append(box);}catch(e){message(e.message,true);} }
$('run-query').addEventListener('click',async()=>{const button=$('run-query');button.disabled=true;button.textContent='Running…';$('query-output').hidden=true;state.result=null;try{const result=await api('/query',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({sql:$('sql').value})});state.result=result;$('query-table').replaceChildren(table(result.columns,result.rows));$('query-info').textContent=`${result.rows.length.toLocaleString()} rows · ${result.elapsed_ms} ms${result.truncated?' · limited to 1,000 rows':''}`;$('query-output').hidden=false;$('chart').replaceChildren();['chart-category','chart-value'].forEach(id=>{$(id).replaceChildren();result.columns.forEach((name,index)=>{const option=element('option',name);option.value=index;$(id).append(option);});});const numeric=result.columns.findIndex((_,i)=>result.rows.some(r=>typeof r[i]==='number'));if(numeric>=0)$('chart-value').value=numeric;message('Query completed.');}catch(e){message(e.message,true);}finally{button.disabled=false;button.textContent='Run query ↗';}});
$('sql').addEventListener('keydown',e=>{if((e.ctrlKey||e.metaKey)&&e.key==='Enter'){e.preventDefault();$('run-query').click();}});
$('save-query').onclick=()=>{if(!$('sql').value.trim()){message('Enter a query first.',true);return;}$('save-dialog').showModal();};$('cancel-save').onclick=()=>$('save-dialog').close();
$('save-form').addEventListener('submit',async e=>{e.preventDefault();try{await api('/saved-queries',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({name:$('query-name').value,sql:$('sql').value})});$('save-dialog').close();$('query-name').value='';message('Query saved.');}catch(err){message(err.message,true);}});
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
      $('sql').value = query.sql;
      state.result = null;
      $('query-output').hidden = true;
      switchView('sql');
    };
    const edit = element('button', 'Edit query');
    edit.onclick = () => openSavedDialog('edit', query);
    const remove = element('button', 'Delete query', 'danger');
    remove.onclick = () => openSavedDialog('delete', query);
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
    const snapshot = {name: query.name, sql: query.sql};
    try {
      await api('/saved-queries/' + encodeURIComponent(query.id), {
        method: kind === 'edit' ? 'PUT' : 'DELETE',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(kind === 'edit' ? {
          name: $('edit-saved-name').value, sql: $('edit-saved-sql').value, expected: snapshot
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
    card.append(heading, sql);
    if (run.elapsed_ms !== null) card.append(element('p', `${run.elapsed_ms.toLocaleString()} ms total duration`));
    if (run.status === 'running') card.append(element('p', 'Running or waiting for another operation to finish.'));
    if (run.result_rows !== null) card.append(element('p', `${run.result_rows.toLocaleString()} rows returned${run.truncated ? ' · limited to 1,000 rows' : ''}`));
    if (run.error) card.append(element('p', run.error, 'import-error'));
    const open = element('button', 'Open in SQL editor');
    open.onclick = () => {
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
