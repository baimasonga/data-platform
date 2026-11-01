/* Explicitly executed, revision-bound business models. No data queries on open/save. */
let businessModels=[];
const businessState={current:null,draft:null,dirty:false,query:null};
function businessField(parent,label,value,change,options,readonly=state.session?.role==='viewer'){
  const wrap=element('label',label),input=element(options?'select':'input');input.setAttribute('aria-label',label);
  if(options)for(const [id,name] of options){const option=element('option',name);option.value=id;input.append(option);}
  input.value=value??'';input.disabled=readonly;input.onchange=()=>change(input.value);wrap.append(input);parent.append(wrap);return input;
}
function businessButton(parent,label,action,disabled=false){const button=element('button',label);button.disabled=disabled;button.onclick=()=>Promise.resolve(action()).catch(e=>message(e.message,true));parent.append(button);return button;}
function renderSemanticBinding(parent,binding,changed,scalar=false,readonly=state.session?.role==='viewer',fixedModel=false){
  const redraw=()=>{changed();box.replaceChildren();draw();},box=element('div');parent.append(box);
  const bindingField=(...args)=>businessField(args[0],args[1],args[2],args[3],args[4],readonly);
  function draw(){
    const modelSelect=bindingField(box,'Business model',binding.model_id,value=>{binding.model_id=value;const model=businessModels.find(m=>m.id===value);binding.model_revision=model?.revision;binding.measures=[];binding.measure_id=null;binding.group_by=[];binding.filters=[];redraw();},[['','Choose a model'],...businessModels.map(m=>[m.id,m.name])]);modelSelect.disabled=readonly||fixedModel;
    const model=businessModels.find(m=>m.id===binding.model_id);if(!model){box.append(element('p','Create a business model first, or choose an available model.'));return;}
    box.append(element('p',`Bound revision ${binding.model_revision??'unset'} · current revision ${model.revision}`));
    if(binding.model_revision!==model.revision)businessButton(box,'Use current model revision',()=>{binding.model_revision=model.revision;redraw();},readonly);
    if(scalar)bindingField(box,'Business measure',binding.measure_id,value=>{binding.measure_id=value;changed();},[['','Choose a measure'],...model.measures.map(m=>[m.id,m.name+(m.unit?' · '+m.unit:'')])]);
    else for(const [key,caption,fields] of [['measures','Measures',model.measures],['group_by','Group by',model.attributes]]){
      const group=element('fieldset');group.append(element('legend',caption));for(const field of fields){const label=element('label',field.name),input=element('input');input.type='checkbox';input.checked=(binding[key]||[]).includes(field.id);input.disabled=readonly;input.onchange=()=>{binding[key]=input.checked?[...(binding[key]||[]),field.id]:(binding[key]||[]).filter(id=>id!==field.id);changed();};label.prepend(input);group.append(label);}box.append(group);
    }
    binding.filters??=[];
    binding.filters.forEach((filter,index)=>{const row=element('div',undefined,'panel');bindingField(row,'Filter field',filter.attribute,value=>{filter.attribute=value;changed();},[['','Choose field'],...model.attributes.map(a=>[a.id,a.name])]);bindingField(row,'Filter operator',filter.op,value=>{filter.op=value;filter.values=['is_null','not_null'].includes(value)?[]:[''];redraw();},['eq','neq','gt','gte','lt','lte','is_null','not_null'].map(x=>[x,x.replaceAll('_',' ')]));if(!['is_null','not_null'].includes(filter.op))bindingField(row,'Filter value',filter.values[0],value=>{filter.values=[value];changed();});businessButton(row,'Remove filter',()=>{binding.filters.splice(index,1);redraw();},readonly);box.append(row);});
    businessButton(box,'Add filter',()=>{binding.filters.push({attribute:model.attributes[0]?.id||'',op:'eq',values:['']});redraw();},readonly||binding.filters.length>=12||!model.attributes.length);
  }draw();
}
async function loadBusinessModels(ident){
  businessModels=await api('/semantic-models');businessState.datasets=await api('/datasets');
  if(ident){businessState.current=await api('/semantic-models/'+ident);businessState.draft=structuredClone(businessState.current);businessState.dirty=false;businessState.query=null;}
  renderBusinessModels();
}
function renderBusinessResult(panel,result){
  panel.replaceChildren();panel.append(element('p',`Model revision ${result.model_revision} · ${new Date(result.refreshed_at).toLocaleString()} · ${result.rows.length} rows${result.truncated?' · result capped':''}. Preview shows up to 50 rows.`),table(result.column_labels||result.columns,result.rows.slice(0,50)));
  const details=element('details');details.append(element('summary','SQL and dataset versions'),element('pre',result.sql,'history-sql'));for(const pin of Object.values(result.input_versions||{}))details.append(element('p',`${pin.name} · version ${pin.number}`));panel.append(details);
}
function renderBusinessModels(){
  const s=businessState,root=$('business-models'),viewer=state.session?.role==='viewer';root.replaceChildren();
  const discard=()=>!s.dirty||confirm('Discard unsaved model changes?');
  businessButton(root,'New business model',()=>{if(!discard())return;s.current=null;s.draft={name:'',description:'',revision:0,fact_dataset_id:'',relationships:[],attributes:[],measures:[]};s.query=null;s.dirty=true;renderBusinessModels();},viewer);
  for(const model of businessModels)businessButton(root,`${model.name} · revision ${model.revision}`,()=>{if(discard())return loadBusinessModels(model.id);});
  if(!s.draft){root.append(element('p','Define shared measures and grouping fields, then reuse them in metrics and dashboards.'));return;}
  const draft=s.draft,editor=element('div',undefined,'panel');root.append(editor);editor.append(element('h2',s.current?'Edit business model':'New business model'));
  const status=element('p',s.dirty?'Unsaved changes. Save before querying.':'Saved. Queries run only when requested.');status.setAttribute('role','status');editor.append(status);
  const dirty=()=>{s.dirty=true;status.textContent='Unsaved changes. Save before querying.';};
  const options=()=>[['','Choose dataset'],...s.datasets.map(d=>[d.id,d.name])];
  const columns=id=>[['','Choose column'],...(s.datasets.find(d=>d.id===id)?.profile||[]).map(c=>[c.name,`${c.name} · ${c.type}`])];
  const field=(parent,label,value,change,opts)=>businessField(parent,label,value,value=>{change(value);dirty();},opts);
  const redraw=()=>{dirty();renderBusinessModels();};
  const unique=prefix=>{let n=1;const used=[...draft.relationships,...draft.attributes,...draft.measures].map(f=>f.id);while(used.includes(prefix+n))n++;return prefix+n;};
  field(editor,'Model name',draft.name,v=>draft.name=v);field(editor,'Model description',draft.description,v=>draft.description=v);
  field(editor,'Main dataset',draft.fact_dataset_id,v=>{draft.fact_dataset_id=v;redraw();},options());
  editor.append(element('h3','Lookup relationships'),element('p','Lookup keys must be unique, non-null and have the same type as the main key. Unmatched main rows are retained.'));
  draft.relationships.forEach((rel,i)=>{const card=element('div',undefined,'panel');editor.append(card);card.append(element('p','Stable ID: '+rel.id));field(card,'Relationship name',rel.name,v=>rel.name=v);field(card,'Lookup dataset',rel.dataset_id,v=>{rel.dataset_id=v;redraw();},options());field(card,'Main key',rel.fact_key,v=>rel.fact_key=v,columns(draft.fact_dataset_id));field(card,'Lookup key',rel.dimension_key,v=>rel.dimension_key=v,columns(rel.dataset_id));businessButton(card,'Remove relationship',()=>{draft.relationships.splice(i,1);redraw();},viewer);});
  businessButton(editor,'Add relationship',()=>{draft.relationships.push({id:unique('lookup_'),name:'Lookup',dataset_id:'',fact_key:'',dimension_key:''});redraw();},viewer||draft.relationships.length>=8);
  editor.append(element('h3','Grouping fields'));
  draft.attributes.forEach((attr,i)=>{const card=element('div',undefined,'panel');editor.append(card);card.append(element('p','Stable ID: '+attr.id));field(card,'Field name',attr.name,v=>attr.name=v);field(card,'Field table',attr.table,v=>{attr.table=v;redraw();},[['fact','Main dataset'],...draft.relationships.map(r=>[r.id,r.name])]);field(card,'Field column',attr.column,v=>attr.column=v,columns(attr.table==='fact'?draft.fact_dataset_id:draft.relationships.find(r=>r.id===attr.table)?.dataset_id));businessButton(card,'Remove grouping field',()=>{draft.attributes.splice(i,1);redraw();},viewer);});
  businessButton(editor,'Add grouping field',()=>{draft.attributes.push({id:unique('group_'),name:'Group',table:'fact',column:''});redraw();},viewer||draft.attributes.length>=40);
  editor.append(element('h3','Measures'));
  draft.measures.forEach((measure,i)=>{const card=element('div',undefined,'panel');editor.append(card);card.append(element('p','Stable ID: '+measure.id));field(card,'Measure name',measure.name,v=>measure.name=v);field(card,'Aggregation',measure.aggregate,v=>{measure.aggregate=v;redraw();},['sum','count','count_distinct','avg','min','max','ratio'].map(v=>[v,v.replaceAll('_',' ')]));if(measure.aggregate==='ratio'){const base=[['','Choose base measure'],...draft.measures.filter(m=>m.aggregate!=='ratio').map(m=>[m.id,m.name])];field(card,'Numerator',measure.numerator,v=>measure.numerator=v,base);field(card,'Denominator',measure.denominator,v=>measure.denominator=v,base);field(card,'Ratio scale',measure.scale||1,v=>measure.scale=Number(v),[['1','Ratio'],['100','Percentage']]);}else field(card,'Measure column',measure.column||'',v=>measure.column=v||null,measure.aggregate==='count'?[['','All rows'],...columns(draft.fact_dataset_id).slice(1)]:columns(draft.fact_dataset_id));field(card,'Measure unit',measure.unit,v=>measure.unit=v);field(card,'Measure description',measure.description,v=>measure.description=v);businessButton(card,'Remove measure',()=>{draft.measures.splice(i,1);redraw();},viewer);});
  businessButton(editor,'Add measure',()=>{draft.measures.push({id:unique('measure_'),name:'Measure',aggregate:'count',column:null,unit:'',description:'',scale:1});redraw();},viewer||draft.measures.length>=30);
  businessButton(editor,'Save business model',async()=>{const result=await api('/semantic-models'+(s.current?'/'+s.current.id:''),{method:s.current?'PUT':'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(draft)});await loadBusinessModels(result.id);message('Business model saved. No data queries were run.');},viewer);
  if(!s.current)return;
  const id=s.current.id;
  businessButton(editor,'Reload saved model',()=>{if(discard())return loadBusinessModels(id);});
  businessButton(editor,'Delete business model',async()=>{if(prompt('Type the model name to delete its definition and query history:')!==s.current.name)return;await api('/semantic-models/'+id,{method:'DELETE',headers:{'Content-Type':'application/json'},body:JSON.stringify({revision:s.current.revision,confirmation_name:s.current.name})});s.current=null;s.draft=null;s.query=null;s.dirty=false;await loadBusinessModels();},viewer);
  const explorer=element('div',undefined,'panel');root.append(explorer);explorer.append(element('h2','Explore saved model'));
  s.query??={model_id:id,model_revision:s.current.revision,measures:[s.current.measures[0].id],group_by:[],filters:[],order_by:[],limit:1000};
  renderSemanticBinding(explorer,s.query,()=>{},false,false,true);
  businessField(explorer,'Row limit',s.query.limit,v=>s.query.limit=Number(v),undefined,false);
  const resultPanel=element('div',undefined,'analysis-result');
  const run=businessButton(explorer,'Run business query',async()=>{if(s.dirty)throw new Error('Save the model before querying.');if(s.query.model_id!==id)throw new Error('Open the selected model to explore it.');run.disabled=true;try{const result=await api('/semantic-models/'+id+'/query',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({...s.query,revision:s.query.model_revision})});renderBusinessResult(resultPanel,result);}catch(e){resultPanel.prepend(element('p','Refresh failed; any visible result is from the previous successful run. '+e.message,'import-error'));}finally{run.disabled=false;}});explorer.append(resultPanel);
  const records=element('div');businessButton(explorer,'Load model history',async()=>{records.replaceChildren();for(const entry of await api('/semantic-models/'+id+'/runs')){const row=element('article',undefined,'history-card');row.append(element('p',`${new Date(entry.started_at).toLocaleString()} · revision ${entry.model_revision} · ${entry.status}`));if(entry.error)row.append(element('p',entry.error,'import-error'));businessButton(row,'View recorded run',async()=>{const record=await api('/semantic-models/'+id+'/runs/'+entry.id);if(record.result)renderBusinessResult(resultPanel,record.result);else resultPanel.replaceChildren(element('p',record.error||record.status));});records.append(row);}const revisions=await api('/semantic-models/'+id+'/versions');records.append(element('p',`Saved revisions: ${revisions.map(r=>r.revision).join(', ')}`));});explorer.append(records);
}
