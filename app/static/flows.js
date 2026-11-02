'use strict';
const flowUI={list:[],pipelines:[],current:null,draft:null,dirty:false,poll:null,sequence:0,pending:null,dates:{},granularity:'day'};
async function loadFlows(){
  try{const [list,pipelines]=await Promise.all([api('/flows'),api('/pipelines')]);flowUI.list=list;flowUI.pipelines=pipelines;renderFlows();}
  catch(error){message(error.message,true);}
}
function renderFlows(){
  const root=$('flow-workflows');if(!root)return;
  clearTimeout(flowUI.poll);flowUI.sequence++;root.replaceChildren();
  const panel=element('div',undefined,'panel');panel.append(element('h2','Multi-step workflows'),element('p','Connect saved pipelines, quality checks and lakehouse publication. Steps run after their dependencies succeed.'));
  const create=element('button','New workflow');create.onclick=()=>{if(flowUI.dirty&&!confirm('Discard unsaved workflow changes?'))return;flowUI.current=null;flowUI.draft={name:'',revision:0,enabled:false,interval_seconds:3600,steps:[],catch_up:'none',catch_up_start:null};flowUI.dirty=false;renderFlows();};panel.append(create);
  for(const definition of flowUI.list){const open=element('button',definition.name);open.onclick=()=>{if(flowUI.dirty&&!confirm('Discard unsaved workflow changes?'))return;flowUI.current=definition;flowUI.draft=structuredClone(definition);flowUI.dirty=false;renderFlows();};panel.append(open);}root.append(panel);
  if(!flowUI.draft)return;
  const draft=flowUI.draft,editor=element('form',undefined,'panel flow-editor');editor.append(element('h3',flowUI.current?'Edit workflow':'New workflow'));
  const status=element('p',flowUI.dirty?'Unsaved changes. Save before running.':'Enabled schedules may queue work after saving.');status.setAttribute('role','status');
  const runtimeButtons=[];
  const dirty=()=>{flowUI.dirty=true;status.textContent='Unsaved changes. Save before running.';runtimeButtons.forEach(button=>button.disabled=true);};
  function field(parent,caption,value,update,type='text'){
    const label=element('label',caption),input=element('input');input.type=type;input.value=value??'';input.setAttribute('aria-label',caption);input.required=true;input.oninput=()=>{update(type==='number'?input.valueAsNumber:input.value);dirty();};label.append(input);parent.append(label);return input;
  }
  function select(parent,caption,value,options,update){
    const label=element('label',caption),input=element('select');input.setAttribute('aria-label',caption);
    for(const [key,text] of options){const option=element('option',text);option.value=key;input.append(option);}input.value=value||'';input.onchange=()=>{update(input.value);dirty();renderFlows();};label.append(input);parent.append(label);return input;
  }
  field(editor,'Workflow name',draft.name,value=>draft.name=value).maxLength=200;
  const schedule=element('label',' Enable automatic workflow schedule'),enabled=element('input');enabled.type='checkbox';enabled.checked=draft.enabled;enabled.onchange=()=>{draft.enabled=enabled.checked;dirty();};schedule.prepend(enabled);editor.append(schedule);
  const interval=field(editor,'Workflow interval in seconds',draft.interval_seconds,value=>draft.interval_seconds=value,'number');interval.min=60;interval.max=31536000;
  select(editor,'Missed partition catch-up',draft.catch_up||'none',[['none','Off · normal interval runs'],['day','Completed UTC days'],['hour','Completed UTC hours']],value=>{draft.catch_up=value;draft.catch_up_start=value==='none'?null:new Date().toISOString().slice(0,value==='day'?10:13)+(value==='hour'?':00:00Z':'');});
  if(draft.catch_up&&draft.catch_up!=='none'){
    const hourly=draft.catch_up==='hour';
    const first=field(editor,hourly?'First catch-up hour (UTC)':'First catch-up date',draft.catch_up_start?.slice(0,hourly?16:10)||'',value=>draft.catch_up_start=value+(hourly&&value?'Z':''),hourly?'datetime-local':'date');if(hourly)first.step=3600;
    editor.append(element('p','SQL workflows only. Enabling the schedule automatically queues completed partitions after saving, with separate outputs and pinned inputs. Changing the partition type or first partition resets catch-up from the selected start. Failures require explicit retry before later batches continue.'));
  }
  editor.append(element('p','Scheduling uses the configured embedded worker or Dagster. In Dagster mode its daemon and a token for this workspace must be available.'),status);
  draft.steps.forEach((step,index)=>{
    const card=element('article',undefined,'flow-step');card.append(element('h4',`Step ${index+1} · ${step.id}`));
    field(card,'Step name',step.name,value=>step.name=value).maxLength=100;
    select(card,'Step type',step.kind,[['pipeline','Run saved pipeline'],['quality','Check quality'],['publish','Publish to lakehouse']],value=>{step.kind=value;step.pipeline_id=null;step.input_step=null;step.rules=[];});
    if(step.kind==='pipeline')select(card,'Saved pipeline',step.pipeline_id,[['','Choose a pipeline'],...flowUI.pipelines.map(item=>[item.id,item.name])],value=>step.pipeline_id=value);
    const dependencies=element('fieldset');dependencies.append(element('legend','Run after these steps'));
    for(const other of draft.steps.filter(item=>item.id!==step.id)){const label=element('label',other.name+' ('+other.id+')'),check=element('input');check.type='checkbox';check.checked=step.depends_on.includes(other.id);check.onchange=()=>{step.depends_on=check.checked?[...step.depends_on,other.id]:step.depends_on.filter(id=>id!==other.id);dirty();};label.prepend(check);dependencies.append(label);}card.append(dependencies);
    if(step.kind!=='pipeline')select(card,'Dataset from step',step.input_step,[['','Choose an input'],...draft.steps.filter(item=>item.id!==step.id&&item.kind!=='publish').map(item=>[item.id,item.name+' ('+item.id+')'])],value=>{step.input_step=value;if(value&&!step.depends_on.includes(value))step.depends_on.push(value);});
    if(step.kind==='quality'){
      for(const [ruleIndex,rule] of step.rules.entries()){
        const row=element('div',undefined,'flow-rule');field(row,'Quality column',rule.column_name,value=>rule.column_name=value);
        select(row,'Quality rule',rule.rule,[['not_null','No missing values'],['unique','Unique and not null'],['min','Minimum'],['max','Maximum']],value=>rule.rule=value);
        if(['min','max'].includes(rule.rule))field(row,'Quality bound',rule.threshold,value=>rule.threshold=value,'number').step='any';
        const remove=element('button','Remove workflow rule');remove.type='button';remove.onclick=()=>{step.rules.splice(ruleIndex,1);dirty();renderFlows();};row.append(remove);card.append(row);
      }
      const addRule=element('button','Add workflow rule');addRule.type='button';addRule.disabled=step.rules.length>=30;addRule.onclick=()=>{step.rules.push({column_name:'',rule:'not_null',threshold:null});dirty();renderFlows();};card.append(addRule);
    }
    const remove=element('button','Remove workflow step');remove.type='button';remove.onclick=()=>{draft.steps.splice(index,1);for(const other of draft.steps){other.depends_on=other.depends_on.filter(id=>id!==step.id);if(other.input_step===step.id)other.input_step=null;}dirty();renderFlows();};card.append(remove);editor.append(card);
  });
  const add=element('button','Add workflow step');add.type='button';add.disabled=draft.steps.length>=20;add.onclick=()=>{let index=1;while(draft.steps.some(step=>step.id==='step_'+index))index++;draft.steps.push({id:'step_'+index,name:'Step '+index,kind:'pipeline',pipeline_id:null,input_step:null,depends_on:[],rules:[]});dirty();renderFlows();};
  const save=element('button','Save workflow','primary');save.type='submit';editor.onsubmit=async event=>{event.preventDefault();save.disabled=true;try{const saved=await api('/flows'+(flowUI.current?'/'+flowUI.current.id:''),{method:flowUI.current?'PUT':'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(draft)});flowUI.current=saved;flowUI.draft=structuredClone(saved);flowUI.dirty=false;await loadFlows();message('Workflow saved.');}catch(error){message(error.message,true);}finally{save.disabled=false;}};
  editor.append(add,save,element('p','In saved pipeline SQL, use {{steps.step_1}} as an upstream table and {{partition_date}} as a date value or {{partition_start}} / {{partition_end}} as UTC timestamp boundaries, without surrounding quotes. Add that upstream step as a dependency.'));
  if(flowUI.current){const remove=element('button','Delete workflow and history','danger');remove.type='button';remove.onclick=async()=>{if(prompt('Delete this workflow and its run history? Outputs are kept. Type its name:')!==flowUI.current.name)return;try{await api('/flows/'+flowUI.current.id,{method:'DELETE',headers:{'Content-Type':'application/json'},body:JSON.stringify({revision:flowUI.current.revision,confirmation_name:flowUI.current.name})});flowUI.current=null;flowUI.draft=null;flowUI.dirty=false;await loadFlows();}catch(error){message(error.message,true);}};editor.append(remove);}root.append(editor);
  if(!flowUI.current)return;
  const controls=element('div',undefined,'panel flow-controls');controls.append(element('h3','Run or backfill'),element('p','Runs use saved definitions and pinned input versions. Retries reuse successful steps. Backfills are SQL-only and create separate output datasets for each partition. Daily ranges allow up to 31 days; hourly ranges allow up to 48 whole UTC hours. Both endpoints identify included partitions.'));
  function dateField(caption,key,type='date'){
    const label=element('label',caption),input=element('input');input.type=type;
    if(!Object.hasOwn(flowUI.dates,key))flowUI.dates[key]=new Date().toISOString().slice(0,type==='date'?10:13)+(type==='date'?'':':00');
    input.value=flowUI.dates[key];input.oninput=()=>{flowUI.dates[key]=input.value;preview();};input.setAttribute('aria-label',caption);if(type==='datetime-local')input.step=3600;label.append(input);controls.append(label);return input;
  }
  const partition=dateField('Workflow run date','partition');
  const granularityLabel=element('label','Backfill interval'),granularity=element('select');granularity.setAttribute('aria-label','Backfill interval');
  for(const [value,caption] of [['day','Daily'],['hour','Hourly · UTC']]){const option=element('option',caption);option.value=value;granularity.append(option);}
  granularity.value=flowUI.granularity;granularity.onchange=()=>{flowUI.granularity=granularity.value;renderFlows();};granularityLabel.append(granularity);controls.append(granularityLabel);
  const hourly=flowUI.granularity==='hour',start=dateField(hourly?'Backfill start hour (UTC)':'Backfill start date',hourly?'start_hour':'start',hourly?'datetime-local':'date'),end=dateField(hourly?'Backfill end hour (UTC)':'Backfill end date',hourly?'end_hour':'end',hourly?'datetime-local':'date');
  const rangeInfo=element('p');rangeInfo.setAttribute('role','status');controls.append(rangeInfo);
  function preview(){
    if(!start?.value||!end?.value){rangeInfo.textContent='Choose a start and end partition.';return;}
    const begin=Date.parse(start.value+(hourly?'Z':'T00:00:00Z')),finish=Date.parse(end.value+(hourly?'Z':'T00:00:00Z')),count=(finish-begin)/(hourly?3600000:86400000)+1,max=hourly?48:31;
    rangeInfo.textContent=Number.isInteger(count)&&count>=1&&count<=max?`${count} inclusive ${hourly?'hourly':'daily'} partition${count===1?'':'s'} in UTC. Each window includes its start and excludes its end.`:`Choose 1–${max} whole ${hourly?'UTC hours':'days'} in increasing order.`;
  }preview();
  async function queue(button,path,body){button.disabled=true;const fingerprint=JSON.stringify({path,body});if(flowUI.pending?.fingerprint!==fingerprint)flowUI.pending={fingerprint,key:crypto.randomUUID()};try{const result=await api('/flows/'+flowUI.current.id+'/'+path,{method:'POST',headers:{'Content-Type':'application/json','Idempotency-Key':flowUI.pending.key},body:JSON.stringify(body)});flowUI.pending=null;message(`Queued ${result.run_ids.length} workflow run${result.run_ids.length===1?'':'s'}.`);await loadFlowRuns();}catch(error){message(error.message,true);}finally{button.disabled=flowUI.dirty;}}
  const run=element('button','Run workflow');run.disabled=flowUI.dirty;run.onclick=()=>queue(run,'run',{revision:flowUI.current.revision,partition_date:partition.value});
  const backfill=element('button',hourly?'Run hourly backfill':'Run daily backfill');backfill.disabled=flowUI.dirty;backfill.onclick=()=>{if(!start.value||!end.value||!start.reportValidity()||!end.reportValidity()){message('Choose valid partition boundaries.',true);return;}return queue(backfill,'backfill',{revision:flowUI.current.revision,granularity:flowUI.granularity,start:start.value+(hourly?'Z':''),end:end.value+(hourly?'Z':'')});};runtimeButtons.push(run,backfill);
  const refresh=element('button','Refresh workflow history');refresh.onclick=loadFlowRuns;controls.append(run,backfill,refresh);root.append(controls,element('div',undefined,'panel flow-schedule-status'),element('div',undefined,'flow-run-list'));loadFlowScheduleStatus();loadFlowRuns();
  if(flowUI.current.enabled){const pause=element('button','Pause workflow schedule');pause.onclick=async()=>{pause.disabled=true;try{const saved=await api('/flows/'+flowUI.current.id+'/pause',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({revision:flowUI.current.revision})});flowUI.current=saved;flowUI.draft.enabled=false;flowUI.draft.revision=saved.revision;renderFlows();message('Workflow schedule paused. Current work continues unless cancelled separately.');}catch(error){message(error.message,true);pause.disabled=false;}};controls.append(pause);}
}
async function loadFlowRuns(){
  if(!flowUI.current||state.view!=='workflows')return;
  clearTimeout(flowUI.poll);const id=flowUI.current.id,sequence=flowUI.sequence,request=flowUI.runRequest=(flowUI.runRequest||0)+1;
  try{
    const runs=await api('/flows/'+id+'/runs');if(flowUI.current?.id!==id||sequence!==flowUI.sequence||request!==flowUI.runRequest)return;
    const list=$('flow-workflows').querySelector('.flow-run-list');if(!list)return;list.replaceChildren();if(!runs.length)list.append(element('p','No workflow runs yet.'));
    for(const run of runs){
      const card=element('article',undefined,'history-card flow-run');card.append(element('h3',`${run.partition_key||run.partition_date} · ${run.status}`),element('p',`Revision ${run.revision} · attempt ${run.attempt}${run.detached?' · separate backfill outputs':''}`));card.append(element('p','Started by: '+(run.trigger||'legacy')));if(run.partition_start)card.append(element('p',`${run.partition_granularity==='hour'?'Hourly':'Daily'} UTC window: ${run.partition_start} to ${run.partition_end} (end excluded).`));if(run.error)card.append(element('p',run.error,'import-error'));
      for(const step of run.steps){const row=element('div',undefined,'flow-step-status');row.append(element('strong',step.step_id+' · '+step.status));if(step.error)row.append(element('pre',step.error,'history-sql'));if(step.result){row.append(element('p','Output version '+(step.result.version??step.result.version_id)));if(step.result.physical_table){const query=element('button','Query output version');query.onclick=()=>{setQueryEngine('duckdb');$('sql').value='SELECT * FROM "'+step.result.physical_table+'" LIMIT 1000';switchView('sql');};row.append(query);}if(step.result.quality_results)row.append(table(['Column','Rule','Passed'],step.result.quality_results.map(rule=>[rule.column_name,rule.rule,rule.passed?'Yes':'No'])));}card.append(row);}
      if(['failed','interrupted','cancelled','queued','running'].includes(run.status)){const action=['queued','running'].includes(run.status)?'cancel':'retry';const control=element('button',action==='cancel'?'Cancel workflow run':'Retry failed steps');control.onclick=async()=>{control.disabled=true;try{await api('/flows/'+id+'/runs/'+run.id+'/'+action,{method:'POST'});await loadFlowRuns();}catch(error){message(error.message,true);control.disabled=false;}};card.append(control);}
      const details=element('details');const summary=element('summary','Saved definition and input versions');details.append(summary);details.ontoggle=async()=>{if(!details.open||details.dataset.loaded)return;try{const saved=await api('/flows/'+id+'/runs/'+run.id);details.append(element('pre',JSON.stringify(saved.snapshot,null,2),'history-sql'));details.dataset.loaded='1';}catch(error){message(error.message,true);}};card.append(details);list.append(card);
    }
    loadFlowScheduleStatus();if(runs.some(run=>['queued','running'].includes(run.status)))flowUI.poll=setTimeout(loadFlowRuns,1000);
  }catch(error){message(error.message,true);}
}

async function loadFlowScheduleStatus(){
  if(!flowUI.current||state.view!=='workflows')return;
  const id=flowUI.current.id,sequence=flowUI.sequence,request=flowUI.scheduleRequest=(flowUI.scheduleRequest||0)+1;
  try{
    const status=await api('/flows/'+id+'/schedule');if(flowUI.current?.id!==id||flowUI.sequence!==sequence||request!==flowUI.scheduleRequest)return;
    const panel=$('flow-workflows').querySelector('.flow-schedule-status');if(!panel)return;
    panel.replaceChildren(element('h3','Saved schedule status'),element('p',status.enabled?'Schedule enabled':'Schedule paused'));
    if(status.next_run)panel.append(element('p','Next schedule check: '+status.next_run));
    if(status.catch_up!=='none'){
      panel.append(element('p',`Generation ${status.generation} · ${status.due_partitions} completed partitions waiting to be queued. Next partition: ${status.next_partition}`),element('p','The checkpoint records accepted runs, not successful outputs. Unsuccessful runs remain in history for explicit retry.'));
      if(status.has_active_runs)panel.append(element('p','Waiting for accepted runs to finish.'));
      if(status.last_checked)panel.append(element('p','Last checked: '+status.last_checked));
      if(status.last_error)panel.append(element('p',status.last_error,'import-error'));
      if(status.next_batch.length){const details=element('details');details.append(element('summary','Preview next completed partitions'),table(['Partition','End (excluded)'],status.next_batch.slice(0,5).map(p=>[p.key,p.end])),element('p',`Next batch: up to ${status.next_batch.length} partitions, subject to queue capacity.`));panel.append(details);}
      const restart=element('button','Restart catch-up from configured start');restart.disabled=status.has_active_runs;restart.onclick=async()=>{if(flowUI.dirty){message('Save or reload workflow changes before restarting catch-up.',true);return;}if(prompt('This keeps old history and creates separate outputs again from the configured start. Type the workflow name:')!==flowUI.current.name)return;try{await api('/flows/'+id+'/schedule/reset',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({revision:flowUI.current.revision,confirmation_name:flowUI.current.name})});await loadFlowRuns();message('Catch-up checkpoint restarted. Enabled schedules may queue work.');}catch(e){message(e.message,true);}};panel.append(restart);
      for(const blocked of status.blocked_runs){const row=element('div');row.append(element('p',`Blocked by ${blocked.id} · ${blocked.status}`));const retry=element('button','Retry blocking catch-up run');retry.onclick=async()=>{retry.disabled=true;try{await api('/flows/'+id+'/runs/'+blocked.id+'/retry',{method:'POST'});await loadFlowRuns();}catch(e){message(e.message,true);retry.disabled=false;}};row.append(retry);panel.append(row);}
    }
    const refresh=element('button','Refresh schedule status');refresh.onclick=loadFlowScheduleStatus;panel.append(refresh);
  }catch(e){message(e.message,true);}
}
