const {chromium}=require('playwright');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const {execFileSync}=require('node:child_process');
(async()=>{
 const root=path.resolve(__dirname,'..'),suffix=Date.now(),schema='cdc_browser_'+suffix,name='Live browser '+suffix;
 const values=Object.fromEntries(fs.readFileSync(path.join(root,'deploy/runtime/stack.env'),'utf8').trim().split('\n').map(line=>{const i=line.indexOf('=');return [line.slice(0,i),line.slice(i+1)];}));
 const fixture=`import json,sys
from pathlib import Path
import psycopg
from psycopg import sql
v=dict(line.split('=',1) for line in Path('deploy/runtime/stack.env').read_text().splitlines() if line)
request=json.load(sys.stdin)
with psycopg.connect(host='127.0.0.1',port=5433,dbname='platform',user='platform',password=v['POSTGRES_PASSWORD'],autocommit=True) as pg:
 table=sql.Identifier(request['schema'],'records')
 if request['action']=='setup':
  pg.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(request['schema'])))
  pg.execute(sql.SQL('CREATE TABLE {}(id BIGINT PRIMARY KEY,label TEXT,amount NUMERIC(12,2))').format(table))
  pg.execute(sql.SQL("INSERT INTO {} VALUES(1,'one',10),(2,'two',20)").format(table))
 elif request['action']=='change':
  with pg.transaction():
   pg.execute(sql.SQL("UPDATE {} SET label='updated',amount=15 WHERE id=1").format(table))
   pg.execute(sql.SQL('DELETE FROM {} WHERE id=2').format(table))
   pg.execute(sql.SQL("INSERT INTO {} VALUES(3,'new',30)").format(table))
 elif request['action']=='cleanup':
  if request.get('connector'):
   slot='dp_slot_'+request['connector'];publication='dp_pub_'+request['connector']
   if pg.execute('SELECT 1 FROM pg_replication_slots WHERE slot_name=%s',(slot,)).fetchone():pg.execute('SELECT pg_drop_replication_slot(%s)',(slot,))
   pg.execute(sql.SQL('DROP PUBLICATION IF EXISTS {}').format(sql.Identifier(publication)))
  pg.execute(sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(sql.Identifier(request['schema'])))
`;
 const source=(action,connector)=>execFileSync(path.join(root,'.venv/bin/python'),['-c',fixture],{cwd:root,input:JSON.stringify({schema,action,connector}),encoding:'utf8'});
 source('setup');let connectorId;
 const browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_EXECUTABLE_PATH||'/usr/bin/chromium',args:['--no-sandbox']});
 try{
  const base=process.env.DATA_PLATFORM_URL||'http://127.0.0.1:8004',page=await browser.newPage({viewport:{width:1440,height:1000}}),errors=[];
  page.on('pageerror',error=>errors.push(error.message));await page.goto(base);await page.locator('nav button[data-view=connections]').click();
  await page.locator('#connector-name').fill(name);await page.locator('#connector-user').fill('platform');await page.locator('#connector-password').fill(values.POSTGRES_PASSWORD);await page.locator('#connector-schema').fill(schema);await page.locator('#connector-table').fill('records');await page.locator('#connector-mode').selectOption('cdc');
  await page.locator('#create-connector').click();await page.locator('#message').filter({hasText:'Connection saved with encrypted credentials.'}).waitFor();
  assert.equal(await page.locator('#connector-password').inputValue(),'');
  const card=page.locator('#connector-list > .history-card').filter({has:page.getByRole('heading',{name,exact:true})});
  await card.getByRole('button',{name:'Sync live changes now',exact:true}).click();await page.locator('#message').filter({hasText:'Live changes published: 2 inserted'}).waitFor({timeout:45000});
  const definition=(await page.request.get(base+'/api/connectors').then(r=>r.json())).find(item=>item.name===name);connectorId=definition.id;
  const dataset=await page.request.get(base+'/api/datasets/'+definition.target_id).then(r=>r.json());
  // Create a saved dashboard against the stable dataset name, then use its actual refresh UI.
  const dashboard=await page.request.post(base+'/api/documents',{data:{kind:'dashboard',name:'Live dashboard '+suffix,widgets:[{kind:'table',title:'Current records',sql:'SELECT id,label,amount FROM '+dataset.table_name+' ORDER BY id'}]}}).then(r=>r.json());
  await card.getByRole('button',{name:'Enable automatic sync',exact:true}).click();await page.locator('#message').filter({hasText:'Automatic sync enabled at a 60-second interval.'}).waitFor();
  source('change');
  const deadline=Date.now()+105000;let updated=false;
  while(Date.now()<deadline){const versions=await page.request.get(base+'/api/datasets/'+dataset.id+'/versions').then(r=>r.json());if(versions.length>=2){updated=true;break;}await page.waitForTimeout(1000);}
  assert.ok(updated,'The actual embedded scheduler must publish source changes without a manual run');
  await page.goto(base+'/#dashboards/'+dashboard.id);await page.getByRole('button',{name:'Refresh dashboard',exact:true}).click();
  await page.locator('#analysis-dashboard tbody td').filter({hasText:'updated'}).waitFor();
  const rows=await page.locator('#analysis-dashboard tbody tr').allTextContents();assert.equal(rows.length,2);assert.ok(rows.some(row=>row.includes('new')));assert.ok(!rows.some(row=>row.includes('two')));
  await page.locator('nav button[data-view=connections]').click();
  await card.getByRole('button',{name:'Sync health',exact:true}).click();await card.getByText('slot status: reserved',{exact:true}).or(card.getByText('slot status: extended',{exact:true})).waitFor();
  await card.getByRole('button',{name:'Sync history',exact:true}).click();await card.getByText(/succeeded/).first().waitFor();
  await card.getByRole('button',{name:'Update connection password',exact:true}).click();await page.locator('#connector-new-password').fill(values.POSTGRES_PASSWORD);await page.getByRole('button',{name:'Save encrypted password',exact:true}).click();await page.locator('#connector-secret-dialog').waitFor({state:'hidden'});assert.equal(await page.locator('#connector-new-password').inputValue(),'');
  await card.getByRole('button',{name:'Pause automatic sync',exact:true}).click();await page.locator('#message').filter({hasText:'Automatic scheduling paused.'}).waitFor();
  const schedules=await page.request.get(base+'/api/pipelines').then(r=>r.json());assert.ok(schedules.filter(item=>item.connector_id===connectorId).every(item=>!item.enabled));
  await page.setViewportSize({width:390,height:844});assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false);await page.screenshot({path:'/tmp/platform-cdc-mobile.png',fullPage:true});
  page.once('dialog',dialog=>dialog.accept(name));await card.getByRole('button',{name:'Stop live sync',exact:true}).click();await page.locator('#message').filter({hasText:'Live sync stopped; source replication resources released.'}).waitFor();
  for(const schedule of schedules.filter(item=>item.connector_id===connectorId))assert.equal((await page.request.delete(base+'/api/pipelines/'+schedule.id,{data:{confirmation_name:schedule.name}})).status(),200);
  page.once('dialog',dialog=>dialog.accept());await card.getByRole('button',{name:'Delete connection',exact:true}).click();await card.waitFor({state:'detached'});
  assert.deepEqual(errors,[]);console.log('CDC browser passed: encrypted source creation, snapshot, actual 60-second scheduled insert/update/delete capture, stable dashboard refresh, health/history, password rotation, schedule pause, mobile layout and source-resource cleanup.');
 }finally{await browser.close();source('cleanup',connectorId);}
})().catch(error=>{console.error(error);process.exitCode=1;});
