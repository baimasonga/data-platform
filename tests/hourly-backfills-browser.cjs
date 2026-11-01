const {chromium}=require('playwright');
const assert=require('node:assert/strict');
(async()=>{
 const browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_EXECUTABLE_PATH||'/usr/bin/chromium',args:['--no-sandbox']});
 try{
  const base=process.env.DATA_PLATFORM_URL||'http://127.0.0.1:8009',page=await browser.newPage({viewport:{width:1440,height:1000}}),errors=[],suffix=Date.now();page.on('pageerror',e=>errors.push(e.message));
  const pipe=await page.request.post(base+'/api/pipelines',{data:{name:'Hours '+suffix,sql:'SELECT {{partition_start}} AS begins,{{partition_end}} AS ends,CAST({{partition_date}} AS DATE) AS day'}}).then(r=>r.json());
  const definition=await page.request.post(base+'/api/flows',{data:{name:'Hourly workflow '+suffix,steps:[{id:'hour',name:'Hour',kind:'pipeline',pipeline_id:pipe.id}]}}).then(r=>r.json());
  await page.goto(base);await page.locator('nav button[data-view=workflows]').click();const root=page.locator('#flow-workflows');await root.getByRole('button',{name:definition.name,exact:true}).click();
  await root.getByLabel('Backfill start date',{exact:true}).fill('2026-01-20');await root.getByLabel('Backfill end date',{exact:true}).fill('2026-01-21');
  await root.getByLabel('Backfill interval',{exact:true}).selectOption('hour');await root.getByLabel('Backfill start hour (UTC)',{exact:true}).fill('2026-01-01T23:00');await root.getByLabel('Backfill end hour (UTC)',{exact:true}).fill('2026-01-02T00:00');await root.getByText(/2 inclusive hourly partitions in UTC/).waitFor();
  await root.getByLabel('Backfill interval',{exact:true}).selectOption('day');assert.equal(await root.getByLabel('Backfill start date',{exact:true}).inputValue(),'2026-01-20');await root.getByLabel('Backfill interval',{exact:true}).selectOption('hour');assert.equal(await root.getByLabel('Backfill start hour (UTC)',{exact:true}).inputValue(),'2026-01-01T23:00');
  // Rerendering after editing a step must retain selected hour boundaries.
  await root.getByLabel('Step type',{exact:true}).selectOption('pipeline');assert.equal(await root.getByLabel('Backfill end hour (UTC)',{exact:true}).inputValue(),'2026-01-02T00:00');await root.getByLabel('Saved pipeline',{exact:true}).selectOption(pipe.id);await root.getByRole('button',{name:'Save workflow',exact:true}).click();await page.locator('#message').filter({hasText:'Workflow saved.'}).waitFor();
  await root.getByRole('button',{name:'Run hourly backfill',exact:true}).click();await root.locator('.flow-run h3').filter({hasText:'2026-01-01T23:00:00+00:00 · succeeded'}).waitFor({timeout:45000});await root.locator('.flow-run h3').filter({hasText:'2026-01-02T00:00:00+00:00 · succeeded'}).waitFor({timeout:45000});
  const runs=await page.request.get(base+'/api/flows/'+definition.id+'/runs').then(r=>r.json());assert.equal(runs.length,2);assert.equal(new Set(runs.map(r=>r.steps[0].result.id)).size,2);assert.ok(runs.every(r=>r.detached&&r.partition_granularity==='hour'));
  const ids=new Set(runs.map(r=>r.id));await root.getByRole('button',{name:'Refresh workflow history',exact:true}).click();assert.equal((await page.request.get(base+'/api/flows/'+definition.id+'/runs').then(r=>r.json())).length,2);
  await root.locator('.flow-run details summary').first().click();await root.locator('.flow-run details pre').first().waitFor();assert.match(await root.locator('.flow-run details pre').first().textContent(),/"granularity": "hour"/);
  await root.getByLabel('Backfill end hour (UTC)',{exact:true}).fill('2026-01-04T00:00');await root.getByRole('button',{name:'Run hourly backfill',exact:true}).click();await page.locator('#message').filter({hasText:'1–48 inclusive hourly partitions'}).waitFor();assert.deepEqual(new Set((await page.request.get(base+'/api/flows/'+definition.id+'/runs').then(r=>r.json())).map(r=>r.id)),ids);
  await page.setViewportSize({width:390,height:844});assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false,'Hourly controls overflow on mobile');await page.screenshot({path:'/tmp/platform-hourly-mobile.png',fullPage:true});assert.deepEqual(errors,[]);
  console.log('Hourly backfill browser passed: UTC range selection, retained daily/hourly fields, dependency-editor rerenders, isolated hourly outputs, history/snapshots, rejected oversized ranges and mobile layout.');
 }finally{await browser.close();}
})().catch(e=>{console.error(e);process.exitCode=1;});
