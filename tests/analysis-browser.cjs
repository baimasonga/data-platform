const {chromium}=require('playwright');
const assert=require('node:assert/strict');
(async()=>{
 const browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_EXECUTABLE_PATH||'/usr/bin/chromium',args:['--no-sandbox']});
 try{
  const page=await browser.newPage({viewport:{width:1440,height:1000}});const errors=[];page.on('pageerror',error=>errors.push(error.message));
  const base=process.env.DATA_PLATFORM_URL||'http://127.0.0.1:8003';const suffix=Date.now();await page.goto(base);
  await page.locator('nav button[data-view=metrics]').click();await page.getByRole('button',{name:'New metric',exact:true}).click();
  await page.getByRole('textbox',{name:'metric name',exact:true}).fill('Metric '+suffix);await page.getByRole('textbox',{name:'Metric SQL',exact:true}).fill('SELECT 42 AS value');await page.getByRole('textbox',{name:'Unit',exact:true}).fill('units');
  await page.getByRole('button',{name:'Save metric',exact:true}).click();await page.locator('#message').filter({hasText:'Saved metric.'}).waitFor();
  const metricId=(await page.evaluate(()=>location.hash)).split('/')[1];
  await page.getByRole('button',{name:'Run metric',exact:true}).click();await page.getByText('42 units',{exact:true}).waitFor();
  await page.locator('nav button[data-view=notebooks]').click();await page.getByRole('button',{name:'New notebook',exact:true}).click();
  await page.getByRole('textbox',{name:'notebook name',exact:true}).fill('Notebook '+suffix);await page.getByRole('button',{name:'Add SQL cell',exact:true}).click();await page.getByRole('textbox',{name:'Cell SQL',exact:true}).fill('SELECT 84 AS answer');
  await page.getByRole('button',{name:'Add note cell',exact:true}).click();await page.getByRole('textbox',{name:'Note text',exact:true}).fill('Notes remain text: <img src=x onerror=alert(1)>');
  await page.getByRole('button',{name:'Save notebook',exact:true}).click();await page.locator('#message').filter({hasText:'Saved notebook.'}).waitFor();
  await page.getByRole('button',{name:'Run cell',exact:true}).nth(0).click();await page.locator('#analysis-notebook tbody td').filter({hasText:'84'}).waitFor();
  await page.getByRole('button',{name:'Run cell',exact:true}).nth(1).click();await page.locator('.analysis-note').waitFor();assert.equal(await page.locator('.analysis-note img').count(),0);
  const before=await page.request.get(base+'/api/query-history');const count=(await before.json()).length;
  await page.reload();await page.getByRole('textbox',{name:'notebook name',exact:true}).waitFor();
  const after=await page.request.get(base+'/api/query-history');assert.equal((await after.json()).length,count,'Saved links must not automatically execute cells');
  await page.locator('nav button[data-view=dashboards]').click();await page.getByRole('button',{name:'New dashboard',exact:true}).click();
  await page.getByRole('textbox',{name:'dashboard name',exact:true}).fill('Dashboard '+suffix);await page.getByRole('button',{name:'Add table widget',exact:true}).click();await page.getByRole('textbox',{name:'Widget SQL',exact:true}).fill('SELECT * FROM range(80)');
  await page.getByRole('button',{name:'Add metric widget',exact:true}).click();await page.getByRole('combobox',{name:'Metric definition',exact:true}).selectOption(metricId);
  await page.getByRole('button',{name:'Save dashboard',exact:true}).click();await page.locator('#message').filter({hasText:'Saved dashboard.'}).waitFor();
  const dashboardId=(await page.evaluate(()=>location.hash)).split('/')[1];
  await page.getByRole('button',{name:'Refresh dashboard',exact:true}).click();await page.locator('#analysis-dashboard').getByText('42 units',{exact:true}).waitFor();
  assert.equal(await page.locator('#analysis-dashboard tbody tr').count(),50);
  const route=base+'/api/documents/'+dashboardId+'/items/0/run';await page.route(route,request=>request.fulfill({status:503,contentType:'application/json',body:JSON.stringify({detail:'Simulated outage'})}));
  await page.getByRole('button',{name:'Refresh widget',exact:true}).first().click();await page.getByText(/Refresh failed. Last successful data remains visible/).waitFor();assert.equal(await page.locator('#analysis-dashboard tbody tr').count(),50);
  await page.unroute(route);await page.getByRole('button',{name:'Refresh dashboard',exact:true}).click();await page.getByRole('button',{name:'Refresh dashboard',exact:true}).waitFor();
  await page.setViewportSize({width:390,height:844});assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false,'Analysis dashboard overflows on mobile');await page.screenshot({path:'/tmp/platform-dashboard-mobile.png',fullPage:true});
  assert.deepEqual(errors,[]);console.log('Analysis browser passed: metric definitions, SQL/note notebooks, revision-backed persistence, non-executing saved links, table/metric dashboard refresh, stale-data fallback, text safety and mobile layout.');
 }finally{await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1;});
