const {chromium}=require('playwright');
const assert=require('node:assert/strict');
(async()=>{
 const browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_EXECUTABLE_PATH||'/usr/bin/chromium',args:['--no-sandbox']});
 try{
  const page=await browser.newPage({viewport:{width:1440,height:1000}}),errors=[];
  page.on('pageerror',error=>errors.push(error.message));
  const base=process.env.DATA_PLATFORM_URL||'http://127.0.0.1:8003',suffix=Date.now();
  await page.goto(base);
  await page.locator('#file').setInputFiles({name:`Python input ${suffix}.csv`,mimeType:'text/csv',buffer:Buffer.from('value\n2\n3\n')});
  await page.locator('#dataset-detail h2').filter({hasText:'Python input '+suffix}).waitFor();
  const dataset=(await page.request.get(base+'/api/datasets').then(r=>r.json())).find(item=>item.name===`Python input ${suffix}.csv`);
  assert.ok(dataset);
  await page.locator('nav button[data-view=notebooks]').click();await page.getByRole('button',{name:'New notebook',exact:true}).click();
  await page.getByRole('textbox',{name:'notebook name',exact:true}).fill('Python notebook '+suffix);
  await page.getByRole('button',{name:'Add Python cell',exact:true}).click();
  await page.getByRole('textbox',{name:'Cell Python',exact:true}).fill(`total = datasets["${dataset.id}"]["value"].sum()\nprint("Total:", total)\npd.DataFrame({"total": [total]})`);
  await page.getByRole('checkbox',{name:`Python input ${suffix}.csv`,exact:true}).check();
  await page.getByRole('button',{name:'Add Python cell',exact:true}).click();
  await page.getByRole('textbox',{name:'Cell Python',exact:true}).nth(1).fill('print("Shared value:", total * 2)');
  await page.getByRole('button',{name:'Save notebook',exact:true}).click();await page.locator('#message').filter({hasText:'Saved notebook.'}).waitFor();
  const id=(await page.evaluate(()=>location.hash)).split('/')[1];
  assert.equal((await page.request.get(base+'/api/documents/'+id+'/runs').then(r=>r.json())).length,0);
  await page.getByRole('button',{name:'Run all Python cells',exact:true}).click();
  await page.locator('.analysis-result pre').filter({hasText:'Shared value: 10'}).waitFor({timeout:75000});
  await page.locator('.analysis-result td').filter({hasText:'5'}).waitFor();
  await page.getByRole('button',{name:'Run Python cell',exact:true}).nth(1).click();
  await page.locator('.analysis-result pre').filter({hasText:'NameError'}).waitFor({timeout:75000});
  // Expand history explicitly; opening a notebook must never execute code.
  await page.getByText('Python execution history',{exact:true}).click();
  await page.getByRole('button',{name:'Refresh Python history',exact:true}).click();
  await page.locator('.history-card').filter({hasText:'succeeded'}).waitFor();
  const before=(await page.request.get(base+'/api/documents/'+id+'/runs').then(r=>r.json())).length;
  await page.reload();await page.getByRole('textbox',{name:'notebook name',exact:true}).waitFor();
  assert.equal((await page.request.get(base+'/api/documents/'+id+'/runs').then(r=>r.json())).length,before);
  await page.getByRole('textbox',{name:'Cell Python',exact:true}).first().fill('print("<img src=x onerror=alert(1)>")');
  assert.equal(await page.getByRole('button',{name:'Run all Python cells',exact:true}).isDisabled(),true);
  await page.getByRole('button',{name:'Save notebook',exact:true}).click();await page.locator('#message').filter({hasText:'Saved notebook.'}).waitFor();
  await page.getByRole('button',{name:'Run Python cell',exact:true}).first().click();
  await page.locator('.analysis-result pre').filter({hasText:'<img src=x onerror=alert(1)>'}).first().waitFor({timeout:75000});
  assert.equal(await page.locator('.analysis-result img').count(),0);
  await page.setViewportSize({width:390,height:844});
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false,'Python notebook overflows on mobile');
  await page.screenshot({path:'/tmp/platform-python-notebook-mobile.png',fullPage:true});
  assert.deepEqual(errors,[]);
  console.log('Python notebook browser passed: dataset selection, shared run-all scope, fresh individual sessions, stdout/tables, durable history, non-executing reload, revision/dirty guards, text-safe output and mobile layout.');
 }finally{await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1;});
