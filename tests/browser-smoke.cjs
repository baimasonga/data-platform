const {chromium} = require('playwright');
const path = require('path');
const fs = require('node:fs/promises');
const assert = require('node:assert/strict');

(async () => {
  const browser = await chromium.launch({
    headless: true,
    executablePath: process.env.CHROMIUM_EXECUTABLE_PATH || undefined,
    args: ['--no-sandbox']
  });
  try {
    const page = await browser.newPage({viewport: {width: 1440, height: 1000}});
    const errors = [];
    page.on('pageerror', error => errors.push(error.message));
    page.on('console', message => {
      if (message.type() === 'error') errors.push(message.text());
    });
    const base = process.env.DATA_PLATFORM_URL || 'http://127.0.0.1:8000';
    await page.goto(base);
    await page.locator('#file').setInputFiles(path.resolve(__dirname, '../examples/sales.csv'));
    await page.getByRole('button', {name: 'Query this dataset', exact: true}).waitFor();
    await page.locator('.source-provenance').waitFor();
    const sourceDownload = page.waitForEvent('download');
    await page.getByRole('link', {name: 'Download original source'}).click();
    assert.equal((await sourceDownload).suggestedFilename(), 'sales.csv');
    await page.getByRole('button', {name: 'Query this dataset', exact: true}).click();
    await page.getByRole('button', {name: 'Run query', exact: false}).click();
    await page.locator('#query-output:not([hidden])').waitFor();
    assert.ok(await page.locator('#query-table tbody tr').count() > 0);
    await page.getByRole('button', {name: 'Draw bar chart'}).click();
    await page.locator('.bar-row').first().waitFor();
    const exportDownload = page.waitForEvent('download');
    await page.getByRole('button', {name: 'Export shown rows'}).click();
    assert.equal((await exportDownload).suggestedFilename(), 'query-results.csv');
    await page.getByRole('button', {name: 'Save query', exact: true}).click();
    const savedName = `Sample sales ${Date.now()}`;
    await page.locator('#query-name').fill(savedName);
    await page.locator('#save-form button[type=submit]').click();
    await page.locator('#save-dialog').waitFor({state: 'hidden'});
    await page.locator('nav button[data-view=saved]').click();
    await page.getByText(savedName, {exact: true}).waitFor();
    await page.setViewportSize({width: 390, height: 844});
    await page.locator('nav button[data-view=datasets]').click();
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > window.innerWidth), false,
      'Mobile layout overflows');
    await page.locator('nav button[data-view=imports]').click();
    await page.locator('#imports-status').filter({hasText: 'up to date'}).waitFor();
    await page.locator('#import-filter').selectOption('succeeded');
    const completed = page.locator('.import-card').first();
    assert.equal(await completed.locator('.import-status').textContent(), 'succeeded');
    await completed.getByRole('button', {name: 'Open dataset'}).click();
    await page.locator('#dataset-detail:not([hidden])').waitFor();
    // Seed a real validation failure, then verify its persisted browser display.
    const brokenName = `broken-${Date.now()}.csv`;
    const failure = await page.request.post(base + '/api/datasets', {
      multipart: {file: {name: brokenName, mimeType: 'text/csv', buffer: Buffer.from('a,b\n1,2,3\n')}}
    });
    assert.equal(failure.status(), 400);
    await page.locator('nav button[data-view=imports]').click();
    await page.locator('#imports-status').filter({hasText: 'up to date'}).waitFor();
    await page.locator('#import-filter').selectOption('failed');
    const failed = page.locator('.import-card').filter({hasText: brokenName});
    await failed.waitFor();
    assert.ok((await failed.locator('.import-error').textContent()).includes('expected 2'));
    assert.equal(await failed.getByRole('button', {name: 'Open dataset'}).count(), 0);
    await page.reload();
    await page.locator('nav button[data-view=imports]').click();
    await page.locator('.import-card').filter({hasText: brokenName}).waitFor();
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > window.innerWidth), false,
      'Import history overflows on mobile');
    await page.screenshot({path: '/tmp/import-history-mobile.png', fullPage: true});
    await page.setViewportSize({width: 1440, height: 1000});
    await page.screenshot({path: '/tmp/import-history-desktop.png', fullPage: true});
    // Manage a dedicated synthetic dataset; existing workspace data is untouched.
    await page.locator('nav button[data-view=datasets]').click();
    const managedFile = `manage-${Date.now()}.csv`;
    await page.locator('#file').setInputFiles({name: managedFile, mimeType: 'text/csv', buffer: Buffer.from('x\n' + Array.from({length: 1005}, (_, i) => String(i)).join('\n') + '\n')});
    await page.locator('#dataset-detail h2').filter({hasText: managedFile}).waitFor();
    await page.getByRole('button', {name: 'Rename dataset', exact: true}).click();
    const renamed = `Quarterly sales ${Date.now()}`;
    await page.locator('#dataset-name').fill(renamed);
    await page.getByRole('button', {name: 'Save name', exact: true}).click();
    await page.locator('#dataset-detail h2').filter({hasText: renamed}).waitFor();
    const fullExport = page.waitForEvent('download');
    await page.getByRole('button', {name: 'Export dataset CSV', exact: true}).click();
    const fullDownload = await fullExport;
    assert.equal(fullDownload.suggestedFilename(), renamed + '.csv');
    const exported = await fs.readFile(await fullDownload.path(), 'utf8');
    const exportedLines = exported.replace(/^\uFEFF/, '').trimEnd().split('\r\n');
    assert.equal(exportedLines.length, 1006, 'Dataset export must include rows beyond the query-result cap');
    assert.equal(exportedLines[0], '"x"');
    assert.equal(exportedLines[1005], '"1004"');
    await page.locator('#message').filter({hasText: '1,005 rows'}).waitFor();
    assert.equal(await page.getByRole('button', {name: 'Export dataset CSV', exact: true}).isEnabled(), true);
    await page.getByRole('button', {name: 'Delete dataset', exact: true}).click();
    assert.equal(await page.locator('#confirm-delete').isDisabled(), true);
    await page.locator('#delete-confirmation').fill('wrong');
    assert.equal(await page.locator('#confirm-delete').isDisabled(), true);
    await page.getByRole('button', {name: 'Cancel', exact: true}).click();
    await page.reload();
    await page.locator('#search').fill(renamed);
    await page.locator('.dataset-card').filter({hasText: renamed}).getByRole('button', {name: 'Inspect dataset →'}).click();
    await page.getByRole('button', {name: 'Delete dataset', exact: true}).click();
    await page.setViewportSize({width: 390, height: 844});
    await page.locator('#delete-confirmation').fill(renamed);
    assert.equal(await page.locator('#confirm-delete').isEnabled(), true);
    assert.equal(await page.locator('#delete-dialog').evaluate(e => e.scrollWidth > e.clientWidth), false,
      'Delete dialog overflows on mobile');
    await page.screenshot({path: '/tmp/dataset-delete-mobile.png', fullPage: true});
    await page.getByRole('button', {name: 'Delete permanently', exact: true}).click();
    await page.locator('#message').filter({hasText: `Deleted ${renamed}.`}).waitFor();
    assert.equal(await page.locator('#dataset-detail').isHidden(), true);
    assert.equal(await page.locator('.dataset-card').filter({hasText: renamed}).count(), 0);
    await page.locator('nav button[data-view=imports]').click();
    const deletedHistory = page.locator('.import-card').filter({hasText: managedFile});
    await deletedHistory.getByText('Dataset is no longer available.').waitFor();
    assert.equal(await deletedHistory.getByRole('button', {name: 'Open dataset'}).count(), 0);
    // Update and delete only the synthetic saved query created by this test.
    await page.locator('nav button[data-view=saved]').click();
    let savedCard = page.locator('.saved-card').filter({hasText: savedName});
    await savedCard.getByRole('button', {name: 'Edit query', exact: true}).click();
    await page.locator('#edit-saved-name').fill('Cancelled change');
    await page.locator('#cancel-edit-saved').click();
    await savedCard.getByRole('button', {name: 'Edit query', exact: true}).click();
    assert.equal(await page.locator('#edit-saved-name').inputValue(), savedName);
    const editedName = `Updated ${savedName}`;
    await page.locator('#edit-saved-name').fill(editedName);
    await page.locator('#edit-saved-sql').fill('SELECT 7 AS answer');
    assert.equal(await page.locator('#edit-saved-dialog').evaluate(e => e.scrollWidth > e.clientWidth), false,
      'Saved query editor overflows on mobile');
    await page.screenshot({path: '/tmp/saved-query-edit-mobile.png', fullPage: true});
    await page.locator('#confirm-edit-saved').click();
    await page.locator('#message').filter({hasText: 'Saved query updated.'}).waitFor();
    await page.reload();
    await page.locator('nav button[data-view=saved]').click();
    savedCard = page.locator('.saved-card').filter({hasText: editedName});
    await savedCard.waitFor();
    assert.ok((await savedCard.locator('code').textContent()).includes('SELECT 7'));
    await savedCard.getByRole('button', {name: 'Open query →'}).click();
    assert.equal(await page.locator('#query-output').isHidden(), true);
    assert.equal(await page.locator('#sql').inputValue(), 'SELECT 7 AS answer');
    await page.getByRole('button', {name: 'Run query', exact: false}).click();
    await page.locator('#query-output:not([hidden])').waitFor();
    assert.equal(await page.locator('#query-table tbody td').textContent(), '7');
    await page.locator('nav button[data-view=saved]').click();
    savedCard = page.locator('.saved-card').filter({hasText: editedName});
    await savedCard.getByRole('button', {name: 'Delete query', exact: true}).click();
    await page.locator('#cancel-delete-saved').click();
    assert.equal(await savedCard.count(), 1);
    await savedCard.getByRole('button', {name: 'Delete query', exact: true}).click();
    await page.locator('#confirm-delete-saved').click();
    await page.locator('#message').filter({hasText: 'Saved query deleted.'}).waitFor();
    assert.equal(await page.locator('.saved-card').filter({hasText: editedName}).count(), 0);
    await page.locator('nav button[data-view=sql]').click();
    assert.equal(await page.locator('#sql').inputValue(), 'SELECT 7 AS answer');
    // History records actual runs and only reopens SQL until explicitly run.
    const marker = `history_${Date.now()}`;
    const historySql = `SELECT 42 AS ${marker}`;
    const succeededRun = await page.request.post(base + '/api/query', {data: {sql: historySql}});
    assert.equal(succeededRun.status(), 200);
    const failedRun = await page.request.post(base + '/api/query', {data: {sql: `SELECT * FROM missing_${marker}`}});
    assert.equal(failedRun.status(), 400);
    await page.locator('nav button[data-view=history]').click();
    await page.locator('#history-status').filter({hasText: 'up to date'}).waitFor();
    await page.locator('#history-search').fill(marker);
    await page.locator('#history-filter').selectOption('failed');
    assert.equal(await page.locator('.history-card').count(), 1);
    assert.ok((await page.locator('.history-card .import-error').textContent()).includes('missing_' + marker));
    await page.locator('#history-filter').selectOption('succeeded');
    assert.equal(await page.locator('.history-card').count(), 1);
    await page.locator('.history-card').getByRole('button', {name: 'Open in SQL editor'}).click();
    assert.equal(await page.locator('#sql').inputValue(), historySql);
    assert.equal(await page.locator('#query-output').isHidden(), true);
    let historyResponse = await page.request.get(base + '/api/query-history');
    assert.equal((await historyResponse.json()).filter(run => run.sql.includes(marker)).length, 2,
      'Reopening history must not execute SQL');
    await page.getByRole('button', {name: 'Run query', exact: false}).click();
    await page.locator('#query-output:not([hidden])').waitFor();
    assert.equal(await page.locator('#query-table tbody td').textContent(), '42');
    await page.reload();
    await page.locator('nav button[data-view=history]').click();
    await page.locator('#history-status').filter({hasText: 'up to date'}).waitFor();
    await page.locator('#history-search').fill(marker);
    assert.equal(await page.locator('.history-card').count(), 3);
    await page.locator('#history-search').fill('no_match_' + marker);
    await page.getByText('No queries match these filters.', {exact: true}).waitFor();
    await page.locator('#history-search').fill(marker);
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > window.innerWidth), false,
      'Query history overflows on mobile');
    await page.screenshot({path: '/tmp/query-history-mobile.png', fullPage: true});
    const externalRequests = [];
    await page.route('**/*', route => {
      if (new URL(route.request().url()).origin !== new URL(base).origin) {
        externalRequests.push(route.request().url());
        return route.abort();
      }
      return route.continue();
    });
    await page.goto(base + '/docs');
    await page.getByText('API specification loaded.', {exact: false}).waitFor();
    assert.ok(await page.locator('#endpoints section').count() > 0);
    assert.deepEqual(externalRequests, []);
    assert.deepEqual(errors, []);
    console.log('Browser smoke passed: import, provenance, download, SQL, chart, export, save, mobile layout, import history with filters and failure details, dataset rename and confirmed deletion, saved-query editing and deletion, query history and explicit reruns, full dataset CSV downloads, and offline API reference.');
  } finally {
    await browser.close();
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
