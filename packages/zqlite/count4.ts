import {LogContext} from '@rocicorp/logger';
import {Database} from './src/db.ts';
const db = new Database(new LogContext('info'), '/Users/harsharanga/code/mono-v2/apps/zbugs/zero.db');

// Various orderings
for (const orderBy of ['id ASC LIMIT 101', 'id DESC LIMIT 101', 'created DESC LIMIT 101', 'modified DESC LIMIT 101']) {
  const ids = db.prepare(`SELECT id FROM issue WHERE open=1 AND visibility='public' AND projectID=(SELECT id FROM project WHERE lowerCaseName='zero') ORDER BY ${orderBy}`).all().map((r: any) => r.id);
  const ph = ids.map(() => '?').join(',');
  const il = db.prepare(`SELECT COUNT(*) as c FROM issueLabel WHERE issueID IN (${ph})`).get(...ids);
  console.log(`${orderBy}: ${ids.length} issues, issueLabels=${(il as any).c}`);
}
// Also check ALL open+public
const allIds = db.prepare(`SELECT id FROM issue WHERE open=1 AND visibility='public' AND projectID=(SELECT id FROM project WHERE lowerCaseName='zero')`).all().map((r: any) => r.id);
console.log(`total open+public: ${allIds.length}`);
