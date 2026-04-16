import {LogContext} from '@rocicorp/logger';
import {Database} from './src/db.ts';
const db = new Database(new LogContext('info'), '/Users/harsharanga/code/mono-v2/apps/zbugs/zero.db');

// Pick top 100 open+public issues ordered as issueListV2 orders (modified desc typically)
const issueIDs = db.prepare(`SELECT id FROM issue WHERE open=1 AND visibility='public' AND projectID=(SELECT id FROM project WHERE lowerCaseName='zero') ORDER BY modified DESC, id DESC LIMIT 100`).all().map((r: any) => r.id);
console.log('selected issue ids (first 100 open+public): count =', issueIDs.length);
const placeholders = issueIDs.map(() => '?').join(',');
const ilCount = db.prepare(`SELECT COUNT(*) as c FROM issueLabel WHERE issueID IN (${placeholders})`).get(...issueIDs);
const labelCount = db.prepare(`SELECT COUNT(DISTINCT labelID) as c FROM issueLabel WHERE issueID IN (${placeholders})`).get(...issueIDs);
console.log('issueLabel rows for those:', ilCount);
console.log('distinct labels referenced:', labelCount);
