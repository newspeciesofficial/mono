import {LogContext} from '@rocicorp/logger';
import {Database} from './src/db.ts';
const db = new Database(new LogContext('info'), '/Users/harsharanga/code/mono-v2/apps/zbugs/zero.db');

const issueCount = db.prepare("SELECT COUNT(*) as c FROM issue").get();
const openIssueCount = db.prepare("SELECT COUNT(*) as c FROM issue WHERE open=1 AND visibility='public'").get();
const issueLabelsPerIssueAll = db.prepare("SELECT COUNT(*) as c FROM issueLabel il JOIN issue i ON il.issueID=i.id WHERE i.visibility='public'").get();
const issueLabelsPerIssueOpen = db.prepare("SELECT COUNT(*) as c FROM issueLabel il JOIN issue i ON il.issueID=i.id WHERE i.visibility='public' AND i.open=1").get();
const labelsPerIssueAll = db.prepare("SELECT COUNT(DISTINCT il.labelID || ':' || il.issueID) as c FROM issueLabel il JOIN issue i ON il.issueID=i.id WHERE i.visibility='public'").get();
const commentsPerIssueAll = db.prepare("SELECT COUNT(*) as c FROM comment c JOIN issue i ON c.issueID=i.id WHERE i.visibility='public'").get();
const viewStatePerIssueAll = db.prepare("SELECT COUNT(*) as c FROM viewState v JOIN issue i ON v.issueID=i.id WHERE i.visibility='public'").get();
console.log('issues total (public):', issueCount, '  open+public:', openIssueCount);
console.log('issueLabel rows joined:', issueLabelsPerIssueAll, '  (open+public):', issueLabelsPerIssueOpen);
console.log('distinct (issue, label) pairs public:', labelsPerIssueAll);
console.log('comments joined:', commentsPerIssueAll);
console.log('viewState joined:', viewStatePerIssueAll);
