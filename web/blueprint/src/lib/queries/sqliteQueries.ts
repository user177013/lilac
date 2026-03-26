import {SqliteService} from '$osmanthus';
import {createApiQuery} from './queryUtils';

const SQLITE_TAG = 'sqlite';
export const queryTables = createApiQuery(SqliteService.getTables, SQLITE_TAG);
