import {LangsmithService} from '$osmanthus';
import {createApiQuery} from './queryUtils';

const LANGSMITH_TAG = 'langsmith';
export const queryDatasets = createApiQuery(
  LangsmithService.getLangsmithDatasets,
  LANGSMITH_TAG,
  {}
);
