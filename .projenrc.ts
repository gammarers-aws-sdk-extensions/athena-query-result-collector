import { ProjenTypeScriptProject } from '@gammarers/projen-projects';
const project = new ProjenTypeScriptProject({
  name: 'athena-query-result-collector',
  repositoryUrl: 'https://github.com/gammarers-aws-sdk-extensions/athena-query-result-collector.git',
  description: 'A TypeScript library for collecting AWS Athena query results via pagination. It supports full collection, streaming, and page-based batch processing, and it uses athena-query-result-pager internally.',
  keywords: ['aws', 'athena', 'query', 'result', 'collector', 'pagination', 'abort', 'signal'],
  releaseToNpm: true,
  npmTrustedPublishing: true,
  minNodeVersion: '20.0.0',
  workflowNodeVersion: '24.x',
  deps: [
    '@aws-sdk/client-athena@^3.983.0',
    'athena-query-result-pager@^0.7.0',
  ],
  devDeps: [
    '@gammarers/projen-projects@^0.5.10',
  ],
});
project.synth();